package io.github.samhom.driverkit.elasticsearch;

import static org.junit.jupiter.api.Assertions.*;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.json.JsonData;
import com.sun.net.httpserver.*;
import io.github.samhom.driverkit.core.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Real Java API Client and HTTP transport against a bounded local protocol fixture. */
class ElasticsearchTransportTest {
  private static final ResourceId RESOURCE = new ResourceId("example", "test", "search");

  @Test
  void authenticatesAndMapsDocumentOperations() throws Exception {
    try (SearchServer server = new SearchServer();
        ClientRuntime runtime = new ClientRuntime()) {
      ElasticsearchSettings settings = server.settings();
      ManagedClient<ElasticsearchClient, ElasticsearchSettings> handle =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings),
              new ElasticsearchAdapter(),
              ClientPolicy.defaults());
      ManagedSearch search = new ManagedSearch(handle, settings);
      assertEquals("a", search.index("catalog", "a", "{\"name\":\"example\"}").id());
      assertTrue(search.get("catalog", "a").found());
      assertTrue(server.authorized.get() >= 2);
      assertThrows(IllegalArgumentException.class, () -> search.get("UPPER", "a"));
      assertThrows(
          IllegalArgumentException.class, () -> search.index("catalog", "large", "x".repeat(1025)));
    }
  }

  @Test
  void classifiesRateLimitAndDoesNotAutomaticallyReplayIt() throws Exception {
    try (SearchServer server = new SearchServer();
        ClientRuntime runtime = new ClientRuntime()) {
      ElasticsearchSettings settings = server.settings();
      ManagedSearch search =
          new ManagedSearch(
              runtime.register(
                  new ConfigSnapshot<>(RESOURCE, 1, settings),
                  new ElasticsearchAdapter(),
                  ClientPolicy.defaults()),
              settings);
      assertEquals(
          ClientException.Kind.RATE_LIMITED,
          assertThrows(ClientException.class, () -> search.get("limited", "a")).kind());
      assertEquals(1, server.limited.get());
    }
  }

  @Test
  void bulkPartialFailureRemainsInspectableAndPayloadIsBounded() throws Exception {
    try (SearchServer server = new SearchServer();
        ClientRuntime runtime = new ClientRuntime()) {
      ElasticsearchSettings settings = server.settings();
      ManagedSearch search =
          new ManagedSearch(
              runtime.register(
                  new ConfigSnapshot<>(RESOURCE, 1, settings),
                  new ElasticsearchAdapter(),
                  ClientPolicy.defaults()),
              settings);
      BulkRequest request =
          BulkRequest.of(
              builder ->
                  builder.operations(
                      op ->
                          op.index(
                              index ->
                                  index
                                      .index("catalog")
                                      .id("a")
                                      .document(JsonData.fromJson("{\"name\":\"example\"}")))));
      ClientException error = assertThrows(ClientException.class, () -> search.bulk(request));
      assertInstanceOf(ManagedSearch.PartialBulkFailure.class, error.getCause());
      assertEquals(1, server.bulks.get());
      BulkRequest oversized =
          BulkRequest.of(
              builder ->
                  builder.operations(
                      op ->
                          op.index(
                              index ->
                                  index
                                      .index("catalog")
                                      .id("a")
                                      .document(Map.of("value", "x".repeat(2048))))));
      assertThrows(ClientException.class, () -> search.bulk(oversized));
      assertEquals(1, server.bulks.get());
    }
  }

  @Test
  void pendingNativeAsyncReleasesCallWorkerAndPinsTransportUntilCompletion() throws Exception {
    try (SearchServer server = new SearchServer();
        ClientRuntime runtime =
            new ClientRuntime(1, 1, 4, io.github.samhom.driverkit.core.Observer.NOOP)) {
      ElasticsearchSettings base = server.settings();
      ElasticsearchSettings settings =
          new ElasticsearchSettings(
              base.endpoint(),
              base.credentials(),
              base.connectTimeout(),
              Duration.ofSeconds(10),
              4,
              10,
              1024);
      ManagedClient<ElasticsearchClient, ElasticsearchSettings> handle =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings),
              new ElasticsearchAdapter(),
              ClientPolicy.defaults());
      ManagedSearch search = new ManagedSearch(handle, settings);
      handle.capabilities().requireManaged(AdapterCapabilities.Feature.ASYNC);
      CompletableFuture<?> pending = search.getAsync("slow", "a");
      try {
        assertTrue(server.slowEntered.await(3, TimeUnit.SECONDS));
        assertTrue(search.getAsync("catalog", "a").get(3, TimeUnit.SECONDS).found());
        assertEquals(
            UpdateResult.APPLIED, handle.reload(new ConfigSnapshot<>(RESOURCE, 2, settings)));
        assertEquals(1, handle.status().retiringGenerations());
        assertTrue(pending.cancel(false));
        assertEquals(1, handle.status().inFlight());
        server.slowRelease.countDown();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (handle.status().retiringGenerations() != 0 && System.nanoTime() < until)
          Thread.yield();
        assertEquals(0, handle.status().retiringGenerations());
        assertEquals(0, handle.status().inFlight());
        assertTrue(search.get("catalog", "a").found());
      } finally {
        server.slowRelease.countDown();
      }
    }
  }

  @Test
  void perRequestBudgetReclaimsPhysicalHttpLeaseBeforeConfiguredReadTimeout() throws Exception {
    try (SearchServer server = new SearchServer();
        ClientRuntime runtime = new ClientRuntime()) {
      ElasticsearchSettings base = server.settings();
      ElasticsearchSettings settings =
          new ElasticsearchSettings(
              base.endpoint(),
              base.credentials(),
              base.connectTimeout(),
              Duration.ofSeconds(5),
              2,
              10,
              1024);
      ManagedClient<ElasticsearchClient, ElasticsearchSettings> handle =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings),
              new ElasticsearchAdapter(),
              ClientPolicy.defaults());
      // Facade ceiling is shorter than the generation's native defaults.
      ManagedSearch search =
          new ManagedSearch(
              handle,
              new ElasticsearchSettings(
                  base.endpoint(),
                  base.credentials(),
                  base.connectTimeout(),
                  Duration.ofMillis(200),
                  2,
                  10,
                  1024));
      try {
        CompletableFuture<?> pending = search.getAsync("slow", "a");
        assertTrue(server.slowEntered.await(2, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class, () -> pending.get(3, TimeUnit.SECONDS));
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (handle.status().inFlight() != 0 && System.nanoTime() < until) Thread.yield();
        assertEquals(0, handle.status().inFlight());
      } finally {
        server.slowRelease.countDown();
      }
      assertTrue(search.get("catalog", "a").found());
    }
  }

  @Test
  void nativeHttpErrorsDistinguishCredentialsPermissionsAndUncertainWrites() throws Exception {
    try (SearchServer server = new SearchServer();
        ClientRuntime runtime = new ClientRuntime()) {
      ElasticsearchSettings settings = server.settings();
      ManagedSearch search =
          new ManagedSearch(
              runtime.register(
                  new ConfigSnapshot<>(RESOURCE, 1, settings),
                  new ElasticsearchAdapter(),
                  ClientPolicy.defaults()),
              settings);
      assertEquals(
          ClientException.Kind.AUTHENTICATION,
          assertThrows(ClientException.class, () -> search.get("unauthenticated", "a")).kind());
      assertEquals(
          ClientException.Kind.AUTHORIZATION,
          assertThrows(ClientException.class, () -> search.get("denied", "a")).kind());
      assertEquals(
          ClientException.Kind.SERVER_REJECTED,
          assertThrows(ClientException.class, () -> search.get("unavailable", "a")).kind());
      ClientException write =
          assertThrows(ClientException.class, () -> search.index("unavailable", "a", "{}"));
      assertEquals(ClientException.Kind.WRITE_OUTCOME_UNKNOWN, write.kind());
      assertEquals(
          ClientException.RetryAdvice.RECONCILE_WRITE,
          write.retryAdvice(Operation.write("index", Duration.ofSeconds(1))));
    }
  }

  private static final class SearchServer implements AutoCloseable {
    final CountDownLatch slowEntered = new CountDownLatch(1), slowRelease = new CountDownLatch(1);
    final String password = UUID.randomUUID().toString();
    final String authorization =
        "Basic "
            + Base64.getEncoder()
                .encodeToString(("fixture:" + password).getBytes(StandardCharsets.UTF_8));
    final AtomicInteger authorized = new AtomicInteger(),
        limited = new AtomicInteger(),
        bulks = new AtomicInteger();
    final HttpServer server;
    final ExecutorService executor =
        new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16));

    SearchServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 8);
      server.setExecutor(executor);
      server.createContext("/", this::handle);
      server.start();
    }

    ElasticsearchSettings settings() {
      return new ElasticsearchSettings(
          new Endpoint(
              server.getAddress().getAddress().getHostAddress(),
              server.getAddress().getPort(),
              false),
          () -> new Credentials("fixture", password.toCharArray()),
          Duration.ofSeconds(1),
          Duration.ofSeconds(2),
          4,
          10,
          1024);
    }

    void handle(HttpExchange exchange) throws IOException {
      try (HttpExchange ignored = exchange) {
        exchange.getResponseHeaders().set("X-Elastic-Product", "Elasticsearch");
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        if (!authorization.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
          exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"fixture\"");
          exchange.sendResponseHeaders(401, -1);
          return;
        }
        authorized.incrementAndGet();
        if (exchange.getRequestMethod().equals("HEAD")) {
          exchange.sendResponseHeaders(200, -1);
          return;
        }
        if (exchange.getRequestBody().readNBytes(65537).length > 65536) {
          exchange.sendResponseHeaders(413, -1);
          return;
        }
        String path = exchange.getRequestURI().getPath(), body;
        if (path.startsWith("/slow/")) {
          slowEntered.countDown();
          try {
            if (!slowRelease.await(10, TimeUnit.SECONDS)) throw new IOException("Fixture deadline");
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          }
        }
        int status = 200;
        if (path.startsWith("/unauthenticated/")
            || path.startsWith("/denied/")
            || path.startsWith("/unavailable/")) {
          status =
              path.startsWith("/unauthenticated/") ? 401 : path.startsWith("/denied/") ? 403 : 503;
          body =
              "{\"error\":{\"type\":\"fixture_exception\",\"reason\":\"fixture\"},\"status\":"
                  + status
                  + "}";
        } else if (path.startsWith("/limited/")) {
          limited.incrementAndGet();
          status = 429;
          body =
              "{\"error\":{\"type\":\"es_rejected_execution_exception\",\"reason\":\"rate limited\"},\"status\":429}";
        } else if (path.equals("/_bulk")) {
          bulks.incrementAndGet();
          body =
              "{\"errors\":true,\"took\":1,\"items\":[{\"index\":{\"_index\":\"catalog\",\"_id\":\"a\",\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\",\"reason\":\"conflict\"}}}]}";
        } else if (exchange.getRequestMethod().equals("PUT"))
          body =
              "{\"_index\":\"catalog\",\"_id\":\"a\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1}";
        else
          body =
              "{\"_index\":\"catalog\",\"_id\":\"a\",\"_version\":1,\"found\":true,\"_source\":{\"name\":\"example\"}}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
      }
    }

    @Override
    public void close() {
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
