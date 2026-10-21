package io.github.samhom.driverkit.elasticsearch.legacy;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.elasticsearch.ElasticsearchSettings;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class Elasticsearch7AdapterTest {
  @Test
  void nativeHlrcUsesAuthenticationAndDoesNotFollowRedirects() throws Exception {
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("co.elastic.clients.elasticsearch.ElasticsearchClient"));
    String password = UUID.randomUUID().toString();
    String expected =
        "Basic "
            + Base64.getEncoder()
                .encodeToString(("fixture:" + password).getBytes(StandardCharsets.UTF_8));
    AtomicInteger redirected = new AtomicInteger();
    AtomicInteger authenticated = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 4);
    server.createContext(
        "/",
        exchange -> {
          if (expected.equals(exchange.getRequestHeaders().getFirst("Authorization")))
            authenticated.incrementAndGet();
          exchange
              .getResponseHeaders()
              .add("Location", "http://127.0.0.1:" + server.getAddress().getPort() + "/redirected");
          exchange.sendResponseHeaders(302, -1);
          exchange.close();
        });
    server.createContext(
        "/redirected",
        exchange -> {
          redirected.incrementAndGet();
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();
    try {
      var adapter = new Elasticsearch7Adapter();
      var client =
          adapter.create(
              ElasticsearchSettings.defaults(
                  new Endpoint("127.0.0.1", server.getAddress().getPort(), false),
                  () -> new Credentials("fixture", password.toCharArray())));
      try {
        assertThrows(Exception.class, () -> adapter.warmup(client, Duration.ofSeconds(1)));
        assertEquals(0, redirected.get());
        assertTrue(authenticated.get() > 0, "Native HLRC must send the ephemeral credentials");
      } finally {
        adapter.close(client);
      }
    } finally {
      server.stop(0);
    }
  }
}
