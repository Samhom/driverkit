package io.github.samhom.driverkit.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.couchbase.client.java.json.JsonObject;
import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.couchbase.*;
import io.github.samhom.driverkit.elasticsearch.*;
import io.github.samhom.driverkit.redis.*;
import io.github.samhom.driverkit.rocketmq.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import redis.clients.jedis.RedisClient;

/** Only the disposable containers created by tools/real_clusters.py can be controlled here. */
@Timeout(value = 420)
class NativeClustersIT {
  static final Duration IO = Duration.ofSeconds(2);
  static final ResourceId RESOURCE = new ResourceId("driverkit", "integration", "real-server");
  static final Operation PROBE = Operation.write("integration-probe", Duration.ofSeconds(3));

  static String required(String name) {
    return Objects.requireNonNull(System.getenv(name), "Missing fixture environment: " + name);
  }

  static CredentialsProvider credentials(String username, String password) {
    return () -> new Credentials(username, password.toCharArray());
  }

  interface Probe<C> {
    void run(C client) throws Exception;
  }

  interface Rotation {
    void run(String password) throws Exception;
  }

  @Test
  void redis() throws Exception {
    if (!selected("redis")) return;
    JedisAdapter adapter = new JedisAdapter();
    Function<String, RedisSettings> settings =
        password ->
            new RedisSettings(
                new Endpoint("127.0.0.1", 6379, false),
                credentials("driverkit", password),
                IO,
                IO,
                4,
                0,
                1024);
    try (RedisClient admin =
        adapter.create(
            new RedisSettings(
                new Endpoint("127.0.0.1", 6379, false),
                credentials("default", required("DRIVERKIT_IT_ADMIN_PASSWORD")),
                IO,
                IO,
                2,
                0,
                1024))) {
      exercise(
          "redis",
          adapter,
          settings,
          client -> {
            client.set("driverkit-it", "value");
            assertEquals("value", client.get("driverkit-it"));
          },
          password ->
              admin.sendCommand(
                  redis.clients.jedis.Protocol.Command.ACL,
                  "SETUSER",
                  "driverkit",
                  "resetpass",
                  ">" + password));
    }
  }

  @Test
  void elasticsearch() throws Exception {
    if (!selected("elasticsearch")) return;
    exercise(
        "elasticsearch",
        new ElasticsearchAdapter(),
        password ->
            new ElasticsearchSettings(
                new Endpoint("127.0.0.1", 9200, false),
                credentials("driverkit", password),
                IO,
                IO,
                4,
                100,
                1024),
        client -> {
          client.index(i -> i.index("driverkit-it").id("probe").document(Map.of("value", "ok")));
          assertTrue(client.get(g -> g.index("driverkit-it").id("probe"), Map.class).found());
        },
        password ->
            adminHttp(
                9200,
                "/_security/user/driverkit/_password",
                "POST",
                "{\"password\":\"" + password + "\"}",
                "application/json",
                "elastic"));
  }

  @Test
  void couchbase() throws Exception {
    if (!selected("couchbase")) return;
    exercise(
        "couchbase",
        new CouchbaseAdapter(),
        password ->
            new CouchbaseSettings(
                new Endpoint("127.0.0.1", 11210, false),
                credentials("driverkit", password),
                "driverkit-it",
                "_default",
                "_default",
                IO,
                IO,
                1024),
        client -> {
          var collection = client.bucket("driverkit-it").defaultCollection();
          collection.upsert("probe", JsonObject.create().put("value", "ok"));
          assertEquals("ok", collection.get("probe").contentAsObject().getString("value"));
        },
        password ->
            adminHttp(
                8091,
                "/settings/rbac/users/local/driverkit",
                "PUT",
                "password="
                    + password
                    + "&roles=data_reader%5Bdriverkit-it%5D%2Cdata_writer%5Bdriverkit-it%5D",
                "application/x-www-form-urlencoded",
                "Administrator"));
  }

  @Test
  void rocketmq() throws Exception {
    if (!selected("rocketmq")) return;
    RocketMqProducerAdapter adapter = new RocketMqProducerAdapter();
    Function<String, RocketMqSettings> settings =
        password ->
            new RocketMqSettings(
                new Endpoint("127.0.0.1", 9876, false),
                credentials("driverkit", password),
                "driverkit-producer",
                "driverkit-it",
                IO,
                1,
                1024);
    var adminSettings =
        new RocketMqSettings(
            new Endpoint("127.0.0.1", 9876, false),
            credentials("administrator", required("DRIVERKIT_IT_ADMIN_PASSWORD")),
            "driverkit-admin",
            "driverkit-it",
            IO,
            1,
            1024);
    DefaultMQProducer admin = adapter.create(adminSettings);
    try {
      adapter.warmup(admin, Duration.ofSeconds(10));
      await(
          () ->
              admin
                  .getDefaultMQProducerImpl()
                  .getmQClientFactory()
                  .getMQClientAPIImpl()
                  .createUser(
                      "127.0.0.1:10911",
                      UserInfo.of(
                          "driverkit", required("DRIVERKIT_IT_PASSWORD"), "Super", "Enable"),
                      2000),
          90);
      exercise(
          "rocketmq",
          adapter,
          settings,
          client ->
              assertEquals(
                  org.apache.rocketmq.client.producer.SendStatus.SEND_OK,
                  client
                      .send(
                          new Message("driverkit-it", "probe".getBytes(StandardCharsets.UTF_8)),
                          2000)
                      .getSendStatus()),
          password ->
              admin
                  .getDefaultMQProducerImpl()
                  .getmQClientFactory()
                  .getMQClientAPIImpl()
                  .updateUser(
                      "127.0.0.1:10911",
                      UserInfo.of("driverkit", password, "Super", "Enable"),
                      2000));
    } finally {
      adapter.close(admin);
    }
  }

  static boolean selected(String product) {
    String selected = required("DRIVERKIT_IT_PRODUCT");
    if (!Set.of("redis", "elasticsearch", "couchbase", "rocketmq").contains(selected))
      throw new IllegalStateException("Unknown fixture product");
    org.junit.jupiter.api.Assumptions.assumeTrue(selected.equals(product));
    return true;
  }

  static <C, S> void exercise(
      String product,
      DriverAdapter<C, S> adapter,
      Function<String, S> settings,
      Probe<C> probe,
      Rotation rotate)
      throws Exception {
    String initial = required("DRIVERKIT_IT_PASSWORD");
    // Readiness is checked with the actual SDK; a listening port is insufficient.
    await(() -> withNative(adapter, settings.apply(initial), probe), 90);
    assertThrows(
        Exception.class,
        () -> withNative(adapter, settings.apply(UUID.randomUUID().toString()), probe),
        "Real authentication must reject invalid credentials");
    ClientPolicy policy = new ClientPolicy(4, Duration.ofSeconds(15), Duration.ofSeconds(10));
    try (ClientRuntime runtime =
        new ClientRuntime(4, 2, 8, io.github.samhom.driverkit.core.Observer.NOOP)) {
      ManagedClient<C, S> managed =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings.apply(initial)), adapter, policy);
      control(product, "pause");
      try {
        assertThrows(ClientException.class, () -> execute(managed, probe));
      } finally {
        control(product, "unpause");
      }
      await(() -> execute(managed, probe), 45);
      control(product, "restart");
      await(() -> execute(managed, probe), 90);
      String next = UUID.randomUUID().toString();
      rotate.run(next);
      await(
          () -> {
            boolean denied = false;
            try {
              withNative(adapter, settings.apply(initial), probe);
            } catch (Exception expected) {
              denied = true;
            }
            assertTrue(denied, "Old credential must eventually be rejected on a fresh connection");
          },
          45);
      assertEquals(
          UpdateResult.APPLIED,
          managed.reload(new ConfigSnapshot<>(RESOURCE, 2, settings.apply(next))));
      await(() -> execute(managed, probe), 45);
      assertEquals(2, managed.status().revision());
      assertEquals(0, managed.cleanupFailures());
    }
  }

  static <C, S> void withNative(DriverAdapter<C, S> adapter, S settings, Probe<C> probe)
      throws Exception {
    C client = adapter.create(settings);
    try {
      adapter.warmup(client, Duration.ofSeconds(10));
      probe.run(client);
    } finally {
      adapter.close(client, Duration.ofSeconds(10));
    }
  }

  static <C, S> void execute(ManagedClient<C, S> client, Probe<C> probe) {
    client.execute(
        PROBE,
        (nativeClient, context) -> {
          probe.run(nativeClient);
          return null;
        });
  }

  interface Checked {
    void run() throws Exception;
  }

  static void await(Checked check, int seconds) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    Throwable lastFailure = null;
    while (System.nanoTime() < deadline) {
      try {
        check.run();
        return;
      } catch (Exception | AssertionError retryableFixtureFailure) {
        lastFailure = retryableFixtureFailure;
        Thread.sleep(300);
      }
    }
    StringBuilder types = new StringBuilder();
    for (int depth = 0;
        lastFailure != null && depth < 6;
        depth++, lastFailure = lastFailure.getCause())
      types.append(lastFailure.getClass().getSimpleName()).append(" ");
    fail("Real server scenario exceeded its recovery deadline; failure types: " + types);
  }

  static void control(String product, String action) throws Exception {
    String run = required("DRIVERKIT_IT_RUN");
    if (!run.matches("[a-f0-9]{32}") || !Set.of("pause", "unpause", "restart").contains(action))
      throw new IllegalStateException("Invalid fixture control");
    String container = "driverkit-it-" + run + "-" + product;
    Process inspect =
        new ProcessBuilder(
                "docker",
                "inspect",
                "--format",
                "{{index .Config.Labels \"driverkit.it.run\"}}",
                container)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start();
    if (!inspect.waitFor(10, TimeUnit.SECONDS)) {
      inspect.destroyForcibly();
      fail("Fixture ownership lookup timed out");
    }
    assertEquals(0, inspect.exitValue());
    assertEquals(
        run,
        new String(inspect.getInputStream().readNBytes(128), StandardCharsets.UTF_8).trim(),
        "Only owned fixture containers may be controlled");
    Process process =
        new ProcessBuilder("docker", action, container)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      fail("Fixture control timed out");
    }
    assertEquals(0, process.exitValue());
  }

  static void adminHttp(
      int port, String path, String method, String body, String type, String username)
      throws Exception {
    // Literal loopback endpoint of this run's fixture, never a user-supplied URL.
    String authorization =
        Base64.getEncoder()
            .encodeToString(
                (username + ":" + required("DRIVERKIT_IT_ADMIN_PASSWORD"))
                    .getBytes(StandardCharsets.UTF_8));
    HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(IO)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    var request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .timeout(IO)
            .header("Authorization", "Basic " + authorization)
            .header("Content-Type", type)
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    assertTrue(status >= 200 && status < 300, "Fixture credential rotation must succeed");
  }
}
