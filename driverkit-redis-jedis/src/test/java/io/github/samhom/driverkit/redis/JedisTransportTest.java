package io.github.samhom.driverkit.redis;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.Response;

/** Exercises the real Jedis socket/connection pool against a bounded RESP protocol fixture. */
class JedisTransportTest {
  private static final ResourceId RESOURCE = new ResourceId("example", "test", "cache");

  @Test
  void nativeDriverAuthenticatesAndFacadeExecutesCommands() throws Exception {
    try (RespServer server = new RespServer();
        ClientRuntime runtime = new ClientRuntime()) {
      RedisSettings settings = server.settings();
      ManagedClient<RedisClient, RedisSettings> handle =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings),
              new JedisAdapter(),
              ClientPolicy.defaults());
      ManagedRedis redis =
          new ManagedRedis(handle, Duration.ofSeconds(2), settings.maxValueBytes());
      redis.set("example", "value", Duration.ofSeconds(30));
      assertEquals("value", redis.get("example"));
      assertEquals(1, redis.increment("counter"));
      assertEquals(2, redis.increment("counter"));
      assertEquals(1, redis.delete("example"));
      assertNull(redis.get("example"));
      assertTrue(server.authentications.get() >= 1);
      int writes = server.writes.get();
      assertThrows(
          IllegalArgumentException.class,
          () ->
              redis.set("large", "x".repeat(settings.maxValueBytes() + 1), Duration.ofSeconds(1)));
      assertEquals(writes, server.writes.get());
      RedisSettings tightened =
          new RedisSettings(
              settings.endpoint(),
              settings.credentials(),
              settings.connectTimeout(),
              settings.readTimeout(),
              settings.maxConnections(),
              settings.database(),
              4);
      assertEquals(
          UpdateResult.APPLIED, handle.reload(new ConfigSnapshot<>(RESOURCE, 2, tightened)));
      assertThrows(ClientException.class, () -> redis.set("large", "value", Duration.ofSeconds(1)));
      assertEquals(writes, server.writes.get());
      redis.set("small", "ok", Duration.ofSeconds(1));
      assertEquals("ok", redis.get("small"));
    }
  }

  @Test
  void pipelineRemainsOnItsOriginalGenerationDuringReplacement() throws Exception {
    try (RespServer server = new RespServer();
        ClientRuntime runtime = new ClientRuntime()) {
      RedisSettings settings = server.settings();
      ManagedClient<RedisClient, RedisSettings> handle =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings),
              new JedisAdapter(),
              ClientPolicy.defaults());
      RedisClient old;
      try (ClientSession<RedisClient> session = handle.session();
          Pipeline pipeline = session.client().pipelined()) {
        old = session.client();
        Response<String> result = pipeline.set("pipeline", "value");
        assertEquals(
            UpdateResult.APPLIED, handle.reload(new ConfigSnapshot<>(RESOURCE, 2, settings)));
        assertFalse(old.getPool().isClosed());
        pipeline.sync();
        assertEquals("OK", result.get());
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (!old.getPool().isClosed() && System.nanoTime() < deadline) Thread.sleep(5);
      assertTrue(old.getPool().isClosed());
      assertEquals(
          "value",
          new ManagedRedis(handle, Duration.ofSeconds(2), settings.maxValueBytes())
              .get("pipeline"));
    }
  }

  @Test
  void settingsRejectInvalidResourcesAndRedactCredentials() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RedisSettings(
                new Endpoint("localhost", 6379, true),
                () -> {
                  throw new AssertionError();
                },
                Duration.ZERO,
                Duration.ofSeconds(1),
                1,
                0,
                1024));
    RedisSettings settings =
        RedisSettings.defaults(
            new Endpoint("localhost", 6379, true),
            () -> {
              throw new AssertionError();
            });
    assertEquals("RedisSettings{redacted}", settings.toString());
  }

  @Test
  void exhaustedPoolAndSlowReadReleaseNativeCapacityWithinCallBudget() throws Exception {
    try (RespServer server = new RespServer();
        ClientRuntime runtime = new ClientRuntime()) {
      RedisSettings base = server.settings();
      RedisSettings settings =
          new RedisSettings(
              base.endpoint(),
              base.credentials(),
              Duration.ofSeconds(5),
              Duration.ofSeconds(5),
              1,
              0,
              1024);
      ManagedClient<RedisClient, RedisSettings> handle =
          runtime.register(
              new ConfigSnapshot<>(RESOURCE, 1, settings),
              new JedisAdapter(),
              ClientPolicy.defaults());
      ManagedRedis redis = new ManagedRedis(handle, Duration.ofMillis(150), 1024);
      try (ClientSession<RedisClient> session = handle.session();
          redis.clients.jedis.Connection held = session.client().getPool().getResource()) {
        assertThrows(ClientException.class, () -> redis.get("waiting"));
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (handle.status().inFlight() != 1 && System.nanoTime() < until) Thread.yield();
        assertEquals(
            1,
            handle.status().inFlight(),
            "timed-out borrow must not occupy a worker for 5 seconds");
      }
      try {
        assertThrows(ClientException.class, () -> redis.get("slow"));
        assertTrue(server.slowEntered.await(1, TimeUnit.SECONDS));
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (handle.status().inFlight() != 0 && System.nanoTime() < until) Thread.yield();
        assertEquals(
            0,
            handle.status().inFlight(),
            "native read must end without waiting for fixture release");
      } finally {
        server.slowRelease.countDown();
      }
      assertNull(redis.get("recovered"));
    }
  }

  private static final class RespServer implements AutoCloseable {
    final CountDownLatch slowEntered = new CountDownLatch(1), slowRelease = new CountDownLatch(1);
    final String password = UUID.randomUUID().toString();
    final AtomicInteger authentications = new AtomicInteger(), writes = new AtomicInteger();
    final Map<String, String> values = new ConcurrentHashMap<>();
    final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    final ServerSocket server;
    final ExecutorService acceptor = Executors.newSingleThreadExecutor();
    final ThreadPoolExecutor workers =
        new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16));

    RespServer() throws IOException {
      server = new ServerSocket(0, 8, InetAddress.getLoopbackAddress());
      acceptor.submit(
          () -> {
            while (!server.isClosed())
              try {
                Socket socket = server.accept();
                socket.setSoTimeout(5000);
                sockets.add(socket);
                try {
                  workers.execute(() -> serve(socket));
                } catch (RejectedExecutionException e) {
                  sockets.remove(socket);
                  socket.close();
                }
              } catch (IOException ignored) {
                break;
              }
          });
    }

    RedisSettings settings() {
      return new RedisSettings(
          new Endpoint(server.getInetAddress().getHostAddress(), server.getLocalPort(), false),
          () -> new Credentials("fixture", password.toCharArray()),
          Duration.ofSeconds(1),
          Duration.ofSeconds(1),
          4,
          0,
          1024);
    }

    private void serve(Socket socket) {
      try (Socket ignored = socket;
          InputStream input = socket.getInputStream();
          OutputStream output = socket.getOutputStream()) {
        boolean authenticated = false;
        while (!socket.isClosed()) {
          String header = line(input);
          if (header == null) break;
          if (!header.startsWith("*")) throw new IOException("Expected RESP array");
          int count = Integer.parseInt(header.substring(1));
          if (count < 1 || count > 64) throw new IOException("Array limit");
          List<String> args = new ArrayList<>();
          for (int i = 0; i < count; i++) {
            String lengthLine = line(input);
            if (lengthLine == null || !lengthLine.startsWith("$"))
              throw new IOException("Expected bulk string");
            int length = Integer.parseInt(lengthLine.substring(1));
            if (length < 0 || length > 1024 * 1024) throw new IOException("Bulk limit");
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length || input.read() != '\r' || input.read() != '\n')
              throw new IOException("Truncated bulk string");
            args.add(new String(bytes, StandardCharsets.UTF_8));
          }
          String command = args.get(0).toUpperCase(Locale.ROOT), response;
          if (command.equals("AUTH")) {
            authenticated = password.equals(args.get(args.size() - 1));
            if (authenticated) authentications.incrementAndGet();
            response = authenticated ? "+OK\r\n" : "-WRONGPASS authentication failed\r\n";
          } else if (!authenticated) response = "-NOAUTH authentication required\r\n";
          else
            switch (command) {
              case "PING" -> response = "+PONG\r\n";
              case "CLIENT", "SELECT" -> response = "+OK\r\n";
              case "SET" -> {
                writes.incrementAndGet();
                values.put(args.get(1), args.get(2));
                response = "+OK\r\n";
              }
              case "GET" -> {
                if (args.get(1).equals("slow")) {
                  slowEntered.countDown();
                  try {
                    slowRelease.await(5, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
                String value = values.get(args.get(1));
                response =
                    value == null
                        ? "$-1\r\n"
                        : "$"
                            + value.getBytes(StandardCharsets.UTF_8).length
                            + "\r\n"
                            + value
                            + "\r\n";
              }
              case "DEL" -> response = ":" + (values.remove(args.get(1)) == null ? 0 : 1) + "\r\n";
              case "INCR" -> {
                String value =
                    values.compute(
                        args.get(1),
                        (key, old) -> Long.toString(old == null ? 1 : Long.parseLong(old) + 1));
                response = ":" + value + "\r\n";
              }
              default -> response = "-ERR unsupported fixture command\r\n";
            }
          output.write(response.getBytes(StandardCharsets.UTF_8));
          output.flush();
        }
      } catch (IOException | RuntimeException ignored) {
      } finally {
        sockets.remove(socket);
      }
    }

    private static String line(InputStream input) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      for (int value; (value = input.read()) != -1; ) {
        if (value == '\r') {
          if (input.read() != '\n') throw new IOException("Invalid line ending");
          return bytes.toString(StandardCharsets.US_ASCII);
        }
        if (bytes.size() >= 1024) throw new IOException("Line limit");
        bytes.write(value);
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      server.close();
      for (Socket socket : sockets) socket.close();
      acceptor.shutdownNow();
      workers.shutdownNow();
    }
  }
}
