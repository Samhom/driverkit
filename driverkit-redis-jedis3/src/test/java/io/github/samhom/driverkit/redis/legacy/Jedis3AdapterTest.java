package io.github.samhom.driverkit.redis.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.redis.RedisSettings;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class Jedis3AdapterTest {
  @Test
  void nativePoolAuthenticatesAndWarmsUpWithoutNewGenerationClasses() throws Exception {
    assertThrows(
        ClassNotFoundException.class, () -> Class.forName("redis.clients.jedis.RedisClient"));
    String password = UUID.randomUUID().toString();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(3000);
      Future<?> accepted =
          executor.submit(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.setSoTimeout(3000);
                  var input =
                      new BufferedReader(
                          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                  var output = socket.getOutputStream();
                  for (int commands = 0; commands < 2; commands++) {
                    String count = input.readLine();
                    if (count == null) break;
                    int size = Integer.parseInt(count.substring(1));
                    if (size < 1 || size > 8) throw new IOException("Fixture command limit");
                    List<String> parts = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                      String length = input.readLine();
                      if (length == null || Integer.parseInt(length.substring(1)) > 256)
                        throw new IOException("Fixture argument limit");
                      parts.add(input.readLine());
                    }
                    if (parts.get(0).equalsIgnoreCase("AUTH")) {
                      assertTrue(
                          List.of("AUTH", "fixture", password).equals(parts),
                          "Expected ephemeral authentication exchange");
                      output.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
                    } else if (parts.get(0).equalsIgnoreCase("PING"))
                      output.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
                    else throw new IOException("Unexpected fixture command");
                    output.flush();
                  }
                } catch (IOException failure) {
                  throw new UncheckedIOException(failure);
                }
              });
      var settings =
          new RedisSettings(
              new Endpoint("127.0.0.1", server.getLocalPort(), false),
              () -> new Credentials("fixture", password.toCharArray()),
              Duration.ofSeconds(1),
              Duration.ofSeconds(1),
              1,
              0,
              1024);
      var adapter = new Jedis3Adapter();
      var pool = adapter.create(settings);
      try {
        adapter.warmup(pool, Duration.ofSeconds(1));
      } finally {
        adapter.close(pool);
      }
      accepted.get(5, TimeUnit.SECONDS);
      assertTrue(pool.isClosed());
    } finally {
      executor.shutdownNow();
    }
  }
}
