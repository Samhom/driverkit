package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ClientRuntimeTest {
  @Test
  void reservesCapacityWhileInitializingWithoutBlockingShutdown() throws Exception {
    CountDownLatch warming = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger closes = new AtomicInteger();
    DriverAdapter<Object, String> adapter =
        new DriverAdapter<Object, String>() {
          public String id() {
            return "test";
          }

          public void validate(String settings) {}

          public Object create(String settings) {
            return new Object();
          }

          public void warmup(Object client, Duration timeout) throws Exception {
            warming.countDown();
            if (!release.await(3, TimeUnit.SECONDS)) throw new TimeoutException();
          }

          public void close(Object client) {
            closes.incrementAndGet();
          }
        };
    ResourceId resource = new ResourceId("example", "test", "cache");
    ConfigSnapshot<String> initial = new ConfigSnapshot<>(resource, 1, "one");
    ClientRuntime runtime = new ClientRuntime(1, 1, 1, Observer.NOOP);
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      Future<?> registration =
          workers.submit(() -> runtime.register(initial, adapter, ClientPolicy.defaults()));
      assertTrue(warming.await(1, TimeUnit.SECONDS));
      assertThrows(
          IllegalArgumentException.class,
          () -> runtime.register(initial, adapter, ClientPolicy.defaults()));
      ClientException overloaded =
          assertThrows(
              ClientException.class,
              () ->
                  runtime.register(
                      new ConfigSnapshot<>(new ResourceId("example", "test", "other"), 1, "one"),
                      adapter,
                      ClientPolicy.defaults()));
      assertEquals(ClientException.Kind.OVERLOADED, overloaded.kind());
      Future<?> shutdown = workers.submit(() -> runtime.close(Duration.ofMillis(50)));
      shutdown.get(1, TimeUnit.SECONDS);
      release.countDown();
      ExecutionException error =
          assertThrows(ExecutionException.class, () -> registration.get(1, TimeUnit.SECONDS));
      assertEquals(
          ClientException.Kind.CLOSED,
          assertInstanceOf(ClientException.class, error.getCause()).kind());
      long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (closes.get() != 1 && System.nanoTime() < cleanupDeadline) Thread.sleep(5);
      assertEquals(1, closes.get());
      assertEquals(0, runtime.clientCount());
    } finally {
      release.countDown();
      workers.shutdownNow();
      workers.awaitTermination(3, TimeUnit.SECONDS);
      runtime.close(Duration.ofMillis(50));
    }
  }
}
