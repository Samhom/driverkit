package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LifecycleIsolationTest {
  private static ConfigSnapshot<String> config(String name, long revision, String value) {
    return new ConfigSnapshot<>(new ResourceId("example", "test", name), revision, value);
  }

  private static void waitIgnoringInterrupts(CountDownLatch release) {
    while (release.getCount() != 0) {
      try {
        release.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
    }
  }

  @Test
  void nativeCleanupContinuesWhileBothConfigurationRebuildsAndInitializersAreBlocked()
      throws Exception {
    CountDownLatch warming = new CountDownLatch(2),
        release = new CountDownLatch(1),
        closed = new CountDownLatch(1);
    FactoryAdapter<String, String> adapter =
        new FactoryAdapter<>(
            "test",
            value -> value,
            (value, remaining) -> {
              if (value.startsWith("blocked")) {
                warming.countDown();
                waitIgnoringInterrupts(release);
              }
            },
            value -> {
              if (value.equals("third")) closed.countDown();
            });
    ClientRuntime runtime = new ClientRuntime(3, 2, 4, Observer.NOOP);
    try {
      ClientPolicy policy = new ClientPolicy(4, Duration.ofSeconds(5), Duration.ofMillis(300));
      ManagedClient<String, String> first =
          runtime.register(config("first", 1, "first"), adapter, policy);
      ManagedClient<String, String> second =
          runtime.register(config("second", 1, "second"), adapter, policy);
      ManagedClient<String, String> third =
          runtime.register(config("third", 1, "third"), adapter, policy);
      PushConfigurationSource<String> a =
          new PushConfigurationSource<>(config("first", 1, "first"));
      PushConfigurationSource<String> b =
          new PushConfigurationSource<>(config("second", 1, "second"));
      first.bind(a);
      second.bind(b);
      a.publish(config("first", 2, "blocked-first"));
      b.publish(config("second", 2, "blocked-second"));
      assertTrue(warming.await(2, TimeUnit.SECONDS));
      third.close();
      assertTrue(closed.await(1, TimeUnit.SECONDS));
      assertEquals(1, first.status().revision());
      assertEquals(
          "second",
          second.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client));
    } finally {
      release.countDown();
      runtime.close(Duration.ofSeconds(1));
    }
  }

  @Test
  void shutdownMaintenanceNeverFallsBackToNativeCloseUnderStateLock() throws Exception {
    CountDownLatch closeEntered = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicReference<String> closeThread = new AtomicReference<>();
    ClientRuntime runtime = new ClientRuntime();
    try {
      ManagedClient<String, String> handle =
          runtime.register(
              config("cache", 1, "native"),
              new FactoryAdapter<>(
                  "test",
                  value -> value,
                  (value, remaining) -> {},
                  value -> {
                    closeThread.set(Thread.currentThread().getName());
                    closeEntered.countDown();
                    waitIgnoringInterrupts(release);
                  }),
              new ClientPolicy(1, Duration.ofSeconds(1), Duration.ofMillis(50)));
      runtime.maintenance.shutdown();
      handle.close();
      assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
      assertTrue(closeThread.get().startsWith("driverkit-cleanup-"));
      ExecutorService probe = Executors.newSingleThreadExecutor();
      try {
        assertTrue(probe.submit(() -> handle.status().closed()).get(1, TimeUnit.SECONDS));
      } finally {
        probe.shutdownNow();
      }
    } finally {
      release.countDown();
      runtime.close(Duration.ofSeconds(1));
    }
  }
}
