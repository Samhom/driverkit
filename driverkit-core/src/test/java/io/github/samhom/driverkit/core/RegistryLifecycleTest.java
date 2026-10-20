package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import org.junit.jupiter.api.Test;

class RegistryLifecycleTest {
  @Test
  void slowValidationCannotHoldCloseStateLockOrPublishIntoReplacementRegistration()
      throws Exception {
    CountDownLatch validating = new CountDownLatch(1), release = new CountDownLatch(1);
    DriverAdapter<String, String> slow =
        new DriverAdapter<String, String>() {
          public String id() {
            return "test";
          }

          public void validate(String value) {}

          public void validateUpdate(String previous, String next) {
            validating.countDown();
            ignoreInterrupts(release);
          }

          public String create(String value) {
            return value;
          }

          public void warmup(String value, Duration remaining) {}

          public void close(String value) {}
        };
    ExecutorService caller = Executors.newSingleThreadExecutor();
    ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP);
    try {
      ManagedClient<String, String> old = runtime.register(config(1, "old"), slow, policy());
      Future<?> validation = caller.submit(() -> old.reload(config(2, "stale")));
      assertTrue(validating.await(1, TimeUnit.SECONDS));
      old.close();
      assertEquals(0, runtime.clientCount());
      ManagedClient<String, String> current =
          runtime.register(config(1, "new"), adapter(), policy());
      release.countDown();
      assertThrows(ExecutionException.class, () -> validation.get(1, TimeUnit.SECONDS));
      assertEquals(
          "new",
          current.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client));
      assertEquals(1, runtime.clientCount());
    } finally {
      release.countDown();
      caller.shutdownNow();
      runtime.close(Duration.ofSeconds(1));
    }
  }

  private static final ResourceId ID = new ResourceId("example", "test", "cache");

  private static ConfigSnapshot<String> config(long revision, String value) {
    return new ConfigSnapshot<>(ID, revision, value);
  }

  private static ClientPolicy policy() {
    return new ClientPolicy(4, Duration.ofSeconds(3), Duration.ofMillis(100));
  }

  private static FactoryAdapter<String, String> adapter() {
    return new FactoryAdapter<>("test", value -> value, (value, remaining) -> {}, value -> {});
  }

  private static void eventually(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
    assertTrue(condition.getAsBoolean());
  }

  private static void ignoreInterrupts(CountDownLatch latch) {
    while (latch.getCount() != 0) {
      try {
        latch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
    }
  }

  @Test
  void repeatedCloseAndReregisterReusesCapacityAndCannotReviveOldHandles() {
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP)) {
      ManagedClient<String, String> previous = null;
      for (int iteration = 0; iteration < 20; iteration++) {
        ManagedClient<String, String> current =
            runtime.register(config(1, "native-" + iteration), adapter(), policy());
        if (previous != null) {
          previous.close();
          assertEquals(1, runtime.clientCount());
          assertThrows(ClientException.class, previous::session);
        }
        assertEquals(
            "native-" + iteration,
            current.execute(
                Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client));
        assertTrue(runtime.unregister(ID));
        assertEquals(0, runtime.clientCount());
        assertEquals(0, current.status().ownedResources());
        previous = current;
      }
      assertFalse(runtime.unregister(ID));
    }
  }

  @Test
  void closeAutomaticallyUnregistersWithoutExplicitRuntimeCall() {
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP)) {
      ManagedClient<String, String> handle =
          runtime.register(config(1, "native"), adapter(), policy());
      handle.close();
      assertEquals(0, runtime.clientCount());
      runtime.register(config(1, "replacement"), adapter(), policy());
    }
  }

  @Test
  void aliasAndCapacityRemainReservedUntilSlowNativeCleanupFinishes() throws Exception {
    CountDownLatch closing = new CountDownLatch(1), release = new CountDownLatch(1);
    ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP);
    try {
      ManagedClient<String, String> old =
          runtime.register(
              config(1, "old"),
              new FactoryAdapter<>(
                  "test",
                  value -> value,
                  (value, remaining) -> {},
                  value -> {
                    closing.countDown();
                    ignoreInterrupts(release);
                  }),
              policy());
      assertFalse(runtime.unregister(ID));
      assertTrue(closing.await(1, TimeUnit.SECONDS));
      assertEquals(1, runtime.clientCount());
      assertEquals(1, old.status().ownedResources());
      assertThrows(
          IllegalArgumentException.class,
          () -> runtime.register(config(1, "new"), adapter(), policy()));
      release.countDown();
      eventually(() -> runtime.clientCount() == 0);
      ManagedClient<String, String> current =
          runtime.register(config(1, "new"), adapter(), policy());
      old.close();
      assertEquals(1, runtime.clientCount());
      assertEquals(
          "new",
          current.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client));
    } finally {
      release.countDown();
      runtime.close(Duration.ofSeconds(1));
    }
  }

  @Test
  void detachedSourceMustActuallyCloseBeforeAliasCanBeReused() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Consumer<ConfigSnapshot<String>>> staleListener = new AtomicReference<>();
    ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP);
    try {
      ManagedClient<String, String> old = runtime.register(config(1, "old"), adapter(), policy());
      AutoCloseable unbind =
          old.bind(
              listener -> {
                staleListener.set(listener);
                return () -> ignoreInterrupts(release);
              });
      old.close();
      assertEquals(1, runtime.clientCount());
      assertEquals(1, old.status().ownedResources());
      release.countDown();
      eventually(() -> runtime.clientCount() == 0);
      ManagedClient<String, String> current =
          runtime.register(config(1, "new"), adapter(), policy());
      PushConfigurationSource<String> source = new PushConfigurationSource<>(config(1, "new"));
      current.bind(source);
      staleListener.get().accept(config(100, "stale"));
      unbind.close();
      old.close();
      source.publish(config(2, "current"));
      eventually(() -> current.status().revision() == 2);
      assertEquals(1, runtime.clientCount());
      assertEquals(
          "current",
          current.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client));
    } finally {
      release.countDown();
      runtime.close(Duration.ofSeconds(1));
    }
  }

  @Test
  void lateCandidateRemainsOwnedAfterHandleCloses() throws Exception {
    CountDownLatch warming = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicInteger closes = new AtomicInteger();
    ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      ManagedClient<String, String> old =
          runtime.register(
              config(1, "one"),
              new FactoryAdapter<>(
                  "test",
                  value -> value,
                  (value, remaining) -> {
                    if (value.equals("two")) {
                      warming.countDown();
                      ignoreInterrupts(release);
                    }
                  },
                  value -> closes.incrementAndGet()),
              policy());
      Future<?> update = caller.submit(() -> old.reload(config(2, "two")));
      assertTrue(warming.await(1, TimeUnit.SECONDS));
      old.close();
      assertEquals(1, runtime.clientCount());
      assertEquals(1, old.status().ownedResources());
      assertThrows(
          IllegalArgumentException.class,
          () -> runtime.register(config(1, "new"), adapter(), policy()));
      release.countDown();
      assertThrows(ExecutionException.class, () -> update.get(1, TimeUnit.SECONDS));
      eventually(() -> runtime.clientCount() == 0);
      assertEquals(2, closes.get());
      runtime.register(config(1, "new"), adapter(), policy());
      old.close();
      assertEquals(1, runtime.clientCount());
    } finally {
      release.countDown();
      caller.shutdownNow();
      runtime.close(Duration.ofSeconds(1));
    }
  }

  @Test
  void failedCleanupQuarantinesRegistrationInsteadOfSilentlyReusingIdentity() {
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP)) {
      ManagedClient<String, String> old =
          runtime.register(
              config(1, "old"),
              new FactoryAdapter<>(
                  "test",
                  value -> value,
                  (value, remaining) -> {},
                  value -> {
                    throw new IllegalStateException("fixture close failure");
                  }),
              policy());
      assertFalse(runtime.unregister(ID));
      assertEquals(1, runtime.clientCount());
      assertEquals(1, old.cleanupFailures());
      assertThrows(
          IllegalArgumentException.class,
          () -> runtime.register(config(1, "new"), adapter(), policy()));
    }
  }

  @Test
  void forcedOldSessionCannotAccessOrRemoveReplacementResource() {
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP)) {
      ManagedClient<String, String> old = runtime.register(config(1, "old"), adapter(), policy());
      ClientSession<String> pinned = old.session();
      old.close();
      assertThrows(ClientException.class, pinned::client);
      // close() may return at its deadline before asynchronous native cleanup's final callback.
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (runtime.clientCount() != 0 && System.nanoTime() < until) Thread.yield();
      assertEquals(0, runtime.clientCount());
      ManagedClient<String, String> current =
          runtime.register(config(1, "new"), adapter(), policy());
      pinned.close();
      old.close();
      assertEquals(1, runtime.clientCount());
      assertEquals(
          "new",
          current.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client));
    }
  }
}
