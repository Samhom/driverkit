package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class LifecycleDeadlineTest {
  private final ResourceId id = new ResourceId("example", "test", "cache");

  private static void ignoreInterruptsUntil(CountDownLatch release) {
    boolean done = false;
    while (!done) {
      try {
        done = release.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
      if (!done && release.getCount() != 0) continue;
    }
  }

  private static void eventually(java.util.function.BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
    assertTrue(condition.getAsBoolean());
  }

  @Test
  void lateCreateIsBoundedAndItsResultIsClosed() throws Exception {
    lateInitialization(false);
  }

  @Test
  void lateWarmupIsBoundedAndItsResultIsClosed() throws Exception {
    lateInitialization(true);
  }

  private void lateInitialization(boolean warmup) throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicInteger closes = new AtomicInteger();
    DriverAdapter<String, String> adapter =
        new DriverAdapter<String, String>() {
          public String id() {
            return "test";
          }

          public void validate(String settings) {}

          public String create(String settings) {
            if (!warmup) {
              entered.countDown();
              ignoreInterruptsUntil(release);
            }
            return settings;
          }

          public void warmup(String client, Duration timeout) {
            if (warmup) {
              entered.countDown();
              ignoreInterruptsUntil(release);
            }
          }

          public void close(String client) {
            closes.incrementAndGet();
          }
        };
    ExecutorService caller = Executors.newSingleThreadExecutor();
    ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP);
    try {
      Future<?> result =
          caller.submit(
              () ->
                  runtime.register(
                      new ConfigSnapshot<>(id, 1, "native"),
                      adapter,
                      new ClientPolicy(1, Duration.ofMillis(150), Duration.ofSeconds(1))));
      assertTrue(entered.await(1, TimeUnit.SECONDS));
      ExecutionException error =
          assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
      assertEquals(
          ClientException.Kind.TIMEOUT,
          assertInstanceOf(ClientException.class, error.getCause()).kind());
      assertEquals(0, runtime.clientCount());
      runtime.close(Duration.ofMillis(30));
      assertEquals(0, closes.get());
      release.countDown();
      eventually(() -> closes.get() == 1);
    } finally {
      release.countDown();
      caller.shutdownNow();
      runtime.close(Duration.ofMillis(30));
    }
  }

  @Test
  void sharedBudgetShrinksBetweenCreateAndWarmupAndReachesNativeClose() throws Exception {
    AtomicReference<Duration> creation = new AtomicReference<>(),
        warmup = new AtomicReference<>(),
        closing = new AtomicReference<>();
    DriverAdapter<String, String> adapter =
        new DriverAdapter<String, String>() {
          public String id() {
            return "test";
          }

          public void validate(String settings) {}

          public String create(String settings) {
            throw new AssertionError("Budget overload required");
          }

          public String create(String settings, Duration remaining) throws Exception {
            creation.set(remaining);
            Thread.sleep(30);
            return settings;
          }

          public void warmup(String client, Duration remaining) {
            warmup.set(remaining);
          }

          public void close(String client) {
            throw new AssertionError("Budget overload required");
          }

          public void close(String client, Duration remaining) {
            closing.set(remaining);
          }
        };
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(id, 1, "native"),
              adapter,
              new ClientPolicy(1, Duration.ofSeconds(2), Duration.ofSeconds(1)));
      assertTrue(creation.get().compareTo(Duration.ofSeconds(2)) <= 0);
      assertTrue(warmup.get().compareTo(creation.get()) < 0);
      handle.close();
      assertTrue(closing.get().compareTo(Duration.ZERO) > 0);
      assertTrue(closing.get().compareTo(Duration.ofSeconds(1)) <= 0);
    }
  }

  @Test
  void repeatedTimeoutsCannotGrowUninterruptibleInitializations() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger creates = new AtomicInteger(), closes = new AtomicInteger();
    ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP);
    FactoryAdapter<String, String> adapter =
        new FactoryAdapter<>(
            "test",
            value -> {
              creates.incrementAndGet();
              ignoreInterruptsUntil(release);
              return value;
            },
            (value, remaining) -> {},
            value -> closes.incrementAndGet());
    try {
      for (int attempt = 0; attempt < 6; attempt++) {
        ClientException error =
            assertThrows(
                ClientException.class,
                () ->
                    runtime.register(
                        new ConfigSnapshot<>(id, 1, "native"),
                        adapter,
                        new ClientPolicy(1, Duration.ofMillis(100), Duration.ofSeconds(1))));
        assertEquals(ClientException.Kind.TIMEOUT, error.kind());
      }
      assertTrue(creates.get() <= 2);
      assertEquals(0, runtime.clientCount());
      runtime.close(Duration.ofMillis(30));
      release.countDown();
      eventually(() -> closes.get() == creates.get());
    } finally {
      release.countDown();
      runtime.close(Duration.ofMillis(30));
    }
  }

  @Test
  void slowSubscriptionDetachCannotBlockShutdown() throws Exception {
    CountDownLatch release = new CountDownLatch(1), detached = new CountDownLatch(1);
    ClientRuntime runtime = new ClientRuntime();
    try {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(id, 1, "native"),
              new FactoryAdapter<>("test", value -> value, (value, remaining) -> {}, value -> {}),
              ClientPolicy.defaults());
      handle.bind(
          listener ->
              () -> {
                ignoreInterruptsUntil(release);
                detached.countDown();
              });
      long start = System.nanoTime();
      runtime.close(Duration.ofMillis(50));
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1));
      assertEquals(1, detached.getCount());
      release.countDown();
      assertTrue(detached.await(1, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      runtime.close(Duration.ofMillis(30));
    }
  }

  @Test
  void blockingNativeCloseCannotExceedRuntimeShutdownBudget() throws Exception {
    CountDownLatch closeEntered = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicInteger closes = new AtomicInteger();
    ClientRuntime runtime = new ClientRuntime();
    try {
      runtime.register(
          new ConfigSnapshot<>(id, 1, "native"),
          new FactoryAdapter<>(
              "test",
              value -> value,
              (value, timeout) -> {},
              value -> {
                closeEntered.countDown();
                ignoreInterruptsUntil(release);
                closes.incrementAndGet();
              }),
          ClientPolicy.defaults());
      long start = System.nanoTime();
      runtime.close(Duration.ofMillis(50));
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1));
      assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
      assertEquals(0, closes.get());
      release.countDown();
      eventually(() -> closes.get() == 1);
    } finally {
      release.countDown();
      runtime.close(Duration.ofMillis(30));
    }
  }
}
