package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class ResourceIsolationTest {
  static final Duration SECOND = Duration.ofSeconds(1);

  static FactoryAdapter<String, String> adapter() {
    return new FactoryAdapter<>("fixture", s -> s, (c, t) -> {}, c -> {});
  }

  @Test
  void blockedResourceCannotOccupyAnotherResourcesCallWorkerAndQueueIsBounded() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try (ClientRuntime runtime = new ClientRuntime(2, 1, 2, Observer.NOOP)) {
      ManagedClient<String, String> slow =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("isolation", "test", "slow"), 1, "slow"),
              adapter(),
              new ClientPolicy(4, SECOND, SECOND, ExecutionPolicy.isolated(1, 1)));
      ManagedClient<String, String> fast =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("isolation", "test", "fast"), 1, "fast"),
              adapter(),
              ClientPolicy.defaults());
      CompletableFuture<String> blocked =
          slow.executeAsync(
              Operation.read("hold", Duration.ofSeconds(10)),
              (c, ctx) -> {
                entered.countDown();
                release.await();
                return CompletableFuture.completedFuture(c);
              });
      try {
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        CompletableFuture<String> queued =
            slow.executeAsync(
                Operation.read("queued", Duration.ofSeconds(10)),
                (c, ctx) -> CompletableFuture.completedFuture(c));
        ExecutionException rejected =
            assertThrows(
                ExecutionException.class,
                () ->
                    slow.executeAsync(
                            Operation.read("overflow", SECOND),
                            (c, ctx) -> CompletableFuture.completedFuture(c))
                        .get(2, TimeUnit.SECONDS));
        assertEquals(
            ClientException.Kind.OVERLOADED, ((ClientException) rejected.getCause()).kind());
        assertEquals(
            "fast",
            fast.executeAsync(
                    Operation.read("get", SECOND), (c, ctx) -> CompletableFuture.completedFuture(c))
                .get(2, TimeUnit.SECONDS));
        assertTrue(queued.cancel(false));
      } finally {
        release.countDown();
        blocked.get(2, TimeUnit.SECONDS);
      }
      slow.close();
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (runtime.isolatedThreadReservations() != 0 && System.nanoTime() < until)
        Thread.sleep(5);
      assertEquals(0, runtime.isolatedThreadReservations());
    } finally {
      release.countDown();
    }
  }

  @Test
  void breakerRejectsWithoutDispatchingOrReplayingNativeOperations() {
    AtomicInteger calls = new AtomicInteger();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("isolation", "test", "breaker"), 1, "native"),
              adapter(),
              new ClientPolicy(
                  4,
                  SECOND,
                  SECOND,
                  ExecutionPolicy.shared().withCircuitBreaker(2, Duration.ofSeconds(5))));
      for (int i = 0; i < 2; i++)
        assertThrows(
            ClientException.class,
            () ->
                handle.execute(
                    Operation.read("get", SECOND),
                    (c, ctx) -> {
                      calls.incrementAndGet();
                      throw new java.io.IOException("fixture");
                    }));
      ClientException open =
          assertThrows(
              ClientException.class,
              () ->
                  handle.execute(
                      Operation.read("get", SECOND),
                      (c, ctx) -> {
                        calls.incrementAndGet();
                        return c;
                      }));
      assertEquals(ClientException.Kind.CIRCUIT_OPEN, open.kind());
      assertEquals(2, calls.get());
      assertEquals(CircuitStatus.State.OPEN, handle.circuitStatus().state());
    }
  }

  @Test
  void halfOpenHasOneProbeAndIgnoresLateResultsFromAnOldEpoch() {
    AtomicLong time = new AtomicLong();
    CircuitBreaker breaker =
        new CircuitBreaker(ExecutionPolicy.shared().withCircuitBreaker(1, SECOND), time::get);
    CircuitBreaker.Permit late = breaker.acquire(), failure = breaker.acquire();
    failure.complete(new ClientException(ClientException.Kind.UNAVAILABLE, "fixture"));
    assertThrows(ClientException.class, breaker::acquire);
    time.addAndGet(SECOND.toNanos());
    CircuitBreaker.Permit probe = breaker.acquire();
    assertThrows(ClientException.class, breaker::acquire);
    probe.abandon();
    probe = breaker.acquire();
    probe.complete(null);
    late.complete(new ClientException(ClientException.Kind.UNAVAILABLE, "fixture"));
    assertEquals(CircuitStatus.State.CLOSED, breaker.status().state());
  }
}
