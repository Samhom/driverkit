package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class QueueReclamationTest {
  @Test
  void timedOutQueuedWriteReleasesCapacityBeforeBlockedWorkerReturns() throws Exception {
    exercise(false);
  }

  @Test
  void cancelledQueuedWriteReleasesCapacityBeforeBlockedWorkerReturns() throws Exception {
    exercise(true);
  }

  private void exercise(boolean cancel) throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 1, Observer.NOOP)) {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("example", "test", "cache"), 1, "native"),
              new FactoryAdapter<>("test", value -> value, (value, timeout) -> {}, value -> {}),
              new ClientPolicy(2, Duration.ofSeconds(1), Duration.ofSeconds(1)));
      try {
        CompletableFuture<String> blocked =
            handle.executeAsync(
                Operation.read("hold", Duration.ofSeconds(5)),
                (client, context) -> {
                  entered.countDown();
                  release.await();
                  return CompletableFuture.completedFuture("first");
                });
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        AtomicBoolean invoked = new AtomicBoolean();
        CompletableFuture<String> expired =
            handle.executeAsync(
                Operation.write("set", cancel ? Duration.ofSeconds(3) : Duration.ofMillis(100)),
                (client, context) -> {
                  invoked.set(true);
                  return CompletableFuture.completedFuture("unexpected");
                });
        assertEquals(1, runtime.calls.getQueue().size());
        if (cancel) assertTrue(expired.cancel(false));
        else {
          ExecutionException error =
              assertThrows(ExecutionException.class, () -> expired.get(2, TimeUnit.SECONDS));
          assertEquals(
              ClientException.Kind.TIMEOUT,
              assertInstanceOf(ClientException.class, error.getCause()).kind());
        }
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (handle.status().inFlight() != 1 && System.nanoTime() < until) Thread.yield();
        assertEquals(1, handle.status().inFlight());
        assertTrue(runtime.calls.getQueue().isEmpty());
        assertFalse(invoked.get());
        CompletableFuture<String> replacement =
            handle.executeAsync(
                Operation.read("replacement", Duration.ofSeconds(3)),
                (client, context) -> CompletableFuture.completedFuture("replacement"));
        assertFalse(replacement.isDone());
        assertEquals(1, runtime.calls.getQueue().size());
        release.countDown();
        assertEquals("first", blocked.get(2, TimeUnit.SECONDS));
        assertEquals("replacement", replacement.get(2, TimeUnit.SECONDS));
        assertFalse(invoked.get());
      } finally {
        release.countDown();
      }
    }
  }
}
