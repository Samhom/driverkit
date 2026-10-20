package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class ConsumerLifecycleDeadlineTest {
  private static ConfigSnapshot<String> snapshot(long revision, String value) {
    return new ConfigSnapshot<>(new ResourceId("example", "test", "events"), revision, value);
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
  void timedOutReplacementCannotOverlapARecoveryConsumer() throws Exception {
    CountDownLatch entered = new CountDownLatch(1),
        release = new CountDownLatch(1),
        lateClosed = new CountDownLatch(1);
    ExclusiveConsumerTest.Adapter adapter =
        new ExclusiveConsumerTest.Adapter() {
          @Override
          public String start(String value, DeliveryGuard guard) {
            if (value.equals("slow")) {
              entered.countDown();
              waitIgnoringInterrupts(release);
            }
            return super.start(value, guard);
          }

          @Override
          public void close(String value) {
            super.close(value);
            if (value.equals("slow")) lateClosed.countDown();
          }
        };
    ExecutorService caller = Executors.newSingleThreadExecutor();
    ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(
            snapshot(1, "one"),
            adapter,
            new ClientPolicy(4, Duration.ofMillis(200), Duration.ofMillis(100)),
            Observer.NOOP);
    try {
      Future<?> replacement = caller.submit(() -> consumer.reload(snapshot(2, "slow")));
      assertTrue(entered.await(1, TimeUnit.SECONDS));
      assertThrows(ExecutionException.class, () -> replacement.get(1, TimeUnit.SECONDS));
      assertEquals(ExclusiveConsumer.State.FAILED, consumer.state());
      assertEquals(1, adapter.events.stream().filter(value -> value.equals("start:one")).count());
      assertEquals("retry", adapter.guard.invoke(() -> "unexpected", "retry"));
      release.countDown();
      assertTrue(lateClosed.await(1, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      caller.shutdownNow();
      consumer.close();
    }
  }

  @Test
  void consumerCloseReturnsWithinBudgetWhenNativeCloseIgnoresInterrupt() throws Exception {
    CountDownLatch entered = new CountDownLatch(1),
        release = new CountDownLatch(1),
        closed = new CountDownLatch(1);
    ExclusiveConsumerTest.Adapter adapter =
        new ExclusiveConsumerTest.Adapter() {
          @Override
          public void close(String value) {
            entered.countDown();
            waitIgnoringInterrupts(release);
            super.close(value);
            closed.countDown();
          }
        };
    ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(
            snapshot(1, "one"),
            adapter,
            new ClientPolicy(4, Duration.ofSeconds(1), Duration.ofMillis(100)),
            Observer.NOOP);
    try {
      long start = System.nanoTime();
      consumer.close();
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1));
      assertTrue(entered.await(1, TimeUnit.SECONDS));
      assertEquals(ExclusiveConsumer.State.CLOSED, consumer.state());
      assertEquals("retry", adapter.guard.invoke(() -> "unexpected", "retry"));
      release.countDown();
      assertTrue(closed.await(1, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      consumer.close();
    }
  }
}
