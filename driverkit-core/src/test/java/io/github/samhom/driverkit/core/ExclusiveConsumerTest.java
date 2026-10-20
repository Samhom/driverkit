package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExclusiveConsumerTest {
  private static final ResourceId RESOURCE = new ResourceId("example", "test", "events");

  private static ConfigSnapshot<String> snapshot(long revision, String settings) {
    return new ConfigSnapshot<>(RESOURCE, revision, settings);
  }

  static class Adapter implements ConsumerAdapter<String, String> {
    final List<String> events = new CopyOnWriteArrayList<>();
    volatile DeliveryGuard guard;

    @Override
    public String id() {
      return "test-consumer";
    }

    @Override
    public void validate(String settings) {}

    @Override
    public String start(String settings, DeliveryGuard guard) {
      this.guard = guard;
      events.add("start:" + settings);
      if (settings.equals("fail")) throw new IllegalStateException("Unavailable");
      return settings;
    }

    @Override
    public void pause(String consumer) {
      events.add("pause:" + consumer);
    }

    @Override
    public void resume(String consumer) {
      events.add("resume:" + consumer);
    }

    @Override
    public void close(String consumer) {
      events.add("close:" + consumer);
    }
  }

  private static ClientPolicy policy() {
    return new ClientPolicy(4, Duration.ofSeconds(1), Duration.ofMillis(100));
  }

  @Test
  void replacementStopsOldGroupMemberBeforeStartingNew() {
    Adapter adapter = new Adapter();
    try (ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(snapshot(1, "one"), adapter, policy(), Observer.NOOP)) {
      assertEquals(UpdateResult.APPLIED, consumer.reload(snapshot(2, "two")));
      assertEquals(List.of("start:one", "pause:one", "close:one", "start:two"), adapter.events);
      assertEquals(UpdateResult.STALE, consumer.reload(snapshot(1, "one")));
    }
  }

  @Test
  void unsuccessfulReplacementRebuildsPreviousConfiguration() {
    Adapter adapter = new Adapter();
    try (ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(snapshot(1, "one"), adapter, policy(), Observer.NOOP)) {
      assertThrows(ClientException.class, () -> consumer.reload(snapshot(2, "fail")));
      assertEquals(ExclusiveConsumer.State.RUNNING, consumer.state());
      assertEquals(1, consumer.revision());
      assertEquals(
          List.of("start:one", "pause:one", "close:one", "start:fail", "start:one"),
          adapter.events);
    }
  }

  @Test
  void drainTimeoutPreservesOriginalConsumerAndDoesNotOverlapMembers() throws Exception {
    Adapter adapter = new Adapter();
    try (ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(snapshot(1, "one"), adapter, policy(), Observer.NOOP)) {
      CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<String> processing =
            executor.submit(
                () ->
                    adapter.guard.invoke(
                        () -> {
                          entered.countDown();
                          release.await();
                          return "ok";
                        },
                        "retry"));
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        assertEquals(
            ClientException.Kind.TIMEOUT,
            assertThrows(ClientException.class, () -> consumer.reload(snapshot(2, "two"))).kind());
        assertEquals(ExclusiveConsumer.State.RUNNING, consumer.state());
        assertFalse(adapter.events.contains("start:two"));
        assertFalse(adapter.events.contains("close:one"));
        release.countDown();
        assertEquals("ok", processing.get(1, TimeUnit.SECONDS));
      } finally {
        release.countDown();
        executor.shutdownNow();
      }
    }
  }

  @Test
  void callbacksProduceRetryOnExceptionAndAfterRevocation() {
    Adapter adapter = new Adapter();
    List<Observation.Outcome> outcomes = new ArrayList<>();
    try (ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(
            snapshot(1, "one"),
            adapter,
            policy(),
            (r, d, o, v) -> (result, elapsed) -> outcomes.add(result))) {
      assertEquals(
          "retry",
          adapter.guard.invoke(
              () -> {
                throw new IllegalArgumentException("business failure");
              },
              "retry"));
      assertEquals(Observation.Outcome.ERROR, outcomes.get(0));
      consumer.revoke();
      AtomicInteger calls = new AtomicInteger();
      assertEquals(
          "retry",
          adapter.guard.invoke(
              () -> {
                calls.incrementAndGet();
                return "ok";
              },
              "retry"));
      assertEquals(0, calls.get());
      assertEquals(ExclusiveConsumer.State.REVOKED, consumer.state());
    }
  }

  @Test
  void rejectsConfigurationFromDifferentResource() {
    try (ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(snapshot(1, "one"), new Adapter(), policy(), Observer.NOOP)) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              consumer.reload(
                  new ConfigSnapshot<>(new ResourceId("other", "test", "events"), 2, "two")));
    }
  }

  @Test
  void localRatePolicyRejectsAfterConfiguredBurst() {
    RateLimitPolicy limiter =
        new RateLimitPolicy(Map.of(RESOURCE, new RateLimitPolicy.Limit(1, 1)));
    limiter.check(RESOURCE, Operation.read("get", Duration.ofSeconds(1)));
    assertEquals(
        ClientException.Kind.OVERLOADED,
        assertThrows(
                ClientException.class,
                () -> limiter.check(RESOURCE, Operation.read("get", Duration.ofSeconds(1))))
            .kind());
  }
}
