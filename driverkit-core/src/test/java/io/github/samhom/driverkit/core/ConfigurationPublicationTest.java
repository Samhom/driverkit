package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class ConfigurationPublicationTest {
  static final ResourceId ID = new ResourceId("publication", "test", "resource");

  static ConfigSnapshot<String> config(long revision, String value) {
    return new ConfigSnapshot<>(ID, revision, value);
  }

  @Test
  void consumerRecoveryKeepsOriginalFailurePhaseAndEffectiveVersion() {
    ConsumerAdapter<String, String> adapter =
        new ConsumerAdapter<>() {
          public String id() {
            return "consumer-fixture";
          }

          public void validate(String settings) {}

          public String start(String settings, DeliveryGuard guard) {
            if (settings.equals("bad")) throw new IllegalStateException("fixture");
            return settings;
          }

          public void pause(String c) {}

          public void resume(String c) {}

          public void close(String c) {}
        };
    try (ExclusiveConsumer<String, String> consumer =
        new ExclusiveConsumer<>(
            config(1, "good"), adapter, ClientPolicy.defaults(), Observer.NOOP)) {
      assertThrows(ClientException.class, () -> consumer.reload(config(2, "bad")));
      assertEquals(ExclusiveConsumer.State.RUNNING, consumer.state());
      assertEquals(1, consumer.configurationStatus().effectiveRevision());
      assertEquals(2, consumer.configurationStatus().attemptedRevision());
      assertEquals(ConfigurationStatus.Phase.CREATE, consumer.configurationStatus().failedPhase());
      assertThrows(ClientException.class, () -> consumer.reload(config(3, "good"), 999));
      assertEquals(
          ConfigurationStatus.Phase.VALIDATION, consumer.configurationStatus().failedPhase());
    }
  }

  @Test
  void failedWarmupReportsSanitizedPhaseAndRetainsTheEffectiveRevision() {
    FactoryAdapter<String, String> adapter =
        new FactoryAdapter<>(
            "fixture",
            s -> s,
            (client, timeout) -> {
              if (client.equals("bad"))
                throw new IllegalStateException("fixture private connection detail");
            },
            c -> {});
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> handle =
          runtime.register(config(1, "good"), adapter, ClientPolicy.defaults());
      assertThrows(ClientException.class, () -> handle.reload(config(2, "bad"), 1));
      ConfigurationStatus status = handle.configurationStatus();
      assertEquals(2, status.desiredRevision());
      assertEquals(2, status.attemptedRevision());
      assertEquals(1, status.effectiveRevision());
      assertEquals(ConfigurationStatus.Phase.FAILED, status.phase());
      assertEquals(ConfigurationStatus.Phase.WARMUP, status.failedPhase());
      assertEquals(ClientException.Kind.UPDATE_REJECTED, status.failureKind());
      assertTrue(status.elapsedNanos() > 0);
      assertEquals(UpdateResult.STALE, handle.reload(config(2, "good")));
      assertThrows(ClientException.class, () -> handle.reload(config(3, "good"), 2));
      assertEquals(
          ConfigurationStatus.Phase.VALIDATION, handle.configurationStatus().failedPhase());
      assertEquals(1, handle.configurationStatus().effectiveRevision());
      assertEquals(UpdateResult.APPLIED, handle.reload(config(4, "good"), 1));
      assertEquals(4, handle.configurationStatus().effectiveRevision());
    }
  }

  @Test
  void statusRemainsReadableWhileCreateIsBlockedAndHistoryIsBounded() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    FactoryAdapter<String, String> adapter =
        new FactoryAdapter<>(
            "fixture",
            value -> {
              if (value.equals("blocked")) {
                entered.countDown();
                release.await(3, TimeUnit.SECONDS);
              }
              return value;
            },
            (c, t) -> {},
            c -> {});
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> handle =
          runtime.register(config(1, "initial"), adapter, ClientPolicy.defaults());
      Future<?> update = worker.submit(() -> handle.reload(config(2, "blocked")));
      assertTrue(entered.await(1, TimeUnit.SECONDS));
      assertEquals(ConfigurationStatus.Phase.CREATE, handle.configurationStatus().phase());
      assertEquals(1, handle.configurationStatus().effectiveRevision());
      release.countDown();
      update.get(2, TimeUnit.SECONDS);
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (handle.status().retiringGenerations() != 0 && System.nanoTime() < until)
        Thread.yield();
      for (int revision = 3; revision < 80; revision++)
        assertThrows(
            ClientException.class,
            () ->
                handle.reload(
                    config(handle.configurationStatus().attemptedRevision() + 1, "unchanged"),
                    999));
      assertEquals(64, handle.configurationHistory().size());
      assertThrows(
          UnsupportedOperationException.class, () -> handle.configurationHistory().clear());
    } finally {
      release.countDown();
      worker.shutdownNow();
    }
  }

  @Test
  void rolloutSelectionIsStableMonotonicAndDoesNotAdvanceSkippedInstances() {
    ConfigurationRelease<String> none =
        new ConfigurationRelease<>(config(2, "next"), 1, "release", 0);
    ConfigurationRelease<String> all =
        new ConfigurationRelease<>(config(2, "next"), 1, "release", 100);
    ConfigurationRelease<String> canary =
        new ConfigurationRelease<>(config(2, "next"), 1, "release", 10);
    ConfigurationRelease<String> wider =
        new ConfigurationRelease<>(config(2, "next"), 1, "release", 50);
    int selected = 0;
    for (int i = 0; i < 100; i++) {
      String instance = "node-" + i;
      assertFalse(none.targets(instance));
      assertTrue(all.targets(instance));
      assertEquals(canary.targets(instance), canary.targets(instance));
      if (canary.targets(instance)) {
        selected++;
        assertTrue(wider.targets(instance));
      }
    }
    assertTrue(selected > 0 && selected < 30);
    PushConfigurationSource<String> source = new PushConfigurationSource<>(config(1, "initial"));
    assertFalse(source.publish(none, "node-1"));
    assertTrue(source.publish(all, "node-1"));
    assertThrows(ClientException.class, () -> source.publish(all, "node-1"));
  }
}
