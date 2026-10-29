package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class CredentialLifecycleTest {
  record Settings(CredentialsProvider credentials) implements ImmutableSettings {}

  static final ResourceId ID = new ResourceId("credentials", "test", "resource");

  static CredentialState state(long version) {
    return new CredentialState(
        version, Instant.now().plusSeconds(8), Instant.now().plusSeconds(10), false);
  }

  static CredentialsProvider resolver(String user) {
    char[] generated = UUID.randomUUID().toString().toCharArray();
    return () -> new Credentials(user, generated);
  }

  static void await(BooleanSupplier condition) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(5);
    assertTrue(condition.getAsBoolean());
  }

  static class Adapter implements DriverAdapter<String, Settings> {
    public String id() {
      return "fixture";
    }

    public void validate(Settings s) {}

    public CredentialsProvider credentials(Settings s) {
      return s.credentials;
    }

    public String create(Settings s) {
      try (Credentials c = s.credentials.resolve()) {
        return c.username();
      }
    }

    public void warmup(String c, Duration t) {}

    public void close(String c) {}
  }

  @Test
  void rotatesWithoutStealingConfigurationRevisionsAndRevokesAdmission() throws Exception {
    RotatingCredentialsProvider source =
        new RotatingCredentialsProvider(resolver("one"), state(1), () -> {});
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, Settings> handle =
          runtime.register(
              new ConfigSnapshot<>(ID, 1, new Settings(source)),
              new Adapter(),
              ClientPolicy.defaults());
      handle.bindCredentials(source);
      await(() -> handle.credentialStatus().state() == CredentialStatus.State.ACTIVE);
      source.rotate(resolver("two"), state(2));
      await(() -> handle.credentialStatus().activeVersion() == 2);
      assertEquals(1, handle.status().revision());
      assertEquals(
          "two", handle.execute(Operation.read("get", Duration.ofSeconds(1)), (c, ctx) -> c));
      source.revoke(3);
      await(() -> handle.status().revoked());
      assertThrows(ClientException.class, handle::session);
      assertThrows(ClientException.class, source::resolve);
      assertThrows(ClientException.class, () -> source.rotate(resolver("three"), state(4)));
    }
  }

  @Test
  void expiryClosesAdmissionEvenWhenTheRefreshHookIsBlocked() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    RotatingCredentialsProvider source =
        new RotatingCredentialsProvider(
            resolver("one"),
            state(1),
            () -> {
              entered.countDown();
              boolean done = false;
              while (!done)
                try {
                  done = release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
            });
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, Settings> handle =
          runtime.register(
              new ConfigSnapshot<>(ID, 1, new Settings(source)),
              new Adapter(),
              new ClientPolicy(2, Duration.ofSeconds(3), Duration.ofSeconds(1)));
      handle.bindCredentials(source);
      await(() -> handle.credentialStatus().state() == CredentialStatus.State.ACTIVE);
      source.rotate(
          resolver("one"),
          new CredentialState(
              2, Instant.now().plusMillis(100), Instant.now().plusMillis(800), false));
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      await(() -> handle.status().revoked());
      assertThrows(ClientException.class, handle::session);
      release.countDown();
    } finally {
      release.countDown();
    }
  }

  @Test
  void consumerCredentialRotationNeverOverlapsGroupMembers() throws Exception {
    AtomicInteger members = new AtomicInteger();
    RotatingCredentialsProvider source =
        new RotatingCredentialsProvider(resolver("one"), state(1), () -> {});
    ConsumerAdapter<String, Settings> adapter =
        new ConsumerAdapter<>() {
          public String id() {
            return "consumer";
          }

          public void validate(Settings s) {}

          public CredentialsProvider credentials(Settings s) {
            return s.credentials;
          }

          public String start(Settings s, DeliveryGuard g) {
            assertEquals(1, members.incrementAndGet());
            try (Credentials c = s.credentials.resolve()) {
              return c.username();
            }
          }

          public void close(String c) {
            members.decrementAndGet();
          }

          public void pause(String c) {}

          public void resume(String c) {}
        };
    try (ExclusiveConsumer<String, Settings> consumer =
        new ExclusiveConsumer<>(
            new ConfigSnapshot<>(ID, 1, new Settings(source)),
            adapter,
            ClientPolicy.defaults(),
            Observer.NOOP)) {
      consumer.bindCredentials(source);
      await(() -> consumer.credentialStatus().state() == CredentialStatus.State.ACTIVE);
      source.rotate(resolver("two"), state(2));
      await(() -> consumer.credentialStatus().activeVersion() == 2);
      assertEquals(1, consumer.revision());
      assertEquals(1, members.get());
    }
    assertEquals(0, members.get());
  }
}
