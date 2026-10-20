package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class ManagedClientTest {
  private static final ResourceId ID = new ResourceId("example", "test", "cache");

  private static ConfigSnapshot<String> config(long revision, String value) {
    return new ConfigSnapshot<>(ID, revision, value);
  }

  static final class Native {
    final String value;
    final AtomicInteger closes = new AtomicInteger();

    Native(String value) {
      this.value = value;
    }
  }

  static final class Adapter implements DriverAdapter<Native, String> {
    final List<Native> instances = new CopyOnWriteArrayList<>();

    @Override
    public String id() {
      return "test";
    }

    @Override
    public void validate(String settings) {
      if (settings.isEmpty()) throw new IllegalArgumentException("Empty settings");
    }

    @Override
    public Native create(String settings) {
      Native client = new Native(settings);
      instances.add(client);
      return client;
    }

    @Override
    public void warmup(Native client, Duration timeout) {
      if (client.value.equals("fail")) throw new IllegalStateException("Unavailable");
    }

    @Override
    public void close(Native client) {
      client.closes.incrementAndGet();
    }
  }

  private static void eventually(BooleanSupplier condition) throws Exception {
    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!condition.getAsBoolean() && System.nanoTime() < until) Thread.sleep(5);
    assertTrue(condition.getAsBoolean());
  }

  private static ClientException failure(CompletableFuture<?> future) {
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
    return assertInstanceOf(ClientException.class, error.getCause());
  }

  @Test
  void retiresOnlyAfterPinnedSessionFinishes() throws Exception {
    Adapter adapter = new Adapter();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), adapter, ClientPolicy.defaults());
      ClientSession<Native> old = handle.session();
      assertEquals(UpdateResult.APPLIED, handle.reload(config(2, "two")));
      try (ClientSession<Native> fresh = handle.session()) {
        assertEquals("two", fresh.client().value);
      }
      assertEquals(0, old.client().closes.get());
      assertEquals(UpdateResult.BUSY, handle.reload(config(3, "three")));
      Native oldClient = old.client();
      old.close();
      old.close();
      eventually(() -> oldClient.closes.get() == 1);
      assertEquals(UpdateResult.APPLIED, handle.reload(config(3, "three")));
    }
    assertTrue(adapter.instances.stream().allMatch(client -> client.closes.get() == 1));
  }

  @Test
  void rejectsStaleAndCrossResourceSnapshots() {
    Adapter adapter = new Adapter();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Native, String> handle =
          runtime.register(config(3, "three"), adapter, ClientPolicy.defaults());
      assertEquals(UpdateResult.STALE, handle.reload(config(2, "two")));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              handle.reload(
                  new ConfigSnapshot<>(new ResourceId("other", "test", "cache"), 4, "four")));
      assertEquals(1, adapter.instances.size());
    }
  }

  @Test
  void failedWarmupClosesCandidateAndPreservesActive() {
    Adapter adapter = new Adapter();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), adapter, ClientPolicy.defaults());
      assertThrows(ClientException.class, () -> handle.reload(config(2, "fail")));
      assertEquals(1, handle.status().revision());
      assertEquals(1, adapter.instances.get(1).closes.get());
      assertEquals(
          "one",
          handle.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> client.value));
    }
  }

  @Test
  void initialWarmupFailureDoesNotRegisterOrLeak() {
    Adapter adapter = new Adapter();
    try (ClientRuntime runtime = new ClientRuntime()) {
      assertThrows(
          ClientException.class,
          () -> runtime.register(config(1, "fail"), adapter, ClientPolicy.defaults()));
      assertEquals(0, runtime.clientCount());
      assertEquals(1, adapter.instances.get(0).closes.get());
    }
  }

  @Test
  void timeoutKeepsPhysicalLeaseAndMarksAmbiguousWrite() throws Exception {
    Adapter adapter = new Adapter();
    List<Observation.Outcome> outcomes = new CopyOnWriteArrayList<>();
    Observer observer =
        (resource, driver, operation, revision) -> (outcome, elapsed) -> outcomes.add(outcome);
    try (ClientRuntime runtime = new ClientRuntime(2, 2, 4, observer)) {
      ManagedClient<Native, String> handle =
          runtime.register(
              config(1, "one"),
              adapter,
              new ClientPolicy(1, Duration.ofSeconds(1), Duration.ofSeconds(1)));
      CompletableFuture<String> nativeResult = new CompletableFuture<>();
      CountDownLatch dispatched = new CountDownLatch(1);
      CompletableFuture<String> result =
          handle.executeAsync(
              Operation.write("increment", Duration.ofMillis(200)),
              (client, context) -> {
                dispatched.countDown();
                return nativeResult;
              });
      assertTrue(dispatched.await(1, TimeUnit.SECONDS));
      assertEquals(ClientException.Kind.WRITE_OUTCOME_UNKNOWN, failure(result).kind());
      assertEquals(1, handle.status().inFlight());
      assertEquals(UpdateResult.APPLIED, handle.reload(config(2, "two")));
      assertEquals(0, adapter.instances.get(0).closes.get());
      nativeResult.complete("late");
      eventually(() -> adapter.instances.get(0).closes.get() == 1);
      assertEquals(List.of(Observation.Outcome.TIMEOUT), outcomes);
    }
  }

  @Test
  void cancellationDoesNotPretendNativeRequestStopped() throws Exception {
    List<Observation.Outcome> outcomes = new CopyOnWriteArrayList<>();
    try (ClientRuntime runtime =
        new ClientRuntime(2, 1, 4, (r, d, o, v) -> (outcome, elapsed) -> outcomes.add(outcome))) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), new Adapter(), ClientPolicy.defaults());
      CompletableFuture<String> nativeResult = new CompletableFuture<>();
      CountDownLatch dispatched = new CountDownLatch(1);
      CompletableFuture<String> result =
          handle.executeAsync(
              Operation.read("get", Duration.ofSeconds(2)),
              (client, context) -> {
                dispatched.countDown();
                return nativeResult;
              });
      assertTrue(dispatched.await(1, TimeUnit.SECONDS));
      result.cancel(false);
      assertFalse(nativeResult.isCancelled());
      assertEquals(1, handle.status().inFlight());
      nativeResult.complete("late");
      eventually(() -> handle.status().inFlight() == 0);
      assertEquals(List.of(Observation.Outcome.CANCELLED), outcomes);
    }
  }

  @Test
  void bulkheadRejectsWithoutDispatching() throws Exception {
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Native, String> handle =
          runtime.register(
              config(1, "one"),
              new Adapter(),
              new ClientPolicy(1, Duration.ofSeconds(1), Duration.ofSeconds(1)));
      CompletableFuture<String> nativeResult = new CompletableFuture<>();
      CompletableFuture<String> first =
          handle.executeAsync(
              Operation.read("get", Duration.ofSeconds(2)), (client, context) -> nativeResult);
      AtomicBoolean invoked = new AtomicBoolean();
      CompletableFuture<String> second =
          handle.executeAsync(
              Operation.read("get", Duration.ofSeconds(2)),
              (client, context) -> {
                invoked.set(true);
                return nativeResult;
              });
      assertEquals(ClientException.Kind.OVERLOADED, failure(second).kind());
      assertFalse(invoked.get());
      nativeResult.complete("ok");
      assertEquals("ok", first.get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void queuedTimeoutNeverExecutesTheWrite() throws Exception {
    try (ClientRuntime runtime = new ClientRuntime(2, 1, 2, Observer.NOOP)) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), new Adapter(), ClientPolicy.defaults());
      CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
      CompletableFuture<String> first =
          handle.executeAsync(
              Operation.read("hold", Duration.ofSeconds(2)),
              (client, context) -> {
                entered.countDown();
                release.await();
                return CompletableFuture.completedFuture("ok");
              });
      assertTrue(entered.await(1, TimeUnit.SECONDS));
      AtomicBoolean invoked = new AtomicBoolean();
      CompletableFuture<String> queued =
          handle.executeAsync(
              Operation.write("set", Duration.ofMillis(30)),
              (client, context) -> {
                invoked.set(true);
                return CompletableFuture.completedFuture("bad");
              });
      assertEquals(ClientException.Kind.TIMEOUT, failure(queued).kind());
      release.countDown();
      first.get(1, TimeUnit.SECONDS);
      eventually(() -> handle.status().inFlight() == 0);
      assertFalse(invoked.get());
    }
  }

  @Test
  void revocationClosesAdmissionAndConfigurationCannotUndoIt() {
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), new Adapter(), ClientPolicy.defaults());
      handle.revoke();
      assertEquals(
          ClientException.Kind.REVOKED,
          assertThrows(ClientException.class, handle::session).kind());
      assertThrows(ClientException.class, () -> handle.reload(config(2, "two")));
    }
  }

  @Test
  void shutdownCompletesPendingCallWithinOneBudget() throws Exception {
    Adapter adapter = new Adapter();
    ClientRuntime runtime = new ClientRuntime();
    ManagedClient<Native, String> handle =
        runtime.register(config(1, "one"), adapter, ClientPolicy.defaults());
    CompletableFuture<String> never = new CompletableFuture<>();
    CompletableFuture<String> result =
        handle.executeAsync(
            Operation.read("get", Duration.ofMinutes(1)), (client, context) -> never);
    long start = System.nanoTime();
    runtime.close(Duration.ofMillis(50));
    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
    assertEquals(ClientException.Kind.CLOSED, failure(result).kind());
    eventually(() -> adapter.instances.get(0).closes.get() == 1);
  }

  @Test
  void configurationStormIsCoalescedBehindPinnedGeneration() throws Exception {
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), new Adapter(), ClientPolicy.defaults());
      PushConfigurationSource<String> source = new PushConfigurationSource<>(config(1, "one"));
      handle.bind(source);
      ClientSession<Native> pinned = handle.session();
      assertEquals(UpdateResult.APPLIED, handle.reload(config(2, "two")));
      for (int revision = 3; revision <= 1000; revision++)
        source.publish(config(revision, "latest"));
      assertEquals(1, handle.status().retiringGenerations());
      pinned.close();
      eventually(() -> handle.status().revision() == 1000);
    }
  }

  @Test
  void observerFailureCannotReplaceTheNativeResult() {
    try (ClientRuntime runtime =
        new ClientRuntime(
            2,
            1,
            2,
            (r, d, o, v) -> {
              throw new IllegalStateException("Exporter unavailable");
            })) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), new Adapter(), ClientPolicy.defaults());
      assertEquals(
          "value",
          handle.execute(
              Operation.read("get", Duration.ofSeconds(1)), (client, context) -> "value"));
    }
  }

  @Test
  void incompatibleObserverCannotLeakLeaseOrReplaceResult() throws Exception {
    Observer broken =
        (r, d, o, v) ->
            new Observation() {
              public AutoCloseable openScope() {
                throw new NoClassDefFoundError("Exporter dependency");
              }

              public void finish(Outcome outcome, long elapsed) {
                throw new NoSuchMethodError("Exporter method");
              }
            };
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 2, broken)) {
      ManagedClient<Native, String> handle =
          runtime.register(
              config(1, "one"),
              new Adapter(),
              new ClientPolicy(1, Duration.ofSeconds(1), Duration.ofSeconds(1)));
      for (int i = 0; i < 2; i++) {
        assertEquals(
            "value",
            handle.execute(
                Operation.read("get", Duration.ofSeconds(1)), (client, context) -> "value"));
        eventually(() -> handle.status().inFlight() == 0);
      }
    }
  }

  @Test
  void concurrentCallsAndReloadDoNotDoubleCloseOrLeak() throws Exception {
    Adapter adapter = new Adapter();
    try (ClientRuntime runtime = new ClientRuntime(2, 8, 128, Observer.NOOP)) {
      ManagedClient<Native, String> handle =
          runtime.register(config(1, "one"), adapter, ClientPolicy.defaults());
      ExecutorService workers = Executors.newFixedThreadPool(8);
      try {
        List<Future<?>> calls = new java.util.ArrayList<>();
        for (int worker = 0; worker < 8; worker++)
          calls.add(
              workers.submit(
                  () -> {
                    for (int i = 0; i < 100; i++)
                      handle.execute(
                          Operation.read("get", Duration.ofSeconds(3)),
                          (client, context) -> {
                            assertEquals(0, client.closes.get());
                            return client.value;
                          });
                  }));
        for (long revision = 2; revision < 20; revision++) {
          long next = revision;
          eventually(() -> handle.status().retiringGenerations() == 0);
          handle.reload(config(next, "value"));
        }
        for (Future<?> call : calls) call.get(10, TimeUnit.SECONDS);
      } finally {
        workers.shutdownNow();
      }
      eventually(() -> handle.status().inFlight() == 0);
    }
    assertTrue(adapter.instances.stream().allMatch(client -> client.closes.get() == 1));
  }
}
