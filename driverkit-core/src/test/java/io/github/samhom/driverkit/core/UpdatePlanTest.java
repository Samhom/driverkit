package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class UpdatePlanTest {
  record Settings(int limit, String binding, Duration timeout) implements ImmutableSettings {}

  static final ResourceId ID = new ResourceId("updates", "test", "resource");

  static class Adapter implements DriverAdapter<Object, Settings> {
    final AtomicInteger created = new AtomicInteger();

    public String id() {
      return "fixture";
    }

    public void validate(Settings s) {
      if (s.limit < 1) throw new IllegalArgumentException();
    }

    public Object create(Settings s) {
      created.incrementAndGet();
      return new Object();
    }

    public void warmup(Object c, Duration t) {}

    public void close(Object c) {}

    public Duration operationTimeout(Settings s, Operation op) {
      return s.timeout;
    }

    public UpdatePlan updatePlan(Settings a, Settings b) {
      return UpdatePlan.builder()
          .field("limit", a.limit, b.limit, UpdatePlan.Effect.LIVE)
          .field("binding", a.binding, b.binding, UpdatePlan.Effect.FORBIDDEN)
          .field("timeout", a.timeout, b.timeout, UpdatePlan.Effect.REBUILD)
          .build();
    }
  }

  @Test
  void liveChangeKeepsNativeOwnershipAndExistingLeasesKeepTheirOldSnapshot() {
    Adapter adapter = new Adapter();
    Settings old = new Settings(8, "fixed", Duration.ofSeconds(1));
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Object, Settings> handle =
          runtime.register(new ConfigSnapshot<>(ID, 1, old), adapter, ClientPolicy.defaults());
      try (ClientSession<Object> pinned = handle.session()) {
        ConfigSnapshot<Settings> next =
            new ConfigSnapshot<>(ID, 2, new Settings(2, "fixed", old.timeout));
        assertEquals(UpdatePlan.Effect.LIVE, handle.planUpdate(next).effect());
        assertEquals(UpdateResult.APPLIED, handle.reload(next));
        try (ClientSession<Object> current = handle.session()) {
          assertSame(pinned.client(), current.client());
          assertEquals(8, pinned.settings(Settings.class).limit);
          assertEquals(2, current.settings(Settings.class).limit);
          assertEquals(2, current.revision());
        }
        assertEquals(1, adapter.created.get());
        assertEquals(0, handle.status().retiringGenerations());
      }
      ConfigSnapshot<Settings> forbidden =
          new ConfigSnapshot<>(ID, 3, new Settings(2, "other", old.timeout));
      assertEquals(UpdatePlan.Effect.FORBIDDEN, handle.planUpdate(forbidden).effect());
      assertThrows(IllegalArgumentException.class, () -> handle.reload(forbidden));
      assertEquals(2, handle.status().revision());
      // A new revision with identical fields is an explicit native rebuild, preserving old
      // semantics.
      assertEquals(
          UpdateResult.APPLIED,
          handle.reload(new ConfigSnapshot<>(ID, 4, new Settings(2, "fixed", old.timeout))));
      assertEquals(2, adapter.created.get());
    }
  }

  @Test
  void currentTimeoutCeilingAppliesToTheWholeManagedCallAndNeverLeaksALease() throws Exception {
    Adapter adapter = new Adapter();
    CompletableFuture<String> nativeWork = new CompletableFuture<>();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<Object, Settings> handle =
          runtime.register(
              new ConfigSnapshot<>(ID, 1, new Settings(2, "fixed", Duration.ofMillis(200))),
              adapter,
              ClientPolicy.defaults());
      CompletableFuture<String> result =
          handle.executeAsync(
              Operation.read("get", Duration.ofSeconds(3)),
              (c, context) -> {
                assertTrue(context.remaining().compareTo(Duration.ofMillis(200)) <= 0);
                return nativeWork;
              });
      ExecutionException failure =
          assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
      assertEquals(ClientException.Kind.TIMEOUT, ((ClientException) failure.getCause()).kind());
      nativeWork.complete("late");
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (handle.status().inFlight() != 0 && System.nanoTime() < until) Thread.yield();
      assertEquals(0, handle.status().inFlight());
    } finally {
      nativeWork.complete("cleanup");
    }
  }
}
