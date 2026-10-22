package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ContextPropagationTest {
  @Test
  void successErrorTimeoutAndCancellationRestoreEveryReceivingThread() throws Exception {
    for (String mode : List.of("success", "error", "timeout", "cancel")) {
      try (ClientRuntime runtime =
          new ClientRuntime(1, 1, 4, Observer.NOOP, AdmissionPolicy.ALLOW, 1, 8)) {
        ManagedClient<String, String> handle =
            runtime.register(
                new ConfigSnapshot<>(new ResourceId("context", "test", "resource"), 1, "native"),
                new FactoryAdapter<>("fixture", s -> s, (c, t) -> {}, c -> {}),
                ClientPolicy.defaults());
        CompletableFuture<String> nativeResult = new CompletableFuture<>();
        CompletableFuture<String> result;
        CountDownLatch entered = new CountDownLatch(1), delivered = new CountDownLatch(1);
        AtomicReference<Map<String, String>> seen = new AtomicReference<>();
        try (ContextPropagation.Scope ignored = CallContexts.open(Map.of("request", mode))) {
          result =
              handle.executeAsync(
                  Operation.read(
                      "get",
                      mode.equals("timeout") ? Duration.ofMillis(250) : Duration.ofSeconds(5)),
                  (c, ctx) -> {
                    assertEquals(mode, CallContexts.current().get("request"));
                    entered.countDown();
                    return nativeResult;
                  });
          result.whenComplete(
              (v, e) -> {
                seen.set(CallContexts.current());
                delivered.countDown();
              });
        }
        assertTrue(CallContexts.current().isEmpty());
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        try (ContextPropagation.Scope ignored = CallContexts.open(Map.of("request", "foreign"))) {
          switch (mode) {
            case "success" -> nativeResult.complete("ok");
            case "error" ->
                nativeResult.completeExceptionally(new IllegalStateException("fixture"));
            case "cancel" -> result.cancel(false);
          }
          assertEquals("foreign", CallContexts.current().get("request"));
        }
        assertTrue(delivered.await(3, TimeUnit.SECONDS));
        assertEquals(Map.of("request", mode), seen.get());
        nativeResult.complete("late");
        assertTrue(
            handle
                .executeAsync(
                    Operation.read("clean", Duration.ofSeconds(2)),
                    (c, ctx) -> CompletableFuture.completedFuture(CallContexts.current().isEmpty()))
                .get(3, TimeUnit.SECONDS));
        CountDownLatch inspected = new CountDownLatch(1);
        AtomicReference<Map<String, String>> completionState = new AtomicReference<>();
        runtime
            .reserveCompletion()
            .deliver(
                () -> {
                  completionState.set(CallContexts.current());
                  inspected.countDown();
                });
        assertTrue(inspected.await(2, TimeUnit.SECONDS));
        assertTrue(completionState.get().isEmpty());
      }
    }
  }

  @Test
  void compositionRollsBackPartialOpenAndRejectsMutableOrUnboundedMetadata() {
    ThreadLocal<String> value = new ThreadLocal<>();
    ContextPropagation first =
        () ->
            () -> {
              value.set("captured");
              return value::remove;
            };
    ContextPropagation failing =
        () ->
            () -> {
              throw new IllegalStateException("fixture");
            };
    assertThrows(
        IllegalStateException.class,
        () -> ContextPropagation.combine(first, failing).capture().open());
    assertNull(value.get());
    Map<String, String> input = new HashMap<>(Map.of("request", "one"));
    try (ContextPropagation.Scope ignored = CallContexts.open(input)) {
      input.put("request", "mutated");
      assertEquals("one", CallContexts.current().get("request"));
      assertThrows(UnsupportedOperationException.class, () -> CallContexts.current().clear());
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> CallContexts.open(Map.of("request", "x".repeat(513))));
    assertTrue(CallContexts.current().isEmpty());
  }

  @Test
  void brokenProviderCannotStrandAnInvocation() throws Exception {
    ContextPropagation broken =
        () ->
            () -> {
              throw new IllegalStateException("fixture");
            };
    try (ClientRuntime runtime =
        new ClientRuntime(1, 1, 2, Observer.NOOP, AdmissionPolicy.ALLOW, 1, 4, broken)) {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("context", "test", "broken"), 1, "native"),
              new FactoryAdapter<>("fixture", s -> s, (c, t) -> {}, c -> {}),
              ClientPolicy.defaults());
      assertThrows(
          ExecutionException.class,
          () ->
              handle
                  .executeAsync(
                      Operation.read("get", Duration.ofSeconds(2)),
                      (c, ctx) -> CompletableFuture.completedFuture(c))
                  .get(3, TimeUnit.SECONDS));
      assertEquals(0, handle.status().inFlight());
    }
  }
}
