package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class CompletionIsolationTest {
  private ManagedClient<String, String> register(ClientRuntime runtime) {
    return runtime.register(
        new ConfigSnapshot<>(new ResourceId("example", "test", "cache"), 1, "native"),
        new FactoryAdapter<>("test", value -> value, (value, timeout) -> {}, value -> {}),
        ClientPolicy.defaults());
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("Latch timed out");
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new AssertionError(error);
    }
  }

  @Test
  void slowUserCallbackDoesNotBlockNativeCompletionOrHoldPhysicalLease() throws Exception {
    CountDownLatch callbackEntered = new CountDownLatch(1), release = new CountDownLatch(1);
    ExecutorService nativeIo =
        Executors.newSingleThreadExecutor(task -> new Thread(task, "fixture-native-io"));
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> handle = register(runtime);
      CompletableFuture<String> nativeResult = new CompletableFuture<>();
      CompletableFuture<String> result =
          handle.executeAsync(
              Operation.read("get", Duration.ofSeconds(3)), (client, context) -> nativeResult);
      AtomicReference<String> thread = new AtomicReference<>();
      CompletableFuture<String> callback =
          result.thenApply(
              value -> {
                thread.set(Thread.currentThread().getName());
                callbackEntered.countDown();
                await(release);
                return value;
              });
      try {
        assertTrue(nativeIo.submit(() -> nativeResult.complete("value")).get(1, TimeUnit.SECONDS));
        assertTrue(callbackEntered.await(1, TimeUnit.SECONDS));
        assertTrue(thread.get().startsWith("driverkit-completion-"));
        assertEquals(0, handle.status().inFlight());
        assertFalse(callback.isDone());
      } finally {
        release.countDown();
      }
      assertEquals("value", callback.get(1, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      nativeIo.shutdownNow();
    }
  }

  @Test
  void slowTimeoutCallbackCannotBlockTimerOrOtherDeadlines() throws Exception {
    CountDownLatch callbackEntered = new CountDownLatch(1), release = new CountDownLatch(1);
    CompletableFuture<String> nativeResult = new CompletableFuture<>();
    try (ClientRuntime runtime = new ClientRuntime(1, 2, 4, Observer.NOOP)) {
      ManagedClient<String, String> handle = register(runtime);
      CompletableFuture<String> timed =
          handle.executeAsync(
              Operation.read("first", Duration.ofMillis(200)), (client, context) -> nativeResult);
      CompletableFuture<String> callback =
          timed.whenComplete(
              (value, error) -> {
                callbackEntered.countDown();
                await(release);
              });
      try {
        assertTrue(callbackEntered.await(1, TimeUnit.SECONDS));
        CountDownLatch timerProbe = new CountDownLatch(1);
        runtime.timers.execute(timerProbe::countDown);
        assertTrue(timerProbe.await(1, TimeUnit.SECONDS));
        CompletableFuture<String> second =
            handle.executeAsync(
                Operation.read("second", Duration.ofMillis(100)),
                (client, context) -> nativeResult);
        ExecutionException error =
            assertThrows(ExecutionException.class, () -> second.get(1, TimeUnit.SECONDS));
        assertEquals(
            ClientException.Kind.TIMEOUT,
            assertInstanceOf(ClientException.class, error.getCause()).kind());
        assertFalse(callback.isDone());
      } finally {
        release.countDown();
        nativeResult.complete("late");
      }
    }
  }

  @Test
  void callbackSaturationRejectsBeforeNativeDispatchWithoutDroppingAcceptedResults()
      throws Exception {
    CountDownLatch callbackEntered = new CountDownLatch(1), release = new CountDownLatch(1);
    try (ClientRuntime runtime =
        new ClientRuntime(1, 1, 4, Observer.NOOP, AdmissionPolicy.ALLOW, 1, 2)) {
      ManagedClient<String, String> handle = register(runtime);
      CompletableFuture<String> nativeResult = new CompletableFuture<>();
      CompletableFuture<String> first =
          handle.executeAsync(
              Operation.read("first", Duration.ofSeconds(3)), (client, context) -> nativeResult);
      CompletableFuture<String> callback =
          first.thenApply(
              value -> {
                callbackEntered.countDown();
                await(release);
                return value;
              });
      nativeResult.complete("first");
      try {
        assertTrue(callbackEntered.await(1, TimeUnit.SECONDS));
        CountDownLatch secondDispatched = new CountDownLatch(1);
        CompletableFuture<String> second =
            handle.executeAsync(
                Operation.read("second", Duration.ofSeconds(3)),
                (client, context) -> {
                  secondDispatched.countDown();
                  return CompletableFuture.completedFuture("second");
                });
        assertTrue(secondDispatched.await(1, TimeUnit.SECONDS));
        AtomicBoolean invoked = new AtomicBoolean();
        CompletableFuture<String> rejected =
            handle.executeAsync(
                Operation.read("third", Duration.ofSeconds(1)),
                (client, context) -> {
                  invoked.set(true);
                  return CompletableFuture.completedFuture("unexpected");
                });
        ExecutionException error =
            assertThrows(ExecutionException.class, () -> rejected.get(1, TimeUnit.SECONDS));
        assertEquals(
            ClientException.Kind.OVERLOADED,
            assertInstanceOf(ClientException.class, error.getCause()).kind());
        assertFalse(invoked.get());
        assertFalse(second.isDone());
        release.countDown();
        assertEquals("first", callback.get(1, TimeUnit.SECONDS));
        assertEquals("second", second.get(1, TimeUnit.SECONDS));
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void nestedBlockingSdkCallFailsFastInsteadOfDeadlockingCompletionPool() throws Exception {
    try (ClientRuntime runtime =
        new ClientRuntime(1, 1, 2, Observer.NOOP, AdmissionPolicy.ALLOW, 1, 2)) {
      ManagedClient<String, String> handle = register(runtime);
      CompletableFuture<String> nativeResult = new CompletableFuture<>();
      CompletableFuture<String> result =
          handle.executeAsync(
              Operation.read("first", Duration.ofSeconds(2)), (client, context) -> nativeResult);
      CompletableFuture<String> nested =
          result.thenApply(
              value ->
                  handle.execute(
                      Operation.read("nested", Duration.ofSeconds(1)),
                      (client, context) -> "unexpected"));
      nativeResult.complete("first");
      ExecutionException error =
          assertThrows(ExecutionException.class, () -> nested.get(1, TimeUnit.SECONDS));
      assertEquals(
          ClientException.Kind.INVALID_REQUEST,
          assertInstanceOf(ClientException.class, error.getCause()).kind());
    }
  }
}
