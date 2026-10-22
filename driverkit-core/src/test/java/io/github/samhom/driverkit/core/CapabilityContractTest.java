package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class CapabilityContractTest {
  @Test
  void unsupportedContractsFailExplicitlyAndDeclarationsAreImmutable() {
    AdapterCapabilities c = AdapterCapabilities.nativeApi(true);
    c.requireManaged(AdapterCapabilities.Feature.ASYNC);
    assertEquals(
        AdapterCapabilities.Support.NATIVE_SESSION_ONLY,
        c.support(AdapterCapabilities.Feature.NATIVE_API));
    for (AdapterCapabilities.Feature f :
        new AdapterCapabilities.Feature[] {
          AdapterCapabilities.Feature.TRANSACTION,
          AdapterCapabilities.Feature.STREAMING_RESULTS,
          AdapterCapabilities.Feature.NATIVE_CANCELLATION
        }) {
      assertEquals(AdapterCapabilities.Support.UNSUPPORTED, c.support(f));
      assertThrows(ClientException.class, () -> c.requireManaged(f));
    }
    assertThrows(UnsupportedOperationException.class, () -> c.declared().clear());
    assertThrows(
        ClientException.class,
        () ->
            AdapterCapabilities.nativeApi(false).requireManaged(AdapterCapabilities.Feature.ASYNC));
  }

  @Test
  void managedAsyncDoesNotBlockWorkerOrCancelNativeWork() throws Exception {
    DriverAdapter<String, String> adapter =
        new DriverAdapter<>() {
          public String id() {
            return "contract";
          }

          public void validate(String s) {}

          public String create(String s) {
            return s;
          }

          public void warmup(String s, Duration d) {}

          public void close(String s) {}

          public AdapterCapabilities capabilities() {
            return AdapterCapabilities.nativeApi(true);
          }
        };
    CompletableFuture<String> nativeWork = new CompletableFuture<>();
    CountDownLatch dispatched = new CountDownLatch(1);
    try (ClientRuntime runtime = new ClientRuntime(1, 1, 2, Observer.NOOP)) {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("contract", "test", "async"), 1, "native"),
              adapter,
              ClientPolicy.defaults());
      handle.capabilities().requireManaged(AdapterCapabilities.Feature.ASYNC);
      CompletableFuture<String> result =
          handle.executeAsync(
              Operation.read("pending", Duration.ofSeconds(5)),
              (c, ctx) -> {
                dispatched.countDown();
                return nativeWork;
              });
      assertTrue(dispatched.await(2, TimeUnit.SECONDS));
      assertEquals(
          "other",
          handle
              .executeAsync(
                  Operation.read("other", Duration.ofSeconds(2)),
                  (c, ctx) -> CompletableFuture.completedFuture("other"))
              .get(3, TimeUnit.SECONDS));
      assertTrue(result.cancel(false));
      assertFalse(nativeWork.isCancelled());
      assertEquals(1, handle.status().inFlight());
      nativeWork.complete("late");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (handle.status().inFlight() != 0 && System.nanoTime() < deadline) Thread.yield();
      assertEquals(0, handle.status().inFlight());
      assertTrue(result.isCancelled());
    } finally {
      nativeWork.complete("cleanup");
    }
  }
}
