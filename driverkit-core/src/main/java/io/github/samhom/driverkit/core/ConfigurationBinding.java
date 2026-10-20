package io.github.samhom.driverkit.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Gated subscription ownership keeps platform callbacks and detachment out of state locks. */
final class ConfigurationBinding<S> {
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private final Object gate = new Object();
  private final LifecycleCoordinator.Owned<AutoCloseable> ownership;

  ConfigurationBinding(
      LifecycleCoordinator lifecycle,
      ConfigurationSource<S> source,
      Consumer<ConfigSnapshot<S>> listener,
      ClientPolicy policy,
      Consumer<Throwable> completion) {
    FactoryAdapter<AutoCloseable, ConfigurationSource<S>> adapter =
        new FactoryAdapter<>(
            "configuration",
            value ->
                value.subscribe(
                    snapshot -> {
                      synchronized (gate) {
                        if (enabled.get()) listener.accept(snapshot);
                      }
                    }),
            (value, remaining) -> {},
            AutoCloseable::close);
    try {
      ownership =
          lifecycle.initialize(adapter, source, policy.warmupTimeout(), policy.shutdownTimeout());
    } catch (RuntimeException | Error failure) {
      if (completion != null) completion.accept(null);
      throw failure;
    }
    if (completion != null) ownership.whenClosed(completion);
    try {
      ownership.awaitReady();
    } catch (RuntimeException failure) {
      close(System.nanoTime() + policy.shutdownTimeout().toNanos());
      throw failure;
    }
  }

  void close(long deadline) {
    synchronized (gate) {
      enabled.set(false);
    }
    ownership.retire(deadline, null);
  }

  boolean isClosed() {
    return ownership.isClosed();
  }
}
