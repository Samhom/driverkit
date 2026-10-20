package io.github.samhom.driverkit.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Gated subscription ownership keeps platform callbacks and detachment out of state locks. */
final class ConfigurationBinding<S> {
  private final AtomicBoolean enabled = new AtomicBoolean(true), closing = new AtomicBoolean();
  private final Object gate = new Object();
  private final LifecycleCoordinator.Owned<AutoCloseable> ownership;
  private final Consumer<Throwable> completion;

  ConfigurationBinding(
      LifecycleCoordinator lifecycle,
      ConfigurationSource<S> source,
      Consumer<ConfigSnapshot<S>> listener,
      ClientPolicy policy,
      Consumer<Throwable> completion) {
    this.completion = completion;
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
    ownership =
        lifecycle.initialize(adapter, source, policy.warmupTimeout(), policy.shutdownTimeout());
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
    ownership.retire(deadline, closing.compareAndSet(false, true) ? completion : null);
  }

  boolean isClosed() {
    return ownership.isClosed();
  }
}
