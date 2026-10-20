package io.github.samhom.driverkit.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/** An authenticated platform integration can publish immutable snapshots through this bridge. */
public final class PushConfigurationSource<S> implements ConfigurationSource<S> {
  private final Map<Long, Consumer<ConfigSnapshot<S>>> listeners = new LinkedHashMap<>();
  private ConfigSnapshot<S> current;
  private long sequence;

  public PushConfigurationSource(ConfigSnapshot<S> initial) {
    current = Objects.requireNonNull(initial, "initial");
  }

  @Override
  public synchronized AutoCloseable subscribe(Consumer<ConfigSnapshot<S>> listener) {
    Objects.requireNonNull(listener, "listener");
    long id = ++sequence;
    listeners.put(id, listener);
    try {
      listener.accept(current);
    } catch (RuntimeException e) {
      listeners.remove(id);
      throw e;
    }
    return () -> {
      synchronized (PushConfigurationSource.this) {
        listeners.remove(id);
      }
    };
  }

  public synchronized boolean publish(ConfigSnapshot<S> snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    if (!current.resource().equals(snapshot.resource()))
      throw new IllegalArgumentException("Resource scope mismatch");
    if (snapshot.revision() <= current.revision()) return false;
    current = snapshot;
    for (Consumer<ConfigSnapshot<S>> listener : new ArrayList<>(listeners.values())) {
      try {
        listener.accept(snapshot);
      } catch (RuntimeException ignored) {
        /* A failing subscriber cannot suppress others. */
      }
    }
    return true;
  }
}
