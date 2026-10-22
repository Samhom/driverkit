package io.github.samhom.driverkit.core;

import java.util.Objects;

/** Capture on the submitting thread; every open must restore the receiving thread on close. */
@FunctionalInterface
public interface ContextPropagation {
  Snapshot capture();

  interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  @FunctionalInterface
  interface Snapshot {
    Scope open();

    default void run(Runnable action) {
      try (Scope ignored = open()) {
        action.run();
      }
    }
  }

  ContextPropagation NONE = () -> () -> () -> {};

  static ContextPropagation combine(ContextPropagation... providers) {
    ContextPropagation[] copy = providers.clone();
    for (ContextPropagation provider : copy) Objects.requireNonNull(provider);
    return () -> {
      Snapshot[] snapshots = new Snapshot[copy.length];
      for (int i = 0; i < copy.length; i++)
        snapshots[i] = Objects.requireNonNull(copy[i].capture());
      return () -> {
        Scope[] scopes = new Scope[snapshots.length];
        int opened = 0;
        try {
          for (; opened < snapshots.length; opened++)
            scopes[opened] = Objects.requireNonNull(snapshots[opened].open());
        } catch (RuntimeException | Error failure) {
          for (int i = opened - 1; i >= 0; i--) {
            try {
              scopes[i].close();
            } catch (RuntimeException | Error cleanup) {
              failure.addSuppressed(cleanup);
            }
          }
          throw failure;
        }
        return () -> {
          Throwable failure = null;
          for (int i = scopes.length - 1; i >= 0; i--) {
            try {
              scopes[i].close();
            } catch (RuntimeException | Error error) {
              if (failure == null) failure = error;
              else failure.addSuppressed(error);
            }
          }
          if (failure instanceof Error) throw (Error) failure;
          if (failure != null) throw (RuntimeException) failure;
        };
      };
    };
  }
}
