package io.github.samhom.driverkit.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class CompositeObserver implements Observer {
  private final List<Observer> delegates;

  public CompositeObserver(Observer... delegates) {
    this.delegates = new ArrayList<>(Arrays.asList(delegates));
  }

  @Override
  public Observation start(ResourceId resource, String driver, Operation operation, long revision) {
    List<Observation> observations = new ArrayList<>();
    for (Observer delegate : delegates)
      try {
        observations.add(delegate.start(resource, driver, operation, revision));
      } catch (RuntimeException | LinkageError ignored) {
      }
    return new Observation() {
      @Override
      public void finish(Outcome outcome, long elapsedNanos) {
        for (Observation item : observations)
          try {
            item.finish(outcome, elapsedNanos);
          } catch (RuntimeException | LinkageError ignored) {
          }
      }

      @Override
      public AutoCloseable openScope() {
        List<AutoCloseable> scopes = new ArrayList<>();
        for (Observation item : observations)
          try {
            scopes.add(item.openScope());
          } catch (RuntimeException | LinkageError ignored) {
          }
        return () -> {
          for (int i = scopes.size() - 1; i >= 0; i--)
            try {
              scopes.get(i).close();
            } catch (Exception | LinkageError ignored) {
            }
        };
      }
    };
  }
}
