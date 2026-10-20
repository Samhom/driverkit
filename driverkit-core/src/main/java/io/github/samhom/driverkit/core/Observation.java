package io.github.samhom.driverkit.core;

public interface Observation {
  void finish(Outcome outcome, long elapsedNanos);

  default AutoCloseable openScope() {
    return () -> {};
  }

  enum Outcome {
    SUCCESS,
    ERROR,
    CANCELLED,
    TIMEOUT,
    REJECTED
  }

  Observation NOOP = (outcome, elapsedNanos) -> {};
}
