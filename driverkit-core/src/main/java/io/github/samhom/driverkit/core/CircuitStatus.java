package io.github.samhom.driverkit.core;

public final class CircuitStatus {
  public enum State {
    DISABLED,
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final State state;
  private final int failures;
  private final long retryAfterNanos;

  CircuitStatus(State state, int failures, long retryAfterNanos) {
    this.state = state;
    this.failures = failures;
    this.retryAfterNanos = retryAfterNanos;
  }

  public State state() {
    return state;
  }

  public int consecutiveFailures() {
    return failures;
  }

  public long retryAfterNanos() {
    return retryAfterNanos;
  }
}
