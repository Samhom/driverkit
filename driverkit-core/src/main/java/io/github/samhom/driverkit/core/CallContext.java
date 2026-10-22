package io.github.samhom.driverkit.core;

import java.time.Duration;

/** A single deadline is shared with the adapter and all native attempts. */
public final class CallContext {
  private final long deadline;
  private Object settings;

  CallContext(Duration timeout) {
    deadline = System.nanoTime() + timeout.toNanos();
  }

  void bindSettings(Object settings) {
    this.settings = settings;
  }

  public <S> S settings(Class<S> type) {
    return type.cast(settings);
  }

  public Duration remaining() {
    return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
  }

  /** Positive milliseconds for native APIs; zero must never become an infinite native timeout. */
  public int remainingMillis(Duration ceiling) {
    checkDeadline();
    long nanos = Math.min(remaining().toNanos(), Limits.timeout(ceiling).toNanos());
    return (int) Math.max(1, Math.min(Integer.MAX_VALUE, (nanos + 999999) / 1000000));
  }

  public void checkDeadline() {
    if (remaining().isZero())
      throw new ClientException(ClientException.Kind.TIMEOUT, "Operation deadline exceeded");
  }
}
