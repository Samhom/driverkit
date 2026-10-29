package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.Objects;

/** Immutable resource ceilings. The core never automatically replays native operations. */
public final class ClientPolicy {
  private final int maxInFlight;
  private final ExecutionPolicy execution;
  private final Duration warmupTimeout;
  private final Duration shutdownTimeout;

  public ClientPolicy(int maxInFlight, Duration warmupTimeout, Duration shutdownTimeout) {
    this(maxInFlight, warmupTimeout, shutdownTimeout, ExecutionPolicy.shared());
  }

  public ClientPolicy(
      int maxInFlight,
      Duration warmupTimeout,
      Duration shutdownTimeout,
      ExecutionPolicy execution) {
    this.execution = Objects.requireNonNull(execution);
    if (maxInFlight < 1 || maxInFlight > 1000000)
      throw new IllegalArgumentException("Invalid in-flight limit");
    this.maxInFlight = maxInFlight;
    this.warmupTimeout = bounded(warmupTimeout);
    this.shutdownTimeout = bounded(shutdownTimeout);
  }

  private static Duration bounded(Duration duration) {
    Objects.requireNonNull(duration, "duration");
    if (duration.isNegative()
        || duration.isZero()
        || duration.compareTo(Duration.ofMinutes(10)) > 0)
      throw new IllegalArgumentException("Duration out of bounds");
    return duration;
  }

  public static ClientPolicy defaults() {
    return new ClientPolicy(1024, Duration.ofSeconds(10), Duration.ofSeconds(30));
  }

  public ExecutionPolicy execution() {
    return execution;
  }

  public int maxInFlight() {
    return maxInFlight;
  }

  public Duration warmupTimeout() {
    return warmupTimeout;
  }

  public Duration shutdownTimeout() {
    return shutdownTimeout;
  }
}
