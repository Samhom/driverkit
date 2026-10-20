package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/** Static operation names keep telemetry cardinality bounded. Payloads are deliberately absent. */
public final class Operation {
  private static final Pattern NAME = Pattern.compile("[a-zA-Z][a-zA-Z0-9_.-]{0,63}");
  private final String name;
  private final Duration timeout;
  private final boolean replaySafe;

  private Operation(String name, Duration timeout, boolean replaySafe) {
    if (name == null || !NAME.matcher(name).matches())
      throw new IllegalArgumentException("Invalid operation name");
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofMinutes(10)) > 0)
      throw new IllegalArgumentException("Timeout out of bounds");
    this.name = name;
    this.timeout = timeout;
    this.replaySafe = replaySafe;
  }

  public static Operation read(String name, Duration timeout) {
    return new Operation(name, timeout, true);
  }

  public static Operation write(String name, Duration timeout) {
    return new Operation(name, timeout, false);
  }

  public String name() {
    return name;
  }

  public Duration timeout() {
    return timeout;
  }

  public boolean replaySafe() {
    return replaySafe;
  }
}
