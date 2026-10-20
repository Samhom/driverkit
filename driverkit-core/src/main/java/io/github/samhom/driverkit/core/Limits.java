package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.Objects;

public final class Limits {
  private Limits() {}

  public static Duration timeout(Duration value) {
    Objects.requireNonNull(value, "timeout");
    if (value.compareTo(Duration.ofMillis(1)) < 0 || value.compareTo(Duration.ofMinutes(10)) > 0)
      throw new IllegalArgumentException("Timeout out of bounds");
    return value;
  }

  public static int count(int value, int maximum) {
    if (value < 1 || value > maximum)
      throw new IllegalArgumentException("Resource count out of bounds");
    return value;
  }

  public static String name(String value) {
    if (value == null || !value.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"))
      throw new IllegalArgumentException("Invalid resource name");
    return value;
  }

  public static void bytes(byte[] value, int maximum) {
    if (value == null || value.length > maximum)
      throw new IllegalArgumentException("Payload is missing or oversized");
  }

  public static void messageKey(String value) {
    if (value == null
        || value.isEmpty()
        || value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 512
        || value
            .chars()
            .anyMatch(
                character ->
                    Character.isISOControl(character) || Character.isWhitespace(character)))
      throw new IllegalArgumentException("Invalid message business key");
  }
}
