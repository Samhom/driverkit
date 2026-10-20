package io.github.samhom.driverkit.redis;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Objects;

public record RedisSettings(
    Endpoint endpoint,
    CredentialsProvider credentials,
    Duration connectTimeout,
    Duration readTimeout,
    int maxConnections,
    int database,
    int maxValueBytes) {
  public RedisSettings {
    Objects.requireNonNull(endpoint);
    Objects.requireNonNull(credentials);
    Limits.timeout(connectTimeout);
    Limits.timeout(readTimeout);
    Limits.count(maxConnections, 10000);
    Limits.count(maxValueBytes, 16 * 1024 * 1024);
    if (database < 0 || database > 65535)
      throw new IllegalArgumentException("Invalid Redis database");
  }

  public static RedisSettings defaults(Endpoint endpoint, CredentialsProvider credentials) {
    return new RedisSettings(
        endpoint, credentials, Duration.ofSeconds(2), Duration.ofSeconds(3), 32, 0, 1024 * 1024);
  }

  @Override
  public String toString() {
    return "RedisSettings{redacted}";
  }
}
