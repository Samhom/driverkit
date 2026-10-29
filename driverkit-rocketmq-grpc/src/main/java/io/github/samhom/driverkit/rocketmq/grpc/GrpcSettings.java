package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Objects;

public record GrpcSettings(
    Endpoint proxy,
    CredentialsProvider credentials,
    String topic,
    Duration requestTimeout,
    int maxMessageBytes)
    implements ImmutableSettings {
  public GrpcSettings {
    Objects.requireNonNull(proxy);
    Objects.requireNonNull(credentials);
    Limits.name(topic);
    Limits.timeout(requestTimeout);
    Limits.count(maxMessageBytes, 4 * 1024 * 1024);
  }

  public static GrpcSettings defaults(
      Endpoint proxy, CredentialsProvider credentials, String topic) {
    return new GrpcSettings(proxy, credentials, topic, Duration.ofSeconds(3), 1024 * 1024);
  }

  @Override
  public String toString() {
    return "GrpcSettings{redacted}";
  }
}
