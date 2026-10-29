package io.github.samhom.driverkit.rocketmq;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Objects;

public record RocketMqSettings(
    Endpoint nameServer,
    CredentialsProvider credentials,
    String group,
    String topic,
    Duration requestTimeout,
    int consumerThreads,
    int maxMessageBytes)
    implements ImmutableSettings {
  public RocketMqSettings {
    Objects.requireNonNull(nameServer);
    Objects.requireNonNull(credentials);
    Limits.name(group);
    Limits.name(topic);
    Limits.timeout(requestTimeout);
    Limits.count(consumerThreads, 256);
    Limits.count(maxMessageBytes, 4 * 1024 * 1024);
  }

  public static RocketMqSettings defaults(
      Endpoint nameServer, CredentialsProvider credentials, String group, String topic) {
    return new RocketMqSettings(
        nameServer, credentials, group, topic, Duration.ofSeconds(3), 8, 1024 * 1024);
  }

  @Override
  public String toString() {
    return "RocketMqSettings{redacted}";
  }
}
