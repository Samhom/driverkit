package io.github.samhom.driverkit.elasticsearch;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Objects;

public record ElasticsearchSettings(
    Endpoint endpoint,
    CredentialsProvider credentials,
    Duration connectTimeout,
    Duration readTimeout,
    int maxConnections,
    int maxBatchItems,
    int maxDocumentBytes)
    implements ImmutableSettings {
  public ElasticsearchSettings {
    Objects.requireNonNull(endpoint);
    Objects.requireNonNull(credentials);
    Limits.timeout(connectTimeout);
    Limits.timeout(readTimeout);
    Limits.count(maxConnections, 10000);
    Limits.count(maxBatchItems, 10000);
    Limits.count(maxDocumentBytes, 16 * 1024 * 1024);
  }

  public static ElasticsearchSettings defaults(Endpoint endpoint, CredentialsProvider credentials) {
    return new ElasticsearchSettings(
        endpoint, credentials, Duration.ofSeconds(2), Duration.ofSeconds(5), 32, 1000, 1024 * 1024);
  }

  @Override
  public String toString() {
    return "ElasticsearchSettings{redacted}";
  }
}
