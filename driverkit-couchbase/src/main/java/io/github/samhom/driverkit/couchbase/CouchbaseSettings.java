package io.github.samhom.driverkit.couchbase;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Objects;

public record CouchbaseSettings(
    Endpoint endpoint,
    CredentialsProvider credentials,
    String bucket,
    String scope,
    String collection,
    Duration connectTimeout,
    Duration operationTimeout,
    int maxDocumentBytes) {
  public CouchbaseSettings {
    Objects.requireNonNull(endpoint);
    Objects.requireNonNull(credentials);
    Limits.name(bucket);
    Limits.name(scope);
    Limits.name(collection);
    Limits.timeout(connectTimeout);
    Limits.timeout(operationTimeout);
    Limits.count(maxDocumentBytes, 16 * 1024 * 1024);
  }

  public static CouchbaseSettings defaults(
      Endpoint endpoint, CredentialsProvider credentials, String bucket) {
    return new CouchbaseSettings(
        endpoint,
        credentials,
        bucket,
        "_default",
        "_default",
        Duration.ofSeconds(5),
        Duration.ofSeconds(3),
        1024 * 1024);
  }

  @Override
  public String toString() {
    return "CouchbaseSettings{redacted}";
  }
}
