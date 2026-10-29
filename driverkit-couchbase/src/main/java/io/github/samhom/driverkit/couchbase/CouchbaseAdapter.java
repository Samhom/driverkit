package io.github.samhom.driverkit.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import io.github.samhom.driverkit.core.*;
import java.time.Duration;

public final class CouchbaseAdapter implements DriverAdapter<Cluster, CouchbaseSettings> {
  @Override
  public String id() {
    return "couchbase3";
  }

  @Override
  public void validate(CouchbaseSettings settings) {
    java.util.Objects.requireNonNull(settings);
  }

  @Override
  public void validateUpdate(CouchbaseSettings previous, CouchbaseSettings next) {
    if (!previous.bucket().equals(next.bucket())
        || !previous.scope().equals(next.scope())
        || !previous.collection().equals(next.collection()))
      throw new IllegalArgumentException("Collection migration requires a new resource binding");
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    error = FailureClassifier.unwrap(error);
    if (error instanceof com.couchbase.client.core.error.DurabilityAmbiguousException
        || error instanceof com.couchbase.client.core.error.RequestCanceledException)
      return FailureClassifier.uncertain(operation, ClientException.Kind.UNAVAILABLE);
    if (error instanceof com.couchbase.client.core.error.RateLimitedException
        || error instanceof com.couchbase.client.core.error.QuotaLimitedException)
      return ClientException.Kind.RATE_LIMITED;
    if (error instanceof com.couchbase.client.core.error.TemporaryFailureException)
      return ClientException.Kind.SERVER_REJECTED;
    if (error instanceof com.couchbase.client.core.error.ServiceNotAvailableException)
      return FailureClassifier.uncertain(operation, ClientException.Kind.UNAVAILABLE);
    if (error instanceof com.couchbase.client.core.error.DocumentExistsException)
      return ClientException.Kind.CONFLICT;
    if (error instanceof com.couchbase.client.core.error.AmbiguousTimeoutException)
      return operation.replaySafe()
          ? ClientException.Kind.TIMEOUT
          : ClientException.Kind.WRITE_OUTCOME_UNKNOWN;
    if (error instanceof com.couchbase.client.core.error.UnambiguousTimeoutException)
      return ClientException.Kind.TIMEOUT;
    if (error instanceof com.couchbase.client.core.error.DocumentNotFoundException)
      return ClientException.Kind.NOT_FOUND;
    if (error instanceof com.couchbase.client.core.error.CasMismatchException)
      return ClientException.Kind.CONFLICT;
    if (error instanceof com.couchbase.client.core.error.AuthenticationFailureException)
      return ClientException.Kind.AUTHENTICATION;
    return FailureClassifier.transport(error, operation);
  }

  @Override
  public Cluster create(CouchbaseSettings settings) {
    try (Credentials credentials = settings.credentials().resolve()) {
      return Cluster.connect(
          settings.endpoint().uri("couchbases", "couchbase").toString(),
          ClusterOptions.clusterOptions(credentials.username(), credentials.passwordText())
              .environment(
                  builder ->
                      builder
                          .ioEnvironment(io -> io.eventLoopThreadCount(2))
                          .ioConfig(io -> io.maxHttpConnections(32))
                          .timeoutConfig(
                              timeouts ->
                                  timeouts
                                      .connectTimeout(settings.connectTimeout())
                                      .kvTimeout(settings.operationTimeout())
                                      .queryTimeout(settings.operationTimeout())
                                      .managementTimeout(settings.operationTimeout()))));
    }
  }

  @Override
  public void warmup(Cluster client, Duration timeout) {
    client.waitUntilReady(timeout);
  }

  @Override
  public void warmup(Cluster client, CouchbaseSettings settings, Duration remaining) {
    long deadline = System.nanoTime() + remaining.toNanos();
    client.waitUntilReady(remaining);
    long nanos = deadline - System.nanoTime();
    if (nanos <= 0)
      throw new ClientException(ClientException.Kind.TIMEOUT, "Bucket readiness deadline exceeded");
    client
        .bucket(settings.bucket())
        .waitUntilReady(
            Duration.ofNanos(nanos),
            com.couchbase.client.java.diagnostics.WaitUntilReadyOptions.waitUntilReadyOptions()
                .serviceTypes(com.couchbase.client.core.service.ServiceType.KV));
  }

  @Override
  public void close(Cluster client) {
    client.disconnect();
  }

  @Override
  public AdapterCapabilities capabilities() {
    return AdapterCapabilities.nativeApi(true);
  }

  @Override
  public UpdatePlan updatePlan(CouchbaseSettings previous, CouchbaseSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }

  @Override
  public java.time.Duration operationTimeout(CouchbaseSettings settings, Operation operation) {
    return settings.operationTimeout();
  }
}
