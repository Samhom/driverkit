package io.github.samhom.driverkit.couchbase.legacy;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.couchbase.CouchbaseSettings;
import java.time.Duration;

/** Migration-only SDK 2 adapter. TLS and collections require the SDK 3 managed adapter. */
public final class Couchbase2Adapter implements DriverAdapter<Couchbase2Client, CouchbaseSettings> {
  @Override
  public String id() {
    return "couchbase2";
  }

  @Override
  public void validate(CouchbaseSettings settings) {
    java.util.Objects.requireNonNull(settings);
    if (settings.endpoint().tls())
      throw new IllegalArgumentException(
          "SDK 2 TLS is not verified; use the SDK 3 adapter or an audited factory");
    if (!settings.scope().equals("_default") || !settings.collection().equals("_default"))
      throw new IllegalArgumentException("SDK 2 has no scopes or collections");
    if (settings.endpoint().port() != 11210)
      throw new IllegalArgumentException("SDK 2 adapter requires the standard KV port");
  }

  @Override
  public void validateUpdate(CouchbaseSettings previous, CouchbaseSettings next) {
    if (!previous.bucket().equals(next.bucket()))
      throw new IllegalArgumentException("Bucket migration requires a new binding");
  }

  @Override
  public Couchbase2Client create(CouchbaseSettings settings) {
    validate(settings);
    return new Couchbase2Client(settings);
  }

  @Override
  public void warmup(Couchbase2Client client, Duration remaining) {
    client.initialize(remaining);
  }

  @Override
  public void close(Couchbase2Client client) {
    client.close(3000);
  }

  @Override
  public void close(Couchbase2Client client, Duration remaining) {
    client.close(Math.max(1, remaining.toMillis()));
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    error = FailureClassifier.unwrap(error);
    if (error instanceof com.couchbase.client.java.error.AuthenticationException
        || error instanceof com.couchbase.client.java.error.AuthenticatorException)
      return ClientException.Kind.AUTHENTICATION;
    if (error instanceof com.couchbase.client.java.error.DocumentDoesNotExistException)
      return ClientException.Kind.NOT_FOUND;
    if (error instanceof com.couchbase.client.java.error.DocumentAlreadyExistsException)
      return ClientException.Kind.CONFLICT;
    if (error instanceof com.couchbase.client.java.error.TemporaryFailureException)
      return ClientException.Kind.SERVER_REJECTED;
    return FailureClassifier.transport(error, operation);
  }

  @Override
  public UpdatePlan updatePlan(CouchbaseSettings previous, CouchbaseSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }

  @Override
  public java.time.Duration operationTimeout(CouchbaseSettings settings, Operation operation) {
    return settings.operationTimeout();
  }
  @Override public CredentialsProvider credentials(CouchbaseSettings settings) { return settings.credentials(); }

}
