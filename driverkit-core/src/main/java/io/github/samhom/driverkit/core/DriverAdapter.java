package io.github.samhom.driverkit.core;

import java.time.Duration;

/** A client generation owns every native resource created by its adapter. */
public interface DriverAdapter<C, S> {
  String id();

  void validate(S settings);

  default void validateUpdate(S previous, S next) {}

  C create(S settings) throws Exception;

  /** Remaining initialization budget, including time already spent waiting for lifecycle work. */
  default C create(S settings, Duration remaining) throws Exception {
    return create(settings);
  }

  void warmup(C client, Duration timeout) throws Exception;

  /** Settings-aware readiness for the actual bucket/database owned by this generation. */
  default void warmup(C client, S settings, Duration remaining) throws Exception {
    warmup(client, remaining);
  }

  void close(C client) throws Exception;

  /** Native implementations should apply this budget where their driver supports it. */
  default void close(C client, Duration remaining) throws Exception {
    close(client);
  }

  default ClientException.Kind classify(Throwable error, Operation operation) {
    return FailureClassifier.transport(error, operation);
  }

  default AdapterCapabilities capabilities() {
    return AdapterCapabilities.none();
  }
}
