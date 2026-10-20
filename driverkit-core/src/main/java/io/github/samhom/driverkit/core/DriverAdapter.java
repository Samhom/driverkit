package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

/** A client generation owns every native resource created by its adapter. */
public interface DriverAdapter<C, S> {
  String id();

  void validate(S settings);

  default void validateUpdate(S previous, S next) {}

  C create(S settings) throws Exception;

  void warmup(C client, Duration timeout) throws Exception;

  void close(C client) throws Exception;

  default ClientException.Kind classify(Throwable error, Operation operation) {
    return ClientException.Kind.DRIVER_FAILURE;
  }

  default Set<String> capabilities() {
    return Collections.emptySet();
  }
}
