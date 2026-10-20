package io.github.samhom.driverkit.core;

/** Exclusive consumers stop the old group member before starting its replacement. */
public interface ConsumerAdapter<C, S> {
  String id();

  void validate(S settings);

  default void validateUpdate(S previous, S next) {}

  C start(S settings, DeliveryGuard guard) throws Exception;

  void pause(C consumer) throws Exception;

  void resume(C consumer) throws Exception;

  void close(C consumer) throws Exception;
}
