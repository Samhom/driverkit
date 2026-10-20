package io.github.samhom.driverkit.core;

import java.util.concurrent.Callable;

/** A broker callback must return its retry status when admission is closed. */
public interface DeliveryGuard {
  <T> T invoke(Callable<T> handler, T retryResult);
}
