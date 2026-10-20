package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.Objects;

/** Bridge existing driver factories without linking their vendor types into the core. */
public final class FactoryAdapter<C, S> implements DriverAdapter<C, S> {
  @FunctionalInterface
  public interface Factory<C, S> {
    C create(S settings) throws Exception;
  }

  @FunctionalInterface
  public interface Readiness<C> {
    void check(C client, Duration timeout) throws Exception;
  }

  @FunctionalInterface
  public interface Closer<C> {
    void close(C client) throws Exception;
  }

  private final String id;
  private final Factory<C, S> factory;
  private final Readiness<C> readiness;
  private final Closer<C> closer;

  public FactoryAdapter(
      String id, Factory<C, S> factory, Readiness<C> readiness, Closer<C> closer) {
    this.id = Limits.name(id);
    this.factory = Objects.requireNonNull(factory);
    this.readiness = Objects.requireNonNull(readiness);
    this.closer = Objects.requireNonNull(closer);
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public void validate(S settings) {
    Objects.requireNonNull(settings);
  }

  @Override
  public C create(S settings) throws Exception {
    return factory.create(settings);
  }

  @Override
  public void warmup(C client, Duration timeout) throws Exception {
    readiness.check(client, timeout);
  }

  @Override
  public void close(C client) throws Exception {
    closer.close(client);
  }
}
