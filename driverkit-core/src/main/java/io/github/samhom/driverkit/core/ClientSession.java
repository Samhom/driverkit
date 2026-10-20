package io.github.samhom.driverkit.core;

import java.util.concurrent.atomic.AtomicBoolean;

/** Pins a generation for transactions, pipelines or streams. Do not let native objects escape. */
public final class ClientSession<C> implements AutoCloseable {
  private final C client;
  private final long revision;
  private final Runnable release;
  private final java.util.function.BooleanSupplier usable;
  private final Object settings;
  private final AtomicBoolean closed = new AtomicBoolean();

  ClientSession(
      C client,
      long revision,
      Object settings,
      Runnable release,
      java.util.function.BooleanSupplier usable) {
    this.client = client;
    this.revision = revision;
    this.settings = settings;
    this.release = release;
    this.usable = usable;
  }

  public <S> S settings(Class<S> type) {
    return type.cast(settings);
  }

  public C client() {
    if (closed.get() || !usable.getAsBoolean())
      throw new ClientException(ClientException.Kind.CLOSED, "Session is closed");
    return client;
  }

  public long revision() {
    return revision;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) release.run();
  }
}
