package io.github.samhom.driverkit.core;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Controlled consumer rotation with callback admission, drain, exclusive restart and recovery. */
public final class ExclusiveConsumer<C, S> implements AutoCloseable {
  public enum State {
    RUNNING,
    DRAINING,
    FAILED,
    REVOKED,
    CLOSED
  }

  private final ResourceId resource;
  private final ConsumerAdapter<C, S> adapter;
  private final ClientPolicy policy;
  private final Observer observer;
  private final Object lifecycle = new Object(), gate = new Object();
  private final ThreadPoolExecutor updates;
  private final AtomicReference<ConfigSnapshot<S>> pending = new AtomicReference<>();
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private final AtomicBoolean revoked = new AtomicBoolean();
  private volatile State state = State.DRAINING;
  private ConfigSnapshot<S> current;
  private long attempted;
  private C consumer;
  private int active;
  private AutoCloseable subscription;
  private final DeliveryGuard guard =
      new DeliveryGuard() {
        @Override
        public <T> T invoke(Callable<T> handler, T retryResult) {
          synchronized (gate) {
            if (revoked.get() || state != State.RUNNING || active >= policy.maxInFlight())
              return retryResult;
            active++;
          }
          long started = System.nanoTime();
          Observation observation;
          try {
            observation =
                observer.start(
                    resource,
                    adapter.id(),
                    Operation.write("consume", policy.shutdownTimeout()),
                    current.revision());
          } catch (RuntimeException | LinkageError ignored) {
            observation = Observation.NOOP;
          }
          Observation.Outcome outcome = Observation.Outcome.SUCCESS;
          AutoCloseable scope;
          try {
            scope = observation.openScope();
          } catch (RuntimeException | LinkageError ignored) {
            scope = () -> {};
          }
          try {
            T result = handler.call();
            if (result == null || Objects.equals(result, retryResult)) {
              outcome = Observation.Outcome.ERROR;
              return retryResult;
            }
            return result;
          } catch (Exception ignored) {
            outcome = Observation.Outcome.ERROR;
            return retryResult;
          } catch (Error error) {
            outcome = Observation.Outcome.ERROR;
            throw error;
          } finally {
            try {
              scope.close();
            } catch (Exception | LinkageError ignored) {
            }
            try {
              observation.finish(outcome, System.nanoTime() - started);
            } catch (RuntimeException | LinkageError ignored) {
            }
            synchronized (gate) {
              active--;
              gate.notifyAll();
            }
          }
        }
      };

  public ExclusiveConsumer(
      ConfigSnapshot<S> initial,
      ConsumerAdapter<C, S> adapter,
      ClientPolicy policy,
      Observer observer) {
    current = Objects.requireNonNull(initial);
    resource = initial.resource();
    attempted = initial.revision();
    this.adapter = Objects.requireNonNull(adapter);
    this.policy = Objects.requireNonNull(policy);
    this.observer = Objects.requireNonNull(observer);
    updates =
        new ThreadPoolExecutor(
            1,
            1,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            task -> {
              Thread thread = new Thread(task, "driverkit-consumer-update");
              thread.setDaemon(true);
              return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());
    updates.allowCoreThreadTimeOut(true);
    adapter.validate(initial.settings());
    try {
      consumer = Objects.requireNonNull(adapter.start(initial.settings(), guard));
      state = State.RUNNING;
    } catch (Exception e) {
      updates.shutdownNow();
      state = State.FAILED;
      throw new ClientException(
          ClientException.Kind.UPDATE_REJECTED, "Consumer initialization failed", e);
    }
  }

  public State state() {
    return state;
  }

  public int inFlight() {
    synchronized (gate) {
      return active;
    }
  }

  public long revision() {
    synchronized (lifecycle) {
      return current.revision();
    }
  }

  private void drain(long deadline) throws InterruptedException {
    synchronized (gate) {
      while (active > 0 && deadline - System.nanoTime() > 0)
        TimeUnit.NANOSECONDS.timedWait(gate, deadline - System.nanoTime());
    }
  }

  public UpdateResult reload(ConfigSnapshot<S> next) {
    if (!resource.equals(next.resource()))
      throw new IllegalArgumentException("Configuration scope mismatch");
    synchronized (lifecycle) {
      if (revoked.get() || state == State.CLOSED || state == State.REVOKED)
        throw new ClientException(ClientException.Kind.CLOSED, "Consumer admission is closed");
      if (next.revision() <= attempted) return UpdateResult.STALE;
      adapter.validate(next.settings());
      adapter.validateUpdate(current.settings(), next.settings());
      attempted = next.revision();
      if (state != State.RUNNING)
        throw new ClientException(
            ClientException.Kind.UPDATE_REJECTED, "Consumer requires explicit recovery");
      state = State.DRAINING;
      try {
        adapter.pause(consumer);
        drain(System.nanoTime() + policy.shutdownTimeout().toNanos());
        synchronized (gate) {
          if (active > 0) {
            adapter.resume(consumer);
            state = State.RUNNING;
            throw new ClientException(
                ClientException.Kind.TIMEOUT, "Consumer drain deadline exceeded");
          }
        }
        adapter.close(consumer);
        consumer = null;
        try {
          consumer = Objects.requireNonNull(adapter.start(next.settings(), guard));
          if (revoked.get())
            throw new ClientException(
                ClientException.Kind.REVOKED, "Consumer was revoked during startup");
          current = next;
          state = State.RUNNING;
          return UpdateResult.APPLIED;
        } catch (Exception replacementFailure) {
          if (revoked.get()) {
            if (consumer != null)
              try {
                adapter.close(consumer);
              } catch (Exception cleanupFailure) {
                replacementFailure.addSuppressed(cleanupFailure);
              } finally {
                consumer = null;
              }
            state = State.REVOKED;
            throw new ClientException(
                ClientException.Kind.REVOKED,
                "Consumer was revoked during replacement",
                replacementFailure);
          }
          try {
            consumer = Objects.requireNonNull(adapter.start(current.settings(), guard));
            state = State.RUNNING;
          } catch (Exception recoveryFailure) {
            state = State.FAILED;
            replacementFailure.addSuppressed(recoveryFailure);
          }
          throw new ClientException(
              ClientException.Kind.UPDATE_REJECTED,
              "Consumer replacement failed",
              replacementFailure);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        state = State.FAILED;
        throw new ClientException(
            ClientException.Kind.UPDATE_REJECTED, "Consumer rotation interrupted", e);
      } catch (ClientException e) {
        throw e;
      } catch (Exception e) {
        state = State.FAILED;
        throw new ClientException(
            ClientException.Kind.UPDATE_REJECTED, "Consumer rotation failed", e);
      }
    }
  }

  public void bind(ConfigurationSource<S> source) {
    synchronized (lifecycle) {
      if (subscription != null || state != State.RUNNING)
        throw new IllegalStateException("Consumer cannot bind a source");
      subscription =
          source.subscribe(
              snapshot -> {
                if (!resource.equals(snapshot.resource()))
                  throw new IllegalArgumentException("Configuration scope mismatch");
                pending.accumulateAndGet(
                    snapshot,
                    (old, value) -> old == null || value.revision() > old.revision() ? value : old);
                schedule();
              });
    }
  }

  private void schedule() {
    if (state == State.CLOSED
        || state == State.REVOKED
        || pending.get() == null
        || !scheduled.compareAndSet(false, true)) return;
    try {
      updates.execute(
          () -> {
            try {
              ConfigSnapshot<S> next = pending.getAndSet(null);
              if (next != null) reload(next);
            } catch (RuntimeException | LinkageError ignored) {
            } finally {
              scheduled.set(false);
              schedule();
            }
          });
    } catch (RejectedExecutionException ignored) {
      scheduled.set(false);
    }
  }

  public void revoke() {
    revoked.set(true);
    stop(true);
  }

  @Override
  public void close() {
    stop(false);
  }

  private void stop(boolean revoke) {
    synchronized (lifecycle) {
      if (state == State.CLOSED || (state == State.REVOKED && consumer == null)) return;
      state = revoke ? State.REVOKED : State.DRAINING;
      pending.set(null);
      if (subscription != null)
        try {
          subscription.close();
        } catch (Exception ignored) {
        } finally {
          subscription = null;
        }
      updates.shutdownNow();
      try {
        if (consumer != null) {
          adapter.pause(consumer);
          drain(System.nanoTime() + policy.shutdownTimeout().toNanos());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new ClientException(
            ClientException.Kind.DRIVER_FAILURE, "Consumer shutdown failed", e);
      } finally {
        try {
          if (consumer != null) adapter.close(consumer);
        } catch (Exception error) {
          throw new ClientException(
              ClientException.Kind.DRIVER_FAILURE, "Consumer resource cleanup failed", error);
        } finally {
          consumer = null;
          state = revoke || revoked.get() ? State.REVOKED : State.CLOSED;
        }
      }
    }
  }
}
