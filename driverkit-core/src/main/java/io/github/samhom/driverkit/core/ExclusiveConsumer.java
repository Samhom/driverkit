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
  private volatile ConfigSnapshot<S> current;
  private final ConfigurationTracker configuration = new ConfigurationTracker();
  private volatile ConfigurationTracker.Attempt configurationAttempt;
  private long attempted;
  private C consumer;
  private int active;
  private ConfigurationBinding<S> subscription;
  private volatile CredentialWatch credentials;
  private ScheduledThreadPoolExecutor credentialTimers;
  private final LifecycleCoordinator lifecycleWork = new LifecycleCoordinator(8);
  private final AtomicBoolean stopping = new AtomicBoolean();
  private LifecycleCoordinator.Owned<C> ownership;
  private boolean cleanupRequested;
  private final DeliveryGuard guard =
      new DeliveryGuard() {
        @Override
        public <T> T invoke(Callable<T> handler, T retryResult) {
          CredentialWatch watch = credentials;
          if (watch != null)
            try {
              watch.check();
            } catch (ClientException denied) {
              return retryResult;
            }
          synchronized (gate) {
            if (stopping.get()
                || revoked.get()
                || state != State.RUNNING
                || active >= policy.maxInFlight()) return retryResult;
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
    configurationAttempt = configuration.begin(initial.revision());
    try {
      ownership = initialize(initial.settings());
      consumer = ownership.awaitReady();
      state = State.RUNNING;
      configuration.finish(configurationAttempt, current.revision(), null);
    } catch (Exception e) {
      configuration.finish(configurationAttempt, current.revision(), e);
      updates.shutdownNow();
      lifecycleWork.stopInitializing();
      state = State.FAILED;
      throw new ClientException(
          ClientException.Kind.UPDATE_REJECTED, "Consumer initialization failed", e);
    }
  }

  private LifecycleCoordinator.Owned<C> initialize(S settings) {
    DriverAdapter<C, S> bridge =
        new DriverAdapter<C, S>() {
          public String id() {
            return adapter.id();
          }

          public void validate(S value) {
            adapter.validate(value);
          }

          public C create(S value) throws Exception {
            return adapter.start(value, guard);
          }

          public C create(S value, java.time.Duration remaining) throws Exception {
            return adapter.start(value, guard, remaining);
          }

          public void warmup(C value, java.time.Duration remaining) {}

          public void close(C value) throws Exception {
            adapter.close(value);
          }

          public void close(C value, java.time.Duration remaining) throws Exception {
            adapter.close(value, remaining);
          }
        };
    LifecycleCoordinator.Owned<C> owned =
        lifecycleWork.initialize(
            bridge, settings, policy.warmupTimeout(), policy.shutdownTimeout());
    configurationAttempt.follow(owned::phase);
    return owned;
  }

  private void control(boolean pause) {
    lifecycleWork.control(
        () -> {
          if (pause) adapter.pause(consumer);
          else adapter.resume(consumer);
          return null;
        },
        policy.shutdownTimeout());
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

  public ConfigurationStatus configurationStatus() {
    return configuration.snapshot(current.revision(), state == State.CLOSED, revoked.get());
  }

  public java.util.List<ConfigurationStatus> configurationHistory() {
    return configuration.history();
  }

  public UpdateResult apply(ConfigurationRelease<S> release, String instanceId) {
    if (!resource.equals(release.snapshot().resource()))
      throw new IllegalArgumentException("Configuration scope mismatch");
    if (!release.targets(instanceId)) return UpdateResult.NOT_TARGETED;
    return reload(release.snapshot(), release.expectedRevision());
  }

  public UpdateResult reload(ConfigSnapshot<S> next) {
    return reload(next, null);
  }

  public UpdateResult reload(ConfigSnapshot<S> next, long expectedRevision) {
    if (expectedRevision < 1) throw new IllegalArgumentException("Invalid expected revision");
    return reload(next, Long.valueOf(expectedRevision));
  }

  private UpdateResult reload(ConfigSnapshot<S> next, Long expectedRevision) {
    Objects.requireNonNull(next);
    if (!resource.equals(next.resource()))
      throw new IllegalArgumentException("Configuration scope mismatch");
    synchronized (lifecycle) {
      configuration.observe(next.revision());
      if (next.revision() <= attempted) return UpdateResult.STALE;
      configurationAttempt = configuration.begin(next.revision());
      attempted = next.revision();
      try {
        if (expectedRevision != null && current.revision() != expectedRevision)
          throw new ClientException(
              ClientException.Kind.UPDATE_REJECTED, "Effective configuration revision conflict");
        UpdateResult result = reloadInternal(next);
        configuration.finish(configurationAttempt, current.revision(), null);
        return result;
      } catch (RuntimeException | Error error) {
        configuration.finish(configurationAttempt, current.revision(), error);
        throw error;
      }
    }
  }

  private UpdateResult reloadInternal(ConfigSnapshot<S> next) {
    if (!resource.equals(next.resource()))
      throw new IllegalArgumentException("Configuration scope mismatch");
    synchronized (lifecycle) {
      if (stopping.get() || revoked.get() || state == State.CLOSED || state == State.REVOKED)
        throw new ClientException(ClientException.Kind.CLOSED, "Consumer admission is closed");
      if (credentials != null && adapter.credentials(next.settings()) != credentials.source)
        throw new IllegalArgumentException(
            "Credential source changes require a new resource binding");
      adapter.validate(next.settings());
      adapter.validateUpdate(current.settings(), next.settings());
      adapter.updatePlan(current.settings(), next.settings()).requireAllowed();
      if (!current.schemaId().equals(next.schemaId())
          || current.schemaVersion() != next.schemaVersion())
        throw new IllegalArgumentException("Online schema changes require a new resource binding");
      if (state != State.RUNNING)
        throw new ClientException(
            ClientException.Kind.UPDATE_REJECTED, "Consumer requires explicit recovery");
      state = State.DRAINING;
      configurationAttempt.phase(ConfigurationStatus.Phase.DRAIN);
      try {
        long drainDeadline = System.nanoTime() + policy.shutdownTimeout().toNanos();
        control(true);
        drain(drainDeadline);
        synchronized (gate) {
          if (active > 0) {
            control(false);
            state = State.RUNNING;
            throw new ClientException(
                ClientException.Kind.TIMEOUT, "Consumer drain deadline exceeded");
          }
        }
        ownership.retire(drainDeadline, null);
        ownership.awaitClosed(drainDeadline);
        consumer = null;
        ownership = initialize(next.settings());
        try {
          consumer = ownership.awaitReady();
          if (stopping.get() || revoked.get())
            throw new ClientException(
                ClientException.Kind.REVOKED, "Consumer was revoked during startup");
          current = next;
          state = State.RUNNING;
          return UpdateResult.APPLIED;
        } catch (Exception replacementFailure) {
          configurationAttempt.failedPhase = configurationAttempt.phase();
          ownership.retire(System.nanoTime() + policy.shutdownTimeout().toNanos(), null);
          if (stopping.get() || revoked.get()) {
            state = revoked.get() ? State.REVOKED : State.CLOSED;
            throw new ClientException(
                ClientException.Kind.CLOSED,
                "Consumer stopped during replacement",
                replacementFailure);
          }
          try {
            // A timed-out start can still produce a group member. Recovery is safe only after
            // that candidate has actually terminated, never merely after its caller timed out.
            ownership.awaitClosed(System.nanoTime() + policy.shutdownTimeout().toNanos());
            ownership = initialize(current.settings());
            consumer = ownership.awaitReady();
            if (stopping.get() || revoked.get())
              throw new ClientException(
                  ClientException.Kind.CLOSED, "Consumer stopped during recovery");
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
        if (state == State.DRAINING) state = State.FAILED;
        throw e;
      } catch (Exception e) {
        state = State.FAILED;
        throw new ClientException(
            ClientException.Kind.UPDATE_REJECTED, "Consumer rotation failed", e);
      }
    }
  }

  public CredentialStatus credentialStatus() {
    CredentialWatch watch = credentials;
    return watch == null ? null : watch.status();
  }

  public void bindCredentials(CredentialSource source) {
    Objects.requireNonNull(source);
    synchronized (lifecycle) {
      if (stopping.get() || credentials != null)
        throw new IllegalStateException("Consumer cannot bind credentials");
      if (adapter.credentials(current.settings()) != source)
        throw new IllegalArgumentException("Credential source does not match adapter settings");
      credentialTimers =
          new ScheduledThreadPoolExecutor(
              1,
              task -> {
                Thread thread = new Thread(task, "driverkit-consumer-credentials");
                thread.setDaemon(true);
                return thread;
              });
      credentialTimers.setRemoveOnCancelPolicy(true);
      credentials =
          new CredentialWatch(
              source,
              resource,
              credentialTimers,
              updates,
              lifecycleWork,
              policy,
              () -> {
                synchronized (lifecycle) {
                  if (stopping.get()) return false;
                  configurationAttempt = new ConfigurationTracker.Attempt(current.revision());
                  try {
                    reloadInternal(current);
                    return true;
                  } catch (RuntimeException failure) {
                    throw failure;
                  }
                }
              },
              () -> {
                revoked.set(true);
                stopping.set(true);
                state = State.REVOKED;
              },
              this::close);
      try {
        credentials.start(null);
      } catch (RuntimeException | Error failure) {
        revoked.set(true);
        stopping.set(true);
        state = State.REVOKED;
        credentials.close(System.nanoTime() + policy.shutdownTimeout().toNanos());
        credentialTimers.shutdown();
        throw failure;
      }
    }
  }

  public void bind(ConfigurationSource<S> source) {
    synchronized (lifecycle) {
      if (subscription != null || state != State.RUNNING)
        throw new IllegalStateException("Consumer cannot bind a source");
      subscription =
          new ConfigurationBinding<>(
              lifecycleWork,
              source,
              snapshot -> {
                if (!resource.equals(snapshot.resource()))
                  throw new IllegalArgumentException("Configuration scope mismatch");
                configuration.observe(snapshot.revision());
                pending.accumulateAndGet(
                    snapshot,
                    (old, value) -> old == null || value.revision() > old.revision() ? value : old);
                schedule();
              },
              policy,
              null);
    }
  }

  private void schedule() {
    if (stopping.get()
        || state == State.CLOSED
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
    long deadline = System.nanoTime() + policy.shutdownTimeout().toNanos();
    stopping.set(true);
    CredentialWatch watch = credentials;
    if (watch != null) watch.close(deadline);
    if (credentialTimers != null) credentialTimers.shutdown();
    lifecycleWork.stopInitializing();
    synchronized (lifecycle) {
      if (cleanupRequested) return;
      cleanupRequested = true;
      state = revoke || revoked.get() ? State.REVOKED : State.DRAINING;
      pending.set(null);
      if (subscription != null) {
        subscription.close(deadline);
        subscription = null;
      }
      updates.shutdownNow();
      try {
        drain(deadline);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
      } finally {
        if (ownership != null) {
          ownership.retire(deadline, null);
          try {
            ownership.awaitClosed(deadline);
          } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
          } catch (TimeoutException ignored) {
            /* Cleanup remains owned and bounded in the background. */
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
}
