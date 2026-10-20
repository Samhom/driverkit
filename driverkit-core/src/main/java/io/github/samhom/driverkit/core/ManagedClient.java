package io.github.samhom.driverkit.core;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Stable handle; the state lock linearizes acquisition, publication, retirement and revocation. */
public final class ManagedClient<C, S> implements AutoCloseable {
  private final ClientRuntime runtime;
  private final DriverAdapter<C, S> adapter;
  private final ClientPolicy policy;
  private final Observer observer;
  private final ResourceId resource;
  private final Object state = new Object();
  private final Object updateLock = new Object();
  private final AtomicReference<ConfigSnapshot<S>> pending = new AtomicReference<>();
  private final AtomicBoolean updateScheduled = new AtomicBoolean();
  private final ConcurrentMap<CompletableFuture<?>, Runnable> outstanding =
      new ConcurrentHashMap<>();
  private Generation active;
  private Generation retiring;
  private long attemptedRevision;
  private long rejectedUpdates;
  private int inFlight;
  private boolean closed;
  private boolean revoked;
  private int cleanupFailures;
  private AutoCloseable subscription;

  private final class Generation {
    final ConfigSnapshot<S> snapshot;
    final C client;
    int references;
    boolean retired;
    boolean closing;
    boolean disposed;

    Generation(ConfigSnapshot<S> snapshot, C client) {
      this.snapshot = snapshot;
      this.client = client;
    }
  }

  ManagedClient(
      ClientRuntime runtime,
      ConfigSnapshot<S> initial,
      DriverAdapter<C, S> adapter,
      ClientPolicy policy,
      Observer observer) {
    this.runtime = runtime;
    this.adapter = adapter;
    this.policy = policy;
    this.observer = observer;
    this.resource = initial.resource();
    active = create(initial);
    attemptedRevision = initial.revision();
  }

  private Generation create(ConfigSnapshot<S> snapshot) {
    adapter.validate(snapshot.settings());
    C candidate = null;
    try {
      candidate =
          Objects.requireNonNull(adapter.create(snapshot.settings()), "Adapter returned no client");
      adapter.warmup(candidate, policy.warmupTimeout());
      return new Generation(snapshot, candidate);
    } catch (Exception e) {
      if (candidate != null)
        try {
          adapter.close(candidate);
        } catch (Exception closeFailure) {
          e.addSuppressed(closeFailure);
        }
      throw new ClientException(
          ClientException.Kind.UPDATE_REJECTED, "Client initialization failed", e);
    }
  }

  public ResourceId resource() {
    return resource;
  }

  public String driverId() {
    return adapter.id();
  }

  public ClientStatus status() {
    synchronized (state) {
      return new ClientStatus(
          active.snapshot.revision(),
          inFlight,
          retiring == null ? 0 : 1,
          closed,
          revoked,
          attemptedRevision,
          rejectedUpdates);
    }
  }

  public int cleanupFailures() {
    synchronized (state) {
      return cleanupFailures;
    }
  }

  public ClientSession<C> session() {
    synchronized (state) {
      ensureOpen();
      if (inFlight >= policy.maxInFlight())
        throw new ClientException(
            ClientException.Kind.OVERLOADED, "Resource in-flight limit reached");
      Generation generation = active;
      generation.references++;
      inFlight++;
      return new ClientSession<>(
          generation.client,
          generation.snapshot.revision(),
          generation.snapshot.settings(),
          () -> release(generation));
    }
  }

  private void ensureOpen() {
    if (closed) throw new ClientException(ClientException.Kind.CLOSED, "Client is closed");
    if (revoked)
      throw new ClientException(ClientException.Kind.REVOKED, "Resource access has been revoked");
  }

  private void release(Generation generation) {
    synchronized (state) {
      generation.references--;
      inFlight--;
      if (generation.retired && generation.references == 0) scheduleDisposal(generation);
      state.notifyAll();
    }
  }

  private void scheduleDisposal(Generation generation) {
    if (generation.closing || generation.disposed) return;
    generation.closing = true;
    try {
      runtime.maintenance.execute(() -> dispose(generation));
    } catch (RejectedExecutionException closedRuntime) {
      dispose(generation);
    }
  }

  private void dispose(Generation generation) {
    try {
      adapter.close(generation.client);
    } catch (Exception ignored) {
      synchronized (state) {
        cleanupFailures++;
      }
    } finally {
      synchronized (state) {
        generation.disposed = true;
        if (retiring == generation) retiring = null;
        state.notifyAll();
      }
      schedulePending();
    }
  }

  public UpdateResult reload(ConfigSnapshot<S> snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    if (!resource.equals(snapshot.resource()))
      throw new IllegalArgumentException("Configuration resource scope mismatch");
    synchronized (updateLock) {
      synchronized (state) {
        ensureOpen();
        if (snapshot.revision() <= attemptedRevision) return UpdateResult.STALE;
        if (retiring != null) return UpdateResult.BUSY;
        adapter.validateUpdate(active.snapshot.settings(), snapshot.settings());
        attemptedRevision = snapshot.revision();
      }
      Generation candidate;
      try {
        candidate = create(snapshot);
      } catch (RuntimeException error) {
        synchronized (state) {
          rejectedUpdates++;
        }
        throw error;
      }
      synchronized (state) {
        if (closed || revoked) {
          candidate.retired = true;
          scheduleDisposal(candidate);
          throw new ClientException(
              ClientException.Kind.UPDATE_REJECTED, "Client no longer accepts updates");
        }
        Generation previous = active;
        active = candidate;
        previous.retired = true;
        retiring = previous;
        if (previous.references == 0) scheduleDisposal(previous);
        return UpdateResult.APPLIED;
      }
    }
  }

  public synchronized AutoCloseable bind(ConfigurationSource<S> source) {
    Objects.requireNonNull(source, "source");
    synchronized (state) {
      ensureOpen();
    }
    if (subscription != null)
      throw new IllegalStateException("A configuration source is already bound");
    subscription = source.subscribe(this::enqueue);
    return () -> {
      synchronized (ManagedClient.this) {
        if (subscription != null) {
          subscription.close();
          subscription = null;
        }
      }
    };
  }

  private void enqueue(ConfigSnapshot<S> snapshot) {
    if (!resource.equals(snapshot.resource()))
      throw new IllegalArgumentException("Configuration resource scope mismatch");
    pending.accumulateAndGet(
        snapshot, (old, next) -> old == null || next.revision() > old.revision() ? next : old);
    schedulePending();
  }

  private void schedulePending() {
    synchronized (state) {
      if (closed || revoked || retiring != null || pending.get() == null) return;
    }
    if (!updateScheduled.compareAndSet(false, true)) return;
    try {
      runtime.maintenance.execute(
          () -> {
            try {
              ConfigSnapshot<S> next = pending.getAndSet(null);
              if (next != null && reload(next) == UpdateResult.BUSY)
                pending.accumulateAndGet(
                    next,
                    (old, value) -> old == null || value.revision() > old.revision() ? value : old);
            } catch (RuntimeException | LinkageError ignored) {
              /* Active generation remains valid; a higher revision can retry. */
            } finally {
              updateScheduled.set(false);
              schedulePending();
            }
          });
    } catch (RejectedExecutionException e) {
      updateScheduled.set(false);
    }
  }

  private Observation start(Operation operation, long revision) {
    try {
      return Objects.requireNonNull(
          observer.start(resource, adapter.id(), operation, revision), "observation");
    } catch (RuntimeException | LinkageError ignored) {
      return Observation.NOOP;
    }
  }

  private static Throwable unwrap(Throwable error) {
    while ((error instanceof CompletionException || error instanceof ExecutionException)
        && error.getCause() != null) error = error.getCause();
    return error;
  }

  private Throwable translate(Throwable error, Operation operation) {
    Throwable root = unwrap(error);
    if (root instanceof ClientException
        || root instanceof Error
        || root instanceof CancellationException) return root;
    ClientException.Kind kind;
    try {
      kind = adapter.classify(root, operation);
    } catch (RuntimeException | LinkageError ignored) {
      kind = ClientException.Kind.DRIVER_FAILURE;
    }
    return new ClientException(kind, "Native operation failed", root);
  }

  public <T> T execute(Operation operation, Call<C, T> call) {
    Objects.requireNonNull(call, "call");
    try {
      return executeAsync(
              operation,
              (client, context) -> CompletableFuture.completedFuture(call.execute(client, context)))
          .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ClientException(
          operation.replaySafe()
              ? ClientException.Kind.DRIVER_FAILURE
              : ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
          "Caller interrupted; native outcome may be unknown",
          e);
    } catch (ExecutionException e) {
      Throwable cause = unwrap(e);
      if (cause instanceof RuntimeException) throw (RuntimeException) cause;
      if (cause instanceof Error) throw (Error) cause;
      throw new ClientException(
          ClientException.Kind.DRIVER_FAILURE, "Native operation failed", cause);
    }
  }

  public <T> CompletableFuture<T> executeAsync(Operation operation, AsyncCall<C, T> call) {
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(call, "call");
    final long start = System.nanoTime();
    final CallContext context = new CallContext(operation.timeout());
    final CompletableFuture<T> result = new CompletableFuture<>();
    final ClientSession<C> lease;
    try {
      runtime.admission.check(resource, operation);
      lease = session();
    } catch (RuntimeException rejected) {
      Observation observation = start(operation, status().revision());
      try {
        observation.finish(Observation.Outcome.REJECTED, System.nanoTime() - start);
      } catch (RuntimeException | LinkageError ignored) {
      }
      result.completeExceptionally(rejected);
      return result;
    }
    final Observation observation = start(operation, lease.revision());
    context.bindSettings(lease.settings(Object.class));
    final AtomicBoolean observed = new AtomicBoolean();
    final Object dispatch = new Object();
    final AtomicBoolean started = new AtomicBoolean();
    java.util.function.Consumer<Observation.Outcome> finish =
        outcome -> {
          if (observed.compareAndSet(false, true))
            try {
              observation.finish(outcome, System.nanoTime() - start);
            } catch (RuntimeException | LinkageError ignored) {
            }
        };
    Runnable releasePhysical =
        () -> {
          outstanding.remove(result);
          lease.close();
        };
    outstanding.put(
        result,
        () -> {
          synchronized (dispatch) {
            ClientException.Kind kind =
                !operation.replaySafe() && started.get()
                    ? ClientException.Kind.WRITE_OUTCOME_UNKNOWN
                    : ClientException.Kind.CLOSED;
            if (result.completeExceptionally(
                new ClientException(kind, "Client shutdown deadline reached")))
              finish.accept(Observation.Outcome.ERROR);
          }
          releasePhysical.run();
        });
    result.whenComplete(
        (value, error) -> {
          if (result.isCancelled()) finish.accept(Observation.Outcome.CANCELLED);
        });
    final ScheduledFuture<?> deadline;
    try {
      deadline =
          runtime.timers.schedule(
              () -> {
                synchronized (dispatch) {
                  ClientException.Kind kind =
                      !operation.replaySafe() && started.get()
                          ? ClientException.Kind.WRITE_OUTCOME_UNKNOWN
                          : ClientException.Kind.TIMEOUT;
                  if (result.completeExceptionally(
                      new ClientException(kind, "Operation deadline exceeded")))
                    finish.accept(Observation.Outcome.TIMEOUT);
                }
              },
              Math.max(1, context.remaining().toNanos()),
              TimeUnit.NANOSECONDS);
    } catch (RejectedExecutionException e) {
      releasePhysical.run();
      result.completeExceptionally(e);
      finish.accept(Observation.Outcome.REJECTED);
      return result;
    }
    result.whenComplete((value, error) -> deadline.cancel(false));
    try {
      runtime.calls.execute(
          () -> {
            try {
              synchronized (dispatch) {
                if (result.isDone()) {
                  releasePhysical.run();
                  return;
                }
                context.checkDeadline();
                started.set(true);
              }
              synchronized (state) {
                ensureOpen();
              }
              AutoCloseable scope;
              try {
                scope = observation.openScope();
              } catch (RuntimeException | LinkageError ignored) {
                scope = () -> {};
              }
              CompletionStage<T> nativeResult;
              try {
                nativeResult =
                    Objects.requireNonNull(
                        call.execute(lease.client(), context),
                        "Native operation returned no completion stage");
              } finally {
                try {
                  scope.close();
                } catch (Exception | LinkageError ignored) {
                }
              }
              nativeResult.whenComplete(
                  (value, error) -> {
                    try {
                      if (error == null) {
                        if (result.complete(value)) finish.accept(Observation.Outcome.SUCCESS);
                      } else if (result.completeExceptionally(translate(error, operation)))
                        finish.accept(
                            unwrap(error) instanceof CancellationException
                                ? Observation.Outcome.CANCELLED
                                : Observation.Outcome.ERROR);
                    } finally {
                      releasePhysical.run();
                    }
                  });
            } catch (Throwable error) {
              try {
                if (result.completeExceptionally(translate(error, operation)))
                  finish.accept(Observation.Outcome.ERROR);
              } finally {
                releasePhysical.run();
              }
            }
          });
    } catch (RejectedExecutionException rejected) {
      releasePhysical.run();
      result.completeExceptionally(
          new ClientException(
              ClientException.Kind.OVERLOADED, "Runtime call queue is full", rejected));
      finish.accept(Observation.Outcome.REJECTED);
    }
    return result;
  }

  /** Revocation immediately closes admission. Existing server-side effects cannot be undone. */
  public void revoke() {
    synchronized (state) {
      revoked = true;
      pending.set(null);
    }
  }

  void beginClose() {
    synchronized (this) {
      if (subscription != null)
        try {
          subscription.close();
        } catch (Exception ignored) {
        } finally {
          subscription = null;
        }
    }
    synchronized (state) {
      if (closed) return;
      closed = true;
      pending.set(null);
      active.retired = true;
      if (active.references == 0) scheduleDisposal(active);
      if (retiring != null && retiring.references == 0) scheduleDisposal(retiring);
      state.notifyAll();
    }
  }

  void awaitClose(long deadline) {
    synchronized (state) {
      while ((!active.disposed || retiring != null) && deadline - System.nanoTime() > 0) {
        try {
          TimeUnit.NANOSECONDS.timedWait(state, deadline - System.nanoTime());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  void forceClose() {
    synchronized (state) {
      scheduleDisposal(active);
      if (retiring != null) scheduleDisposal(retiring);
    }
    for (Runnable abort : outstanding.values()) abort.run();
  }

  @Override
  public void close() {
    beginClose();
    awaitClose(System.nanoTime() + policy.shutdownTimeout().toNanos());
    forceClose();
  }
}
