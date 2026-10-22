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
  private int ownedResources = 1;
  private ConfigurationBinding<S> subscription;
  private boolean bindingInProgress;
  private long closingDeadline = Long.MAX_VALUE;

  private final class Generation {
    final ConfigSnapshot<S> snapshot;
    final C client;
    final LifecycleCoordinator.Owned<C> ownership;
    int references;
    boolean retired;
    boolean closing;
    boolean disposed;

    Generation(ConfigSnapshot<S> snapshot, C client, LifecycleCoordinator.Owned<C> ownership) {
      this.snapshot = snapshot;
      this.client = client;
      this.ownership = ownership;
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
    LifecycleCoordinator.Owned<C> ownership;
    try {
      ownership =
          runtime.lifecycle.initialize(
              adapter, snapshot.settings(), policy.warmupTimeout(), policy.shutdownTimeout());
    } catch (RuntimeException | Error failure) {
      resourceFinished(null);
      throw failure;
    }
    ownership.whenClosed(this::resourceFinished);
    return new Generation(snapshot, ownership.awaitReady(), ownership);
  }

  private void resourceFinished(Throwable error) {
    synchronized (state) {
      ownedResources--;
      if (error != null) cleanupFailures++;
      state.notifyAll();
    }
    runtime.releaseRegistration(this);
  }

  boolean canUnregister() {
    synchronized (state) {
      return closed && ownedResources == 0 && cleanupFailures == 0;
    }
  }

  public ResourceId resource() {
    return resource;
  }

  public AdapterCapabilities capabilities() {
    return adapter.capabilities();
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
          rejectedUpdates,
          ownedResources);
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
          () -> release(generation),
          () -> {
            synchronized (state) {
              return !generation.closing && !generation.disposed;
            }
          });
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
    // Retiring ownership only queues native work. No adapter code runs under the state lock.
    dispose(generation);
  }

  private void dispose(Generation generation) {
    long deadline;
    synchronized (state) {
      deadline = Math.min(closingDeadline, System.nanoTime() + policy.shutdownTimeout().toNanos());
    }
    generation.ownership.retire(
        deadline,
        error -> {
          synchronized (state) {
            generation.disposed = true;
            if (retiring == generation) retiring = null;
            state.notifyAll();
          }
          runtime.releaseRegistration(this);
          schedulePending();
        });
  }

  public UpdateResult reload(ConfigSnapshot<S> snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    if (!resource.equals(snapshot.resource()))
      throw new IllegalArgumentException("Configuration resource scope mismatch");
    synchronized (updateLock) {
      S previousSettings;
      synchronized (state) {
        ensureOpen();
        if (snapshot.revision() <= attemptedRevision) return UpdateResult.STALE;
        if (retiring != null) return UpdateResult.BUSY;
        previousSettings = active.snapshot.settings();
      }
      // Validation is adapter code; it must not block close() or registry retirement's state lock.
      adapter.validateUpdate(previousSettings, snapshot.settings());
      synchronized (state) {
        ensureOpen();
        attemptedRevision = snapshot.revision();
        ownedResources++;
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

  public AutoCloseable bind(ConfigurationSource<S> source) {
    Objects.requireNonNull(source, "source");
    synchronized (this) {
      synchronized (state) {
        ensureOpen();
      }
      if (subscription != null || bindingInProgress)
        throw new IllegalStateException("A configuration source is already bound");
      synchronized (state) {
        ensureOpen();
        ownedResources++;
      }
      bindingInProgress = true;
    }
    ConfigurationBinding<S> candidate = null;
    try {
      candidate =
          new ConfigurationBinding<>(
              runtime.lifecycle, source, this::enqueue, policy, this::resourceFinished);
      synchronized (this) {
        synchronized (state) {
          ensureOpen();
        }
        subscription = candidate;
      }
      ConfigurationBinding<S> binding = candidate;
      return () -> {
        binding.close(System.nanoTime() + policy.shutdownTimeout().toNanos());
        synchronized (ManagedClient.this) {
          if (subscription == binding) subscription = null;
        }
      };
    } catch (RuntimeException failure) {
      if (candidate != null)
        candidate.close(System.nanoTime() + policy.shutdownTimeout().toNanos());
      throw failure;
    } finally {
      synchronized (this) {
        bindingInProgress = false;
      }
    }
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
    if (CompletionDispatcher.inCompletionThread())
      throw new ClientException(
          ClientException.Kind.INVALID_REQUEST,
          "Use asynchronous composition instead of a blocking SDK call inside a completion callback");
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
    long started = System.nanoTime();
    CallContext context = new CallContext(operation.timeout());
    CompletionDispatcher.Reservation completion = null;
    ClientSession<C> lease;
    try {
      runtime.admission.check(resource, operation);
      completion = runtime.reserveCompletion();
      lease = session();
    } catch (RuntimeException rejected) {
      if (completion != null) completion.release();
      Observation observation = start(operation, status().revision());
      try {
        observation.finish(Observation.Outcome.REJECTED, System.nanoTime() - started);
      } catch (RuntimeException | LinkageError ignored) {
      }
      CompletableFuture<T> rejectedResult = new CompletableFuture<>();
      rejectedResult.completeExceptionally(rejected);
      return rejectedResult;
    }
    Invocation<T> invocation =
        new Invocation<>(operation, call, lease, completion, started, context);
    outstanding.put(invocation.result, invocation::abort);
    invocation.submit();
    return invocation.result;
  }

  private static final class CallFuture<T> extends CompletableFuture<T> {
    private final java.util.function.BooleanSupplier cancellation;

    CallFuture(java.util.function.BooleanSupplier cancellation) {
      this.cancellation = cancellation;
    }

    @Override
    public boolean cancel(boolean interrupt) {
      return cancellation.getAsBoolean();
    }

    boolean cancelDirectly() {
      return super.cancel(false);
    }
  }

  private final class Invocation<T> {
    final Operation operation;
    final AsyncCall<C, T> call;
    final ClientSession<C> lease;
    final CompletionDispatcher.Reservation completion;
    final long start;
    final CallContext context;
    final Observation observation;
    final CallFuture<T> result = new CallFuture<>(this::cancel);
    final Object dispatch = new Object();
    final Runnable task = this::run;
    ScheduledFuture<?> deadline;
    boolean started, terminal;

    Invocation(
        Operation operation,
        AsyncCall<C, T> call,
        ClientSession<C> lease,
        CompletionDispatcher.Reservation completion,
        long start,
        CallContext context) {
      this.operation = operation;
      this.call = call;
      this.lease = lease;
      this.completion = completion;
      this.start = start;
      this.context = context;
      context.bindSettings(lease.settings(Object.class));
      observation = start(operation, lease.revision());
    }

    void releasePhysical() {
      outstanding.remove(result);
      lease.close();
    }

    void discardQueued() {
      if (!started) {
        runtime.calls.remove(task);
        releasePhysical();
      }
    }

    boolean select() {
      if (terminal) return false;
      terminal = true;
      if (deadline != null) deadline.cancel(false);
      discardQueued();
      return true;
    }

    void observe(Observation.Outcome outcome, long elapsed) {
      try {
        observation.finish(outcome, elapsed);
      } catch (RuntimeException | LinkageError ignored) {
      }
    }

    void deliver(T value, Throwable error, Observation.Outcome outcome) {
      long elapsed = System.nanoTime() - start;
      completion.deliver(
          () -> {
            observe(outcome, elapsed);
            if (error == null) result.complete(value);
            else result.completeExceptionally(error);
          });
    }

    boolean cancel() {
      synchronized (dispatch) {
        if (!select()) return result.isCancelled();
      }
      long elapsed = System.nanoTime() - start;
      completion.deliver(() -> observe(Observation.Outcome.CANCELLED, elapsed));
      // Explicit caller cancellation keeps CompletableFuture's immediate state change and runs
      // its synchronous dependents on that caller, never a driver or timer thread.
      return result.cancelDirectly();
    }

    void abort() {
      boolean selected;
      Throwable error;
      synchronized (dispatch) {
        error =
            new ClientException(
                !operation.replaySafe() && started
                    ? ClientException.Kind.WRITE_OUTCOME_UNKNOWN
                    : ClientException.Kind.CLOSED,
                "Client shutdown deadline reached");
        selected = select();
      }
      releasePhysical();
      if (selected) deliver(null, error, Observation.Outcome.ERROR);
    }

    void expire() {
      Throwable error;
      synchronized (dispatch) {
        error =
            new ClientException(
                !operation.replaySafe() && started
                    ? ClientException.Kind.WRITE_OUTCOME_UNKNOWN
                    : ClientException.Kind.TIMEOUT,
                "Operation deadline exceeded");
        if (!select()) return;
      }
      deliver(null, error, Observation.Outcome.TIMEOUT);
    }

    void submit() {
      try {
        synchronized (dispatch) {
          if (terminal) return;
          deadline =
              runtime.timers.schedule(
                  this::expire, Math.max(1, context.remaining().toNanos()), TimeUnit.NANOSECONDS);
          runtime.calls.execute(task);
        }
      } catch (RejectedExecutionException rejected) {
        finish(
            null,
            new ClientException(
                ClientException.Kind.OVERLOADED,
                "Runtime no longer accepts queued calls",
                rejected),
            Observation.Outcome.REJECTED);
      }
    }

    void finish(T value, Throwable error, Observation.Outcome outcome) {
      boolean selected;
      synchronized (dispatch) {
        selected = select();
      }
      // Native work has finished: release capacity before any user callback is delivered.
      releasePhysical();
      if (selected) deliver(value, error, outcome);
    }

    void run() {
      try {
        synchronized (dispatch) {
          if (terminal) return;
          context.checkDeadline();
          synchronized (state) {
            ensureOpen();
          }
          started = true;
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
            (value, error) ->
                finish(
                    value,
                    error == null ? null : translate(error, operation),
                    error == null
                        ? Observation.Outcome.SUCCESS
                        : unwrap(error) instanceof CancellationException
                            ? Observation.Outcome.CANCELLED
                            : Observation.Outcome.ERROR));
      } catch (Throwable error) {
        finish(null, translate(error, operation), Observation.Outcome.ERROR);
      }
    }
  }

  /** Revocation immediately closes admission. Existing server-side effects cannot be undone. */
  public void revoke() {
    synchronized (state) {
      revoked = true;
      pending.set(null);
    }
  }

  void beginClose() {
    beginClose(System.nanoTime() + policy.shutdownTimeout().toNanos());
  }

  void beginClose(long deadline) {
    ConfigurationBinding<S> binding;
    synchronized (this) {
      synchronized (state) {
        closingDeadline = Math.min(closingDeadline, deadline);
        closed = true;
        pending.set(null);
        active.retired = true;
        if (active.references == 0) scheduleDisposal(active);
        if (retiring != null && retiring.references == 0) scheduleDisposal(retiring);
        state.notifyAll();
      }
      binding = subscription;
      subscription = null;
    }
    if (binding != null) binding.close(deadline);
  }

  void awaitClose(long deadline) {
    synchronized (state) {
      while ((!active.disposed || retiring != null || ownedResources != 0)
          && deadline - System.nanoTime() > 0) {
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
    forceClose(System.nanoTime() + policy.shutdownTimeout().toNanos());
  }

  void forceClose(long deadline) {
    synchronized (state) {
      closingDeadline = Math.min(closingDeadline, deadline);
      active.ownership.retire(closingDeadline, null);
      if (retiring != null) retiring.ownership.retire(closingDeadline, null);
      scheduleDisposal(active);
      if (retiring != null) scheduleDisposal(retiring);
    }
    for (Runnable abort : outstanding.values()) abort.run();
  }

  @Override
  public void close() {
    long deadline = System.nanoTime() + policy.shutdownTimeout().toNanos();
    beginClose(deadline);
    awaitClose(deadline);
    forceClose(deadline);
    runtime.releaseRegistration(this);
  }
}
