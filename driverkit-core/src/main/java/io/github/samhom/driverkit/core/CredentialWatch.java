package io.github.samhom.driverkit.core;

import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Two bounded timers per binding. Native work and external refresh never run on a timer thread. */
final class CredentialWatch {
  final CredentialSource source;
  private final ResourceId resource;
  private final ScheduledExecutorService timers;
  private final Executor executor;
  private final LifecycleCoordinator lifecycle;
  private final ClientPolicy policy;
  private final Callable<Boolean> rotate;
  private final Runnable revoke, shutdown;
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private volatile CredentialState state;
  private volatile long activeVersion;
  private volatile long expiryDeadline = Long.MAX_VALUE;
  private volatile boolean closed, terminal;
  private volatile ClientException.Kind failure;
  private int rotationFailures;
  private long refreshedVersion;
  private ScheduledFuture<?> expiry, refresh, retry;
  private ConfigurationBinding<CredentialState> binding;

  CredentialWatch(
      CredentialSource source,
      ResourceId resource,
      ScheduledExecutorService timers,
      Executor executor,
      LifecycleCoordinator lifecycle,
      ClientPolicy policy,
      Callable<Boolean> rotate,
      Runnable revoke,
      Runnable shutdown) {
    this.source = source;
    this.resource = resource;
    this.timers = timers;
    this.executor = executor;
    this.lifecycle = lifecycle;
    this.policy = policy;
    this.rotate = rotate;
    this.revoke = revoke;
    this.shutdown = shutdown;
  }

  void start(Consumer<Throwable> finished) {
    ConfigurationBinding<CredentialState> candidate =
        new ConfigurationBinding<>(
            lifecycle,
            listener ->
                source.subscribe(
                    value ->
                        listener.accept(new ConfigSnapshot<>(resource, value.version(), value))),
            snapshot -> receive(snapshot.settings()),
            policy,
            finished);
    boolean stop;
    synchronized (this) {
      binding = candidate;
      stop = closed;
    }
    if (stop) candidate.close(System.nanoTime() + policy.shutdownTimeout().toNanos());
  }

  private synchronized void receive(CredentialState next) {
    if (closed
        || terminal
        || (state != null
            && (next.version() < state.version()
                || (next.version() == state.version() && !next.revoked())))) return;
    state = next;
    Duration lifetime = Duration.between(Instant.now(), next.expiresAt());
    expiryDeadline =
        lifetime.compareTo(Duration.ofDays(365)) <= 0
            ? System.nanoTime() + Math.max(0, lifetime.toNanos())
            : Long.MAX_VALUE;
    failure = null;
    rotationFailures = 0;
    cancel(expiry);
    cancel(refresh);
    if (next.revoked() || !Instant.now().isBefore(next.expiresAt())) {
      revokeNow();
      return;
    }
    expiry = timers.schedule(this::revokeNow, delay(next.expiresAt()), TimeUnit.NANOSECONDS);
    refresh = timers.schedule(this::refresh, delay(next.refreshAt()), TimeUnit.NANOSECONDS);
    scheduleRotation();
  }

  private static long delay(Instant instant) {
    Duration remaining = Duration.between(Instant.now(), instant);
    if (remaining.isNegative()) return 0;
    return remaining.compareTo(Duration.ofDays(365)) > 0
        ? Duration.ofDays(365).toNanos()
        : remaining.toNanos();
  }

  private static void cancel(Future<?> future) {
    if (future != null) future.cancel(false);
  }

  private void refresh() {
    synchronized (this) {
      if (closed || terminal || state == null || refreshedVersion >= state.version()) return;
      refreshedVersion = state.version();
    }
    try {
      executor.execute(
          () -> {
            try {
              lifecycle.control(
                  () -> {
                    source.refresh();
                    return null;
                  },
                  policy.warmupTimeout());
            } catch (RuntimeException error) {
              failure = ClientException.Kind.UNAVAILABLE;
            }
          });
    } catch (RejectedExecutionException error) {
      failure = ClientException.Kind.OVERLOADED;
    }
  }

  private void scheduleRotation() {
    if (closed || terminal || !scheduled.compareAndSet(false, true)) return;
    try {
      executor.execute(
          () -> {
            long target = state.version();
            boolean applied = false;
            try {
              checkLifetime();
              applied = rotate.call();
              if (applied && !terminal) {
                activeVersion = target;
                failure = null;
              }
            } catch (Exception error) {
              synchronized (this) {
                if (state.version() == target) rotationFailures++;
              }
              failure =
                  error instanceof ClientException
                      ? ((ClientException) error).kind()
                      : ClientException.Kind.DRIVER_FAILURE;
            } finally {
              scheduled.set(false);
              synchronized (this) {
                if (!closed
                    && !terminal
                    && rotationFailures < 3
                    && (!applied || state.version() != target)) {
                  cancel(retry);
                  retry = timers.schedule(this::scheduleRotation, 200, TimeUnit.MILLISECONDS);
                }
              }
            }
          });
    } catch (RejectedExecutionException error) {
      scheduled.set(false);
      failure = ClientException.Kind.OVERLOADED;
    }
  }

  private void checkLifetime() {
    CredentialState value = state;
    if (value == null)
      throw new ClientException(
          ClientException.Kind.UNAVAILABLE, "Credential subscription is initializing");
    if (terminal
        || value.revoked()
        || !Instant.now().isBefore(value.expiresAt())
        || (expiryDeadline != Long.MAX_VALUE && System.nanoTime() - expiryDeadline >= 0)) {
      revokeNow();
      throw new ClientException(ClientException.Kind.REVOKED, "Credentials are expired or revoked");
    }
  }

  void check() {
    if (closed)
      throw new ClientException(ClientException.Kind.CLOSED, "Credential binding is closed");
    checkLifetime();
    if (activeVersion != state.version())
      throw new ClientException(
          ClientException.Kind.UNAVAILABLE, "Credential generation is rotating");
  }

  private synchronized void revokeNow() {
    if (closed || terminal) return;
    // A long lifetime may have been capped to one year for scheduling arithmetic.
    if (state != null
        && !state.revoked()
        && Instant.now().isBefore(state.expiresAt())
        && (expiryDeadline == Long.MAX_VALUE || System.nanoTime() - expiryDeadline < 0)) {
      cancel(expiry);
      expiry = timers.schedule(this::revokeNow, delay(state.expiresAt()), TimeUnit.NANOSECONDS);
      return;
    }
    terminal = true;
    cancel(expiry);
    cancel(refresh);
    cancel(retry);
    revoke.run();
    try {
      executor.execute(shutdown);
    } catch (RejectedExecutionException ignored) {
      /* Admission is already closed; caller close retains cleanup ownership. */
    }
  }

  CredentialStatus status() {
    CredentialState value = state;
    return new CredentialStatus(
        value == null ? 0 : value.version(),
        activeVersion,
        value == null ? null : value.expiresAt(),
        terminal
            ? CredentialStatus.State.REVOKED
            : closed
                ? CredentialStatus.State.CLOSED
                : failure != null
                    ? CredentialStatus.State.FAILED
                    : value != null && activeVersion == value.version()
                        ? CredentialStatus.State.ACTIVE
                        : CredentialStatus.State.ROTATING,
        failure);
  }

  void close(long deadline) {
    ConfigurationBinding<CredentialState> owned;
    synchronized (this) {
      closed = true;
      cancel(expiry);
      cancel(refresh);
      cancel(retry);
      owned = binding;
    }
    if (owned != null) owned.close(deadline);
  }
}
