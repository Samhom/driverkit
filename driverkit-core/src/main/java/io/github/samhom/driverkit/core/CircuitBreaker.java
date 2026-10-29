package io.github.samhom.driverkit.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/** One caller-owned half-open probe; epochs ignore late results from earlier circuit states. */
final class CircuitBreaker {
  private final ExecutionPolicy policy;
  private final LongSupplier clock;
  private CircuitStatus.State state = CircuitStatus.State.CLOSED;
  private int failures, probes;
  private long epoch, until;

  CircuitBreaker(ExecutionPolicy policy) {
    this(policy, System::nanoTime);
  }

  CircuitBreaker(ExecutionPolicy policy, LongSupplier clock) {
    this.policy = policy;
    this.clock = clock;
  }

  synchronized Permit acquire() {
    if (policy.failureThreshold() == 0) return new Permit(epoch, false);
    if (state == CircuitStatus.State.OPEN) {
      if (clock.getAsLong() - until < 0)
        throw new ClientException(ClientException.Kind.CIRCUIT_OPEN, "Resource circuit is open");
      state = CircuitStatus.State.HALF_OPEN;
      epoch++;
      probes = 0;
    }
    if (state == CircuitStatus.State.HALF_OPEN) {
      if (probes != 0)
        throw new ClientException(
            ClientException.Kind.CIRCUIT_OPEN, "Circuit recovery probe is in flight");
      probes++;
    }
    return new Permit(epoch, state == CircuitStatus.State.HALF_OPEN);
  }

  synchronized CircuitStatus status() {
    return new CircuitStatus(
        policy.failureThreshold() == 0 ? CircuitStatus.State.DISABLED : state,
        failures,
        state == CircuitStatus.State.OPEN ? Math.max(0, until - clock.getAsLong()) : 0);
  }

  final class Permit {
    final long generation;
    final boolean probe;
    final AtomicBoolean finished = new AtomicBoolean();

    Permit(long generation, boolean probe) {
      this.generation = generation;
      this.probe = probe;
    }

    void abandon() {
      if (!finished.compareAndSet(false, true)) return;
      synchronized (CircuitBreaker.this) {
        if (generation == epoch && probe) probes = 0;
      }
    }

    void complete(Throwable error) {
      if (!finished.compareAndSet(false, true)) return;
      synchronized (CircuitBreaker.this) {
        if (policy.failureThreshold() == 0 || generation != epoch) return;
        if (error == null) {
          failures = 0;
          if (probe) {
            state = CircuitStatus.State.CLOSED;
            probes = 0;
            epoch++;
          }
          return;
        }
        ClientException.Kind kind =
            error instanceof ClientException
                ? ((ClientException) error).kind()
                : ClientException.Kind.DRIVER_FAILURE;
        boolean transientFailure =
            kind == ClientException.Kind.TIMEOUT
                || kind == ClientException.Kind.UNAVAILABLE
                || kind == ClientException.Kind.CONNECTION_FAILURE
                || kind == ClientException.Kind.DRIVER_FAILURE
                || kind == ClientException.Kind.WRITE_OUTCOME_UNKNOWN
                || kind == ClientException.Kind.RATE_LIMITED
                || kind == ClientException.Kind.SERVER_REJECTED;
        if (!transientFailure) {
          if (probe) probes = 0;
          return;
        }
        if (++failures >= policy.failureThreshold() || probe) {
          state = CircuitStatus.State.OPEN;
          until = clock.getAsLong() + policy.openFor().toNanos();
          probes = 0;
          epoch++;
        }
      }
    }
  }
}
