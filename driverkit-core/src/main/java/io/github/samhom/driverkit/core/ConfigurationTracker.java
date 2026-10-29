package io.github.samhom.driverkit.core;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;

final class ConfigurationTracker {
  static final class Attempt {
    final long revision, started = System.nanoTime();
    volatile ConfigurationStatus.Phase phase = ConfigurationStatus.Phase.VALIDATION, failedPhase;
    volatile Supplier<ConfigurationStatus.Phase> nativePhase;
    long ended;
    ClientException.Kind failure;

    Attempt(long revision) {
      this.revision = revision;
    }

    ConfigurationStatus.Phase phase() {
      Supplier<ConfigurationStatus.Phase> p = nativePhase;
      return p == null ? phase : p.get();
    }

    void phase(ConfigurationStatus.Phase value) {
      phase = value;
      nativePhase = null;
    }

    void follow(Supplier<ConfigurationStatus.Phase> supplier) {
      nativePhase = supplier;
    }
  }

  private long desired, attempted;
  private Attempt last;
  private final Deque<ConfigurationStatus> history = new ArrayDeque<>();

  synchronized void observe(long revision) {
    desired = Math.max(desired, revision);
  }

  synchronized Attempt begin(long revision) {
    observe(revision);
    attempted = Math.max(attempted, revision);
    last = new Attempt(revision);
    return last;
  }

  synchronized void finish(Attempt attempt, long effective, Throwable error) {
    attempt.ended = System.nanoTime();
    if (error != null) {
      if (attempt.failedPhase == null) attempt.failedPhase = attempt.phase();
      attempt.failure =
          error instanceof ClientException
              ? ((ClientException) error).kind()
              : error instanceof IllegalArgumentException
                  ? ClientException.Kind.INVALID_REQUEST
                  : ClientException.Kind.DRIVER_FAILURE;
      attempt.phase(ConfigurationStatus.Phase.FAILED);
    } else attempt.phase(ConfigurationStatus.Phase.APPLIED);
    if (history.size() == 64) history.removeFirst();
    history.addLast(status(attempt, effective, false, false));
  }

  synchronized ConfigurationStatus snapshot(long effective, boolean closed, boolean revoked) {
    return status(last, effective, closed, revoked);
  }

  private ConfigurationStatus status(
      Attempt attempt, long effective, boolean closed, boolean revoked) {
    ConfigurationStatus.Phase phase =
        attempt == null ? ConfigurationStatus.Phase.QUEUED : attempt.phase();
    if (attempt != null && attempt.ended != 0 && desired > attempted)
      phase = ConfigurationStatus.Phase.WAITING;
    if (closed) phase = ConfigurationStatus.Phase.CLOSED;
    if (revoked) phase = ConfigurationStatus.Phase.REVOKED;
    long elapsed =
        attempt == null
            ? 0
            : Math.max(
                0, (attempt.ended == 0 ? System.nanoTime() : attempt.ended) - attempt.started);
    return new ConfigurationStatus(
        desired,
        attempted,
        effective,
        phase,
        attempt == null ? null : attempt.failedPhase,
        attempt == null ? null : attempt.failure,
        elapsed,
        Instant.now());
  }

  synchronized List<ConfigurationStatus> history() {
    return Collections.unmodifiableList(new ArrayList<>(history));
  }
}
