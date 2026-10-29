package io.github.samhom.driverkit.core;

import java.time.Instant;

/** Immutable publication metadata; intentionally contains no settings, cause or error message. */
public final class ConfigurationStatus {
  public enum Phase {
    QUEUED,
    VALIDATION,
    CREATE,
    WARMUP,
    DRAIN,
    RECOVERY,
    PUBLISH,
    APPLIED,
    FAILED,
    WAITING,
    CLOSED,
    REVOKED
  }

  private final long desiredRevision, attemptedRevision, effectiveRevision, elapsedNanos;
  private final Phase phase, failedPhase;
  private final ClientException.Kind failureKind;
  private final Instant observedAt;

  ConfigurationStatus(
      long desired,
      long attempted,
      long effective,
      Phase phase,
      Phase failedPhase,
      ClientException.Kind failureKind,
      long elapsedNanos,
      Instant observedAt) {
    this.desiredRevision = desired;
    this.attemptedRevision = attempted;
    this.effectiveRevision = effective;
    this.phase = phase;
    this.failedPhase = failedPhase;
    this.failureKind = failureKind;
    this.elapsedNanos = elapsedNanos;
    this.observedAt = observedAt;
  }

  public long desiredRevision() {
    return desiredRevision;
  }

  public long attemptedRevision() {
    return attemptedRevision;
  }

  public long effectiveRevision() {
    return effectiveRevision;
  }

  public Phase phase() {
    return phase;
  }

  public Phase failedPhase() {
    return failedPhase;
  }

  public ClientException.Kind failureKind() {
    return failureKind;
  }

  public long elapsedNanos() {
    return elapsedNanos;
  }

  public Instant observedAt() {
    return observedAt;
  }
}
