package io.github.samhom.driverkit.core;

import java.time.Instant;

public final class CredentialStatus {
  public enum State {
    ROTATING,
    ACTIVE,
    FAILED,
    REVOKED,
    CLOSED
  }

  private final long desiredVersion, activeVersion;
  private final Instant expiresAt;
  private final State state;
  private final ClientException.Kind failure;

  CredentialStatus(
      long desired, long active, Instant expires, State state, ClientException.Kind failure) {
    desiredVersion = desired;
    activeVersion = active;
    expiresAt = expires;
    this.state = state;
    this.failure = failure;
  }

  public long desiredVersion() {
    return desiredVersion;
  }

  public long activeVersion() {
    return activeVersion;
  }

  public Instant expiresAt() {
    return expiresAt;
  }

  public State state() {
    return state;
  }

  public ClientException.Kind failureKind() {
    return failure;
  }
}
