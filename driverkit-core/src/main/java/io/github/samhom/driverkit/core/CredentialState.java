package io.github.samhom.driverkit.core;

import java.time.Instant;
import java.util.Objects;

/** Public metadata only; credential material remains behind the provider. */
public final class CredentialState implements ImmutableSettings {
  private final long version;
  private final Instant refreshAt, expiresAt;
  private final boolean revoked;

  public CredentialState(long version, Instant refreshAt, Instant expiresAt, boolean revoked) {
    if (version < 1) throw new IllegalArgumentException("Invalid credential version");
    this.version = version;
    this.refreshAt = Objects.requireNonNull(refreshAt);
    this.expiresAt = Objects.requireNonNull(expiresAt);
    if (refreshAt.isAfter(expiresAt))
      throw new IllegalArgumentException("Invalid credential lifetime");
    this.revoked = revoked;
  }

  public long version() {
    return version;
  }

  public Instant refreshAt() {
    return refreshAt;
  }

  public Instant expiresAt() {
    return expiresAt;
  }

  public boolean revoked() {
    return revoked;
  }
}
