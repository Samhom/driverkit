package io.github.samhom.driverkit.core;

public final class ClientStatus {
  private final long revision;
  private final int inFlight;
  private final int retiring;
  private final boolean closed;
  private final boolean revoked;
  private final long attemptedRevision;
  private final long rejectedUpdates;

  ClientStatus(
      long revision,
      int inFlight,
      int retiring,
      boolean closed,
      boolean revoked,
      long attemptedRevision,
      long rejectedUpdates) {
    this.revision = revision;
    this.inFlight = inFlight;
    this.retiring = retiring;
    this.closed = closed;
    this.revoked = revoked;
    this.attemptedRevision = attemptedRevision;
    this.rejectedUpdates = rejectedUpdates;
  }

  public long revision() {
    return revision;
  }

  public int inFlight() {
    return inFlight;
  }

  public int retiringGenerations() {
    return retiring;
  }

  public boolean closed() {
    return closed;
  }

  public boolean revoked() {
    return revoked;
  }

  public long attemptedRevision() {
    return attemptedRevision;
  }

  public long rejectedUpdates() {
    return rejectedUpdates;
  }
}
