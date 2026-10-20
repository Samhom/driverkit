package io.github.samhom.driverkit.core;

/** Messages contain SDK metadata only; the native cause remains available to application code. */
public final class ClientException extends RuntimeException {
  public enum Kind {
    CLOSED,
    REVOKED,
    OVERLOADED,
    TIMEOUT,
    UPDATE_REJECTED,
    DRIVER_FAILURE,
    WRITE_OUTCOME_UNKNOWN,
    AUTHENTICATION,
    AUTHORIZATION,
    NOT_FOUND,
    CONFLICT,
    INVALID_REQUEST,
    UNAVAILABLE
  }

  private final Kind kind;

  public ClientException(Kind kind, String message) {
    super(message);
    this.kind = kind;
  }

  public ClientException(Kind kind, String message, Throwable cause) {
    super(message, cause);
    this.kind = kind;
  }

  public Kind kind() {
    return kind;
  }
}
