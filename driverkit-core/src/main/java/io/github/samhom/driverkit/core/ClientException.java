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
    RATE_LIMITED,
    CONNECTION_FAILURE,
    TLS_FAILURE,
    SERVER_REJECTED,
    UNAVAILABLE
  }

  public enum RetryAdvice {
    DO_NOT_RETRY,
    RETRY_READ_WITH_BACKOFF,
    RECONCILE_WRITE
  }

  /** Advice only: the SDK never starts another attempt. Reads must truly be replay safe. */
  public RetryAdvice retryAdvice(Operation operation) {
    java.util.Objects.requireNonNull(operation);
    if (kind == Kind.WRITE_OUTCOME_UNKNOWN) return RetryAdvice.RECONCILE_WRITE;
    if (!operation.replaySafe()) return RetryAdvice.DO_NOT_RETRY;
    switch (kind) {
      case RATE_LIMITED:
      case OVERLOADED:
      case TIMEOUT:
      case CONNECTION_FAILURE:
      case UNAVAILABLE:
      case SERVER_REJECTED:
        return RetryAdvice.RETRY_READ_WITH_BACKOFF;
      default:
        return RetryAdvice.DO_NOT_RETRY;
    }
  }

  private final Kind kind;

  public ClientException(Kind kind, String message) {
    super(message);
    this.kind = java.util.Objects.requireNonNull(kind);
  }

  public ClientException(Kind kind, String message, Throwable cause) {
    super(message, cause);
    this.kind = java.util.Objects.requireNonNull(kind);
  }

  public Kind kind() {
    return kind;
  }
}
