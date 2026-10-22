package io.github.samhom.driverkit.core;

/** Common bounded cause inspection. No message text is copied into SDK errors or telemetry. */
public final class FailureClassifier {
  private FailureClassifier() {}

  public static Throwable unwrap(Throwable error) {
    Throwable current = java.util.Objects.requireNonNull(error);
    java.util.Set<Throwable> visited =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
    for (int depth = 0; depth < 32 && visited.add(current); depth++) {
      if (!(current instanceof java.util.concurrent.CompletionException)
          && !(current instanceof java.util.concurrent.ExecutionException)) break;
      if (current.getCause() == null || visited.contains(current.getCause())) break;
      current = current.getCause();
    }
    return current;
  }

  public static ClientException.Kind http(int status, Operation operation) {
    switch (status) {
      case 401:
        return ClientException.Kind.AUTHENTICATION;
      case 403:
        return ClientException.Kind.AUTHORIZATION;
      case 404:
        return ClientException.Kind.NOT_FOUND;
      case 409:
        return ClientException.Kind.CONFLICT;
      case 429:
        return ClientException.Kind.RATE_LIMITED;
      case 408:
      case 504:
        return uncertain(operation, ClientException.Kind.TIMEOUT);
      default:
        if (status >= 400 && status < 500) return ClientException.Kind.INVALID_REQUEST;
        if (status >= 500) return uncertain(operation, ClientException.Kind.SERVER_REJECTED);
        return ClientException.Kind.DRIVER_FAILURE;
    }
  }

  public static ClientException.Kind uncertain(Operation operation, ClientException.Kind readKind) {
    return operation.replaySafe() ? readKind : ClientException.Kind.WRITE_OUTCOME_UNKNOWN;
  }

  public static ClientException.Kind transport(Throwable error, Operation operation) {
    Throwable current = unwrap(error);
    if (current instanceof IllegalArgumentException) return ClientException.Kind.INVALID_REQUEST;
    ClientException.Kind found = ClientException.Kind.DRIVER_FAILURE;
    java.util.Set<Throwable> visited =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
    for (int depth = 0;
        current != null && depth < 32 && visited.add(current);
        depth++, current = current.getCause()) {
      if (current instanceof javax.net.ssl.SSLException) return ClientException.Kind.TLS_FAILURE;
      if (current instanceof java.net.ConnectException
          || current instanceof java.net.UnknownHostException
          || current instanceof java.net.NoRouteToHostException)
        found = ClientException.Kind.CONNECTION_FAILURE;
      else if (current instanceof java.net.SocketTimeoutException
          || current instanceof java.util.concurrent.TimeoutException)
        found = uncertain(operation, ClientException.Kind.TIMEOUT);
      else if (current instanceof java.io.IOException
          && found == ClientException.Kind.DRIVER_FAILURE)
        found = uncertain(operation, ClientException.Kind.UNAVAILABLE);
    }
    return found;
  }
}
