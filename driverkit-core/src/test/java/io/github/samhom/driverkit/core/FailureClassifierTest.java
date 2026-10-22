package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class FailureClassifierTest {
  static final Operation READ = Operation.read("get", Duration.ofSeconds(2));
  static final Operation WRITE = Operation.write("set", Duration.ofSeconds(2));

  @Test
  void retryAdviceNeverReplaysWritesOrPermanentFailures() {
    for (ClientException.Kind kind : ClientException.Kind.values()) {
      ClientException failure = new ClientException(kind, "SDK metadata");
      assertNotEquals(
          ClientException.RetryAdvice.RETRY_READ_WITH_BACKOFF, failure.retryAdvice(WRITE));
    }
    assertEquals(
        ClientException.RetryAdvice.RECONCILE_WRITE,
        new ClientException(ClientException.Kind.WRITE_OUTCOME_UNKNOWN, "SDK metadata")
            .retryAdvice(WRITE));
    for (ClientException.Kind kind :
        new ClientException.Kind[] {
          ClientException.Kind.AUTHENTICATION,
          ClientException.Kind.AUTHORIZATION,
          ClientException.Kind.TLS_FAILURE,
          ClientException.Kind.CONFLICT,
          ClientException.Kind.INVALID_REQUEST,
          ClientException.Kind.NOT_FOUND
        }) {
      assertEquals(
          ClientException.RetryAdvice.DO_NOT_RETRY,
          new ClientException(kind, "SDK metadata").retryAdvice(READ));
    }
    for (ClientException.Kind kind :
        new ClientException.Kind[] {
          ClientException.Kind.TIMEOUT,
          ClientException.Kind.CONNECTION_FAILURE,
          ClientException.Kind.RATE_LIMITED,
          ClientException.Kind.SERVER_REJECTED
        }) {
      assertEquals(
          ClientException.RetryAdvice.RETRY_READ_WITH_BACKOFF,
          new ClientException(kind, "SDK metadata").retryAdvice(READ));
    }
  }

  @Test
  void transportCausesAndCyclicWrappersAreBounded() {
    assertEquals(
        ClientException.Kind.TLS_FAILURE,
        FailureClassifier.transport(
            new java.io.IOException(new javax.net.ssl.SSLHandshakeException("fixture")), READ));
    assertEquals(
        ClientException.Kind.CONNECTION_FAILURE,
        FailureClassifier.transport(new java.net.ConnectException("fixture"), WRITE));
    assertEquals(
        ClientException.Kind.TIMEOUT,
        FailureClassifier.transport(new java.net.SocketTimeoutException(), READ));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        FailureClassifier.transport(new java.net.SocketTimeoutException(), WRITE));
    class CyclicCompletion extends CompletionException {
      CyclicCompletion() {
        super();
      }
    }
    CompletionException a = new CyclicCompletion(), b = new CyclicCompletion();
    a.initCause(b);
    b.initCause(a);
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> FailureClassifier.unwrap(a));
    assertEquals(ClientException.Kind.WRITE_OUTCOME_UNKNOWN, FailureClassifier.http(503, WRITE));
    assertEquals(ClientException.Kind.SERVER_REJECTED, FailureClassifier.http(503, READ));
    assertEquals(ClientException.Kind.RATE_LIMITED, FailureClassifier.http(429, READ));
  }

  @Test
  void nativeCauseIsPreservedWithoutCopyingItsMessageOrRetrying() {
    java.net.SocketTimeoutException nativeError =
        new java.net.SocketTimeoutException("fixture private request detail");
    java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> handle =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("errors", "test", "resource"), 1, "native"),
              new FactoryAdapter<>("fixture", s -> s, (c, t) -> {}, c -> {}),
              ClientPolicy.defaults());
      ClientException failure =
          assertThrows(
              ClientException.class,
              () ->
                  handle.executeAwait(
                      WRITE,
                      (c, ctx) -> {
                        calls.incrementAndGet();
                        CompletableFuture<String> result = new CompletableFuture<>();
                        result.completeExceptionally(new CompletionException(nativeError));
                        return result;
                      }));
      assertEquals(ClientException.Kind.WRITE_OUTCOME_UNKNOWN, failure.kind());
      assertSame(nativeError, failure.getCause());
      assertFalse(failure.getMessage().contains(nativeError.getMessage()));
      assertEquals(1, calls.get());
    }
  }
}
