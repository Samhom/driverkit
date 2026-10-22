package io.github.samhom.driverkit.rocketmq.grpc;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void classifiesNativeFailuresWithoutBlindWriteRetry() {
    GrpcProducerAdapter adapter = new GrpcProducerAdapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.AUTHENTICATION,
        adapter.classify(io.grpc.Status.UNAUTHENTICATED.asRuntimeException(), write));
    assertEquals(
        ClientException.Kind.AUTHORIZATION,
        adapter.classify(io.grpc.Status.PERMISSION_DENIED.asException(), write));
    assertEquals(
        ClientException.Kind.RATE_LIMITED,
        adapter.classify(
            new org.apache.rocketmq.client.java.exception.TooManyRequestsException(
                429, "fixture", "request"),
            read));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException(), write));
    assertEquals(
        ClientException.Kind.TLS_FAILURE,
        adapter.classify(
            io.grpc.Status.UNAVAILABLE
                .withCause(new javax.net.ssl.SSLHandshakeException("fixture"))
                .asRuntimeException(),
            read));
  }
}
