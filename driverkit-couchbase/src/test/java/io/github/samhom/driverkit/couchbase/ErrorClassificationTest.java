package io.github.samhom.driverkit.couchbase;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void classifiesNativeFailuresWithoutBlindWriteRetry() {
    CouchbaseAdapter adapter = new CouchbaseAdapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.TIMEOUT,
        adapter.classify(
            new com.couchbase.client.core.error.UnambiguousTimeoutException("fixture", null),
            write));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(
            new com.couchbase.client.core.error.AmbiguousTimeoutException("fixture", null), write));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(
            new com.couchbase.client.core.error.DurabilityAmbiguousException(null), write));
    assertEquals(
        ClientException.Kind.RATE_LIMITED,
        adapter.classify(new com.couchbase.client.core.error.RateLimitedException(null), read));
    assertEquals(
        ClientException.Kind.SERVER_REJECTED,
        adapter.classify(
            new com.couchbase.client.core.error.TemporaryFailureException(null), read));
  }
}
