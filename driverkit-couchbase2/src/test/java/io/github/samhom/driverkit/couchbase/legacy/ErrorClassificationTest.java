package io.github.samhom.driverkit.couchbase.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void legacyNativeErrorsKeepTheSameGovernanceSemantics() {
    Couchbase2Adapter adapter = new Couchbase2Adapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.AUTHENTICATION,
        adapter.classify(new com.couchbase.client.java.error.AuthenticationException(), read));
    assertEquals(
        ClientException.Kind.NOT_FOUND,
        adapter.classify(
            new com.couchbase.client.java.error.DocumentDoesNotExistException(), read));
    assertEquals(
        ClientException.Kind.CONFLICT,
        adapter.classify(
            new com.couchbase.client.java.error.DocumentAlreadyExistsException(), write));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(new java.util.concurrent.TimeoutException(), write));
  }
}
