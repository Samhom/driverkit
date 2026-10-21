package io.github.samhom.driverkit.couchbase.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.couchbase.CouchbaseSettings;
import org.junit.jupiter.api.Test;

class Couchbase2AdapterTest {
  @Test
  void ownsWrapperBeforeResolvingCredentialsAndCanCloseFailedInitialization() {
    var settings =
        CouchbaseSettings.defaults(
            new Endpoint("localhost", 11210, false),
            () -> {
              throw new IllegalStateException("Credential provider unavailable");
            },
            "fixture");
    var adapter = new Couchbase2Adapter();
    var client = adapter.create(settings);
    assertThrows(
        IllegalStateException.class, () -> adapter.warmup(client, java.time.Duration.ofSeconds(1)));
    assertDoesNotThrow(() -> adapter.close(client, java.time.Duration.ofSeconds(1)));
  }

  @Test
  void rejectsUnverifiedTlsBeforeCredentialsAndHasNoSdk3Classes() {
    var settings =
        CouchbaseSettings.defaults(
            new Endpoint("localhost", 11210, true),
            () -> {
              throw new AssertionError("Must reject before secrets");
            },
            "fixture");
    assertThrows(IllegalArgumentException.class, () -> new Couchbase2Adapter().create(settings));
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("com.couchbase.client.java.ClusterOptions"));
  }
}
