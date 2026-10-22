package io.github.samhom.driverkit.elasticsearch.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void legacyNativeErrorsKeepTheSameGovernanceSemantics() {
    Elasticsearch7Adapter adapter = new Elasticsearch7Adapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.RATE_LIMITED,
        adapter.classify(
            new org.elasticsearch.ElasticsearchStatusException(
                "fixture", org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS),
            read));
    assertEquals(
        ClientException.Kind.AUTHORIZATION,
        adapter.classify(
            new org.elasticsearch.ElasticsearchStatusException(
                "fixture", org.elasticsearch.rest.RestStatus.FORBIDDEN),
            write));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(
            new org.elasticsearch.ElasticsearchStatusException(
                "fixture", org.elasticsearch.rest.RestStatus.SERVICE_UNAVAILABLE),
            write));
  }
}
