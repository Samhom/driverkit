package io.github.samhom.driverkit.rocketmq.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void legacyNativeErrorsKeepTheSameGovernanceSemantics() {
    RocketMq4ProducerAdapter adapter = new RocketMq4ProducerAdapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.AUTHORIZATION,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(16, "fixture"), write));
    assertEquals(
        ClientException.Kind.OVERLOADED,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(2, "fixture"), read));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(10, "fixture"), write));
  }
}
