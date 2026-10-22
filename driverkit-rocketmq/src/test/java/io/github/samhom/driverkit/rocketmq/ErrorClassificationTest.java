package io.github.samhom.driverkit.rocketmq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void classifiesNativeFailuresWithoutBlindWriteRetry() {
    RocketMqProducerAdapter adapter = new RocketMqProducerAdapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.AUTHORIZATION,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(16, "fixture"), write));
    assertEquals(
        ClientException.Kind.AUTHENTICATION,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(3001, "fixture"), write));
    assertEquals(
        ClientException.Kind.RATE_LIMITED,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(215, "fixture"), write));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(
            new org.apache.rocketmq.client.exception.MQBrokerException(10, "fixture"), write));
    assertEquals(
        ClientException.Kind.INVALID_REQUEST,
        adapter.classify(new IllegalArgumentException("fixture"), write));
  }
}
