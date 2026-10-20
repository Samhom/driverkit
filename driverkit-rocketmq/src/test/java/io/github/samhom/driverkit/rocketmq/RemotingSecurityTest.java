package io.github.samhom.driverkit.rocketmq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.junit.jupiter.api.Test;

class RemotingSecurityTest {
  private RocketMqSettings tlsSettings() {
    return new RocketMqSettings(
        new Endpoint("broker.example.org", 9876, true),
        () -> {
          throw new AssertionError(
              "Unsafe transport must be rejected before resolving credentials");
        },
        "example-producer",
        "example-topic",
        Duration.ofSeconds(1),
        1,
        1024);
  }

  @Test
  void producerFailsBeforeCredentialsOrNetworkForUnverifiedTls() {
    assertThrows(
        IllegalArgumentException.class, () -> new RocketMqProducerAdapter().create(tlsSettings()));
  }

  @Test
  void consumerFailsBeforeCredentialsOrNetworkForUnverifiedTls() {
    RocketMqConsumerAdapter adapter =
        new RocketMqConsumerAdapter(
            (messages, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
    assertThrows(IllegalArgumentException.class, () -> adapter.start(tlsSettings(), null));
  }
}
