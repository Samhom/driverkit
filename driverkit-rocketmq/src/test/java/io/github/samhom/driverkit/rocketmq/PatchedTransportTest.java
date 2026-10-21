package io.github.samhom.driverkit.rocketmq;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PatchedTransportTest {
  @Test
  void producerAcceptsTlsWithPatchedTransport() {
    RocketMqSettings settings =
        new RocketMqSettings(
            new Endpoint("localhost", 9876, true),
            () -> new Credentials("fixture", java.util.UUID.randomUUID().toString().toCharArray()),
            "fixture-group",
            "fixture-topic",
            Duration.ofSeconds(1),
            1,
            1024);
    var producer = new RocketMqProducerAdapter().create(settings);
    assertTrue(producer.isUseTLS());
    assertEquals(0, producer.getRetryTimesWhenSendFailed());
    assertEquals(0, producer.getRetryTimesWhenSendAsyncFailed());
  }
}
