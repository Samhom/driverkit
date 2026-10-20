package io.github.samhom.driverkit.micrometer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MicrometerObserverTest {
  private static final ResourceId RESOURCE = new ResourceId("example", "test", "cache");

  @Test
  void recordsOneLogicalResultWithoutChangingInjectedRegistry() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try {
      Observation observation =
          new MicrometerObserver(registry)
              .start(RESOURCE, "redis", Operation.read("get", Duration.ofSeconds(1)), 1);
      observation.finish(Observation.Outcome.SUCCESS, 1000000);
      observation.finish(Observation.Outcome.ERROR, 2000000);
      assertEquals(
          1,
          registry.get("driverkit.client.operations").tag("outcome", "success").counter().count());
      assertEquals(1, registry.get("driverkit.client.operation.duration").timer().count());
      assertEquals(2, registry.getMeters().size());
    } finally {
      registry.close();
    }
  }

  @Test
  void capsMetricCardinalityWithASingleOverflowIdentity() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try {
      MicrometerObserver observer = new MicrometerObserver(registry, 1);
      for (int i = 0; i < 100; i++)
        observer
            .start(RESOURCE, "redis", Operation.read("operation" + i, Duration.ofSeconds(1)), 1)
            .finish(Observation.Outcome.SUCCESS, 1);
      assertEquals(4, registry.getMeters().size());
      assertEquals(
          99,
          registry
              .get("driverkit.client.operations")
              .tag("resource", "overflow")
              .counter()
              .count());
    } finally {
      registry.close();
    }
  }
}
