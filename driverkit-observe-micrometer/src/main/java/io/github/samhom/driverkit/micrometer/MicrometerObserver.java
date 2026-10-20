package io.github.samhom.driverkit.micrometer;

import io.github.samhom.driverkit.core.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Uses the application's registry; bounded identities prevent arbitrary operation names exploding
 * series.
 */
public final class MicrometerObserver implements Observer {
  private final MeterRegistry registry;
  private final int maxIdentities;
  private final Set<String> identities = new HashSet<>();

  public MicrometerObserver(MeterRegistry registry) {
    this(registry, 4096);
  }

  public MicrometerObserver(MeterRegistry registry, int maxIdentities) {
    this.registry = Objects.requireNonNull(registry, "registry");
    if (maxIdentities < 1 || maxIdentities > 100000)
      throw new IllegalArgumentException("Invalid metric identity limit");
    this.maxIdentities = maxIdentities;
  }

  @Override
  public Observation start(ResourceId resource, String driver, Operation operation, long revision) {
    String identity = resource + "/" + driver + "/" + operation.name();
    boolean admitted;
    synchronized (identities) {
      admitted = identities.contains(identity) || identities.size() < maxIdentities;
      if (admitted) identities.add(identity);
    }
    Tags tags =
        admitted
            ? Tags.of(
                "application",
                resource.application(),
                "environment",
                resource.environment(),
                "resource",
                resource.alias(),
                "driver",
                driver,
                "operation",
                operation.name())
            : Tags.of(
                "application",
                "overflow",
                "environment",
                "overflow",
                "resource",
                "overflow",
                "driver",
                "overflow",
                "operation",
                "overflow");
    AtomicBoolean finished = new AtomicBoolean();
    return (outcome, elapsed) -> {
      if (!finished.compareAndSet(false, true)) return;
      Tags result = tags.and("outcome", outcome.name().toLowerCase(java.util.Locale.ROOT));
      registry.counter("driverkit.client.operations", result).increment();
      Timer.builder("driverkit.client.operation.duration")
          .tags(result)
          .register(registry)
          .record(Math.max(0, elapsed), TimeUnit.NANOSECONDS);
    };
  }
}
