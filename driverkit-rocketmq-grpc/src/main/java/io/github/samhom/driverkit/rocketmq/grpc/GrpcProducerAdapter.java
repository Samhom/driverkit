package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Set;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;

public final class GrpcProducerAdapter implements DriverAdapter<Producer, GrpcSettings> {
  @Override
  public String id() {
    return "rocketmq-grpc5";
  }

  @Override
  public void validate(GrpcSettings settings) {
    java.util.Objects.requireNonNull(settings);
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    return operation.replaySafe()
        ? ClientException.Kind.UNAVAILABLE
        : ClientException.Kind.WRITE_OUTCOME_UNKNOWN;
  }

  @Override
  public void validateUpdate(GrpcSettings previous, GrpcSettings next) {
    if (!previous.topic().equals(next.topic()))
      throw new IllegalArgumentException("Topic migration requires a new resource binding");
  }

  @Override
  public Producer create(GrpcSettings settings) throws Exception {
    return ClientServiceProvider.loadService()
        .newProducerBuilder()
        .setClientConfiguration(GrpcConfiguration.create(settings))
        .setTopics(settings.topic())
        .setMaxAttempts(1)
        .build();
  }

  @Override
  public void warmup(Producer producer, Duration timeout) {
    /* The native builder performs bounded startup. */
  }

  @Override
  public void close(Producer producer) throws Exception {
    producer.close();
  }

  @Override
  public Set<String> capabilities() {
    return Set.of("SEND", "ASYNC_SEND", "TRANSACTION", "NATIVE_API");
  }
}
