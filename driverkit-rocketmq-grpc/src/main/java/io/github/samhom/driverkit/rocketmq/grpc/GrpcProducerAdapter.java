package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
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
    error = FailureClassifier.unwrap(error);
    if (FailureClassifier.transport(error, operation) == ClientException.Kind.TLS_FAILURE)
      return ClientException.Kind.TLS_FAILURE;
    if (error instanceof org.apache.rocketmq.client.java.exception.UnauthorizedException)
      return ClientException.Kind.AUTHENTICATION;
    if (error instanceof org.apache.rocketmq.client.java.exception.ForbiddenException)
      return ClientException.Kind.AUTHORIZATION;
    if (error instanceof org.apache.rocketmq.client.java.exception.TooManyRequestsException)
      return ClientException.Kind.RATE_LIMITED;
    if (error instanceof org.apache.rocketmq.client.java.exception.NotFoundException)
      return ClientException.Kind.NOT_FOUND;
    if (error instanceof org.apache.rocketmq.client.java.exception.BadRequestException
        || error instanceof org.apache.rocketmq.client.java.exception.PayloadTooLargeException
        || error instanceof org.apache.rocketmq.client.java.exception.PayloadEmptyException
        || error instanceof org.apache.rocketmq.client.java.exception.UnsupportedException)
      return ClientException.Kind.INVALID_REQUEST;
    if (error instanceof io.grpc.StatusRuntimeException
        || error instanceof io.grpc.StatusException) {
      switch (io.grpc.Status.fromThrowable(error).getCode()) {
        case UNAUTHENTICATED:
          return ClientException.Kind.AUTHENTICATION;
        case PERMISSION_DENIED:
          return ClientException.Kind.AUTHORIZATION;
        case RESOURCE_EXHAUSTED:
          return ClientException.Kind.RATE_LIMITED;
        case INVALID_ARGUMENT:
        case UNIMPLEMENTED:
          return ClientException.Kind.INVALID_REQUEST;
        case NOT_FOUND:
          return ClientException.Kind.NOT_FOUND;
        case DEADLINE_EXCEEDED:
          return FailureClassifier.uncertain(operation, ClientException.Kind.TIMEOUT);
        default:
          return FailureClassifier.uncertain(operation, ClientException.Kind.UNAVAILABLE);
      }
    }
    ClientException.Kind common = FailureClassifier.transport(error, operation);
    if (common != ClientException.Kind.DRIVER_FAILURE) return common;
    if (error instanceof org.apache.rocketmq.client.apis.ClientException)
      return FailureClassifier.uncertain(operation, ClientException.Kind.SERVER_REJECTED);
    return ClientException.Kind.DRIVER_FAILURE;
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
  public AdapterCapabilities capabilities() {
    return AdapterCapabilities.nativeApi(true);
  }

  @Override
  public UpdatePlan updatePlan(GrpcSettings a, GrpcSettings b) {
    return UpdatePlan.builder()
        .field("proxy", a.proxy(), b.proxy(), UpdatePlan.Effect.REBUILD)
        .field("credentials", a.credentials(), b.credentials(), UpdatePlan.Effect.REBUILD)
        .field("topic", a.topic(), b.topic(), UpdatePlan.Effect.FORBIDDEN)
        .field("requestTimeout", a.requestTimeout(), b.requestTimeout(), UpdatePlan.Effect.REBUILD)
        .field(
            "maxMessageBytes", a.maxMessageBytes(), b.maxMessageBytes(), UpdatePlan.Effect.REBUILD)
        .build();
  }

  @Override
  public java.time.Duration operationTimeout(GrpcSettings settings, Operation operation) {
    return settings.requestTimeout();
  }

  @Override
  public CredentialsProvider credentials(GrpcSettings settings) {
    return settings.credentials();
  }
}
