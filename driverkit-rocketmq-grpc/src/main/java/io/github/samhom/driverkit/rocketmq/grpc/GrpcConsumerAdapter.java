package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.*;
import java.util.Map;
import java.util.Objects;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.*;

public final class GrpcConsumerAdapter
    implements ConsumerAdapter<PushConsumer, GrpcConsumerSettings> {
  private final MessageListener handler;

  public GrpcConsumerAdapter(MessageListener handler) {
    this.handler = Objects.requireNonNull(handler);
  }

  @Override
  public String id() {
    return "rocketmq-grpc5-consumer";
  }

  @Override
  public void validate(GrpcConsumerSettings settings) {
    Objects.requireNonNull(settings);
  }

  @Override
  public void validateUpdate(GrpcConsumerSettings previous, GrpcConsumerSettings next) {
    if (!previous.group().equals(next.group())
        || !previous.client().topic().equals(next.client().topic()))
      throw new IllegalArgumentException("Topic/group migration requires a new resource binding");
  }

  @Override
  public PushConsumer start(GrpcConsumerSettings settings, DeliveryGuard guard) throws Exception {
    return ClientServiceProvider.loadService()
        .newPushConsumerBuilder()
        .setClientConfiguration(GrpcConfiguration.create(settings.client()))
        .setConsumerGroup(settings.group())
        .setSubscriptionExpressions(Map.of(settings.client().topic(), new FilterExpression("*")))
        .setConsumptionThreadCount(settings.threads())
        .setMaxCacheMessageCount(settings.maxCachedMessages())
        .setMaxCacheMessageSizeInBytes(settings.maxCachedBytes())
        .setMessageListener(
            message ->
                guard.invoke(
                    () -> {
                      if (!settings.client().topic().equals(message.getTopic())
                          || message.getBody().remaining() > settings.client().maxMessageBytes())
                        return ConsumeResult.FAILURE;
                      return Objects.requireNonNull(handler.consume(message));
                    },
                    ConsumeResult.FAILURE))
        .build();
  }

  @Override
  public void pause(PushConsumer consumer) {
    /* DeliveryGuard closes admission; PushConsumer has no public pause API. */
  }

  @Override
  public void resume(PushConsumer consumer) {}

  @Override
  public void close(PushConsumer consumer) throws Exception {
    consumer.close();
  }

  @Override
  public UpdatePlan updatePlan(GrpcConsumerSettings a, GrpcConsumerSettings b) {
    return UpdatePlan.builder()
        .field("proxy", a.client().proxy(), b.client().proxy(), UpdatePlan.Effect.REBUILD)
        .field(
            "credentials",
            a.client().credentials(),
            b.client().credentials(),
            UpdatePlan.Effect.REBUILD)
        .field("topic", a.client().topic(), b.client().topic(), UpdatePlan.Effect.FORBIDDEN)
        .field(
            "requestTimeout",
            a.client().requestTimeout(),
            b.client().requestTimeout(),
            UpdatePlan.Effect.REBUILD)
        .field(
            "maxMessageBytes",
            a.client().maxMessageBytes(),
            b.client().maxMessageBytes(),
            UpdatePlan.Effect.REBUILD)
        .field("group", a.group(), b.group(), UpdatePlan.Effect.FORBIDDEN)
        .field("threads", a.threads(), b.threads(), UpdatePlan.Effect.REBUILD)
        .field(
            "maxCachedMessages",
            a.maxCachedMessages(),
            b.maxCachedMessages(),
            UpdatePlan.Effect.REBUILD)
        .field("maxCachedBytes", a.maxCachedBytes(), b.maxCachedBytes(), UpdatePlan.Effect.REBUILD)
        .build();
  }
  @Override public CredentialsProvider credentials(GrpcConsumerSettings settings) { return settings.client().credentials(); }

}
