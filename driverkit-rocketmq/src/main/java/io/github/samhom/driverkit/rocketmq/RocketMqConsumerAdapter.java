package io.github.samhom.driverkit.rocketmq;

import io.github.samhom.driverkit.core.*;
import java.util.Objects;
import java.util.UUID;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;

/** The native SDK owns broker retry, acknowledgment and offset persistence. */
public final class RocketMqConsumerAdapter
    implements ConsumerAdapter<DefaultMQPushConsumer, RocketMqSettings> {
  private final MessageListenerConcurrently handler;

  public RocketMqConsumerAdapter(MessageListenerConcurrently handler) {
    this.handler = Objects.requireNonNull(handler);
  }

  @Override
  public String id() {
    return "rocketmq-remoting5-consumer";
  }

  @Override
  public void validate(RocketMqSettings settings) {
    RemotingAuthentication.validateTransport(settings);
  }

  @Override
  public void validateUpdate(RocketMqSettings previous, RocketMqSettings next) {
    RemotingAuthentication.validateUpdate(previous, next);
  }

  @Override
  public DefaultMQPushConsumer start(RocketMqSettings settings, DeliveryGuard guard)
      throws Exception {
    validate(settings);
    DefaultMQPushConsumer consumer =
        new DefaultMQPushConsumer(
            settings.group(), RemotingAuthentication.hook(settings.credentials()));
    consumer.setNamesrvAddr(settings.nameServer().authority());
    consumer.setUseTLS(settings.nameServer().tls());
    consumer.setInstanceName("driverkit-" + UUID.randomUUID());
    consumer.setMqClientApiTimeout(Math.toIntExact(settings.requestTimeout().toMillis()));
    consumer.setConsumeThreadMin(settings.consumerThreads());
    consumer.setConsumeThreadMax(settings.consumerThreads());
    consumer.setConsumeMessageBatchMaxSize(1);
    consumer.setPullBatchSize(16);
    consumer.setPullThresholdForQueue(256);
    consumer.setPullThresholdSizeForQueue(16);
    consumer.setMaxReconsumeTimes(16);
    consumer.setAwaitTerminationMillisWhenShutdown(settings.requestTimeout().toMillis());
    consumer.subscribe(settings.topic(), "*");
    consumer.registerMessageListener(
        (MessageListenerConcurrently)
            (messages, context) ->
                guard.invoke(
                    () -> {
                      for (org.apache.rocketmq.common.message.MessageExt message : messages) {
                        if (!settings.topic().equals(message.getTopic()))
                          return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        Limits.bytes(message.getBody(), settings.maxMessageBytes());
                      }
                      return Objects.requireNonNull(handler.consumeMessage(messages, context));
                    },
                    ConsumeConcurrentlyStatus.RECONSUME_LATER));
    try {
      consumer.start();
      return consumer;
    } catch (Exception e) {
      consumer.shutdown();
      throw e;
    }
  }

  @Override
  public void pause(DefaultMQPushConsumer consumer) {
    consumer.suspend();
  }

  @Override
  public void resume(DefaultMQPushConsumer consumer) {
    consumer.resume();
  }

  @Override
  public void close(DefaultMQPushConsumer consumer) {
    consumer.shutdown();
  }

  @Override
  public void close(DefaultMQPushConsumer consumer, java.time.Duration remaining) {
    long budget = Math.max(1, (remaining.toNanos() + 999999) / 1000000);
    long configured = consumer.getAwaitTerminationMillisWhenShutdown();
    consumer.setAwaitTerminationMillisWhenShutdown(
        configured <= 0 ? budget : Math.min(configured, budget));
    consumer.shutdown();
  }

  @Override
  public UpdatePlan updatePlan(RocketMqSettings previous, RocketMqSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }
  @Override public CredentialsProvider credentials(RocketMqSettings settings) { return settings.credentials(); }

}
