package io.github.samhom.driverkit.rocketmq.legacy;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.rocketmq.RocketMqSettings;
import java.util.Objects;
import java.util.UUID;
import org.apache.rocketmq.acl.common.*;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;

/**
 * Retains native Remoting 4 acknowledgment and offset behavior behind the exclusive delivery gate.
 */
public final class RocketMq4ConsumerAdapter
    implements ConsumerAdapter<DefaultMQPushConsumer, RocketMqSettings> {
  private final MessageListenerConcurrently handler;

  public RocketMq4ConsumerAdapter(MessageListenerConcurrently handler) {
    this.handler = Objects.requireNonNull(handler);
  }

  @Override
  public String id() {
    return "rocketmq-remoting4-consumer";
  }

  @Override
  public void validate(RocketMqSettings settings) {
    new RocketMq4ProducerAdapter().validate(settings);
  }

  @Override
  public void validateUpdate(RocketMqSettings previous, RocketMqSettings next) {
    new RocketMq4ProducerAdapter().validateUpdate(previous, next);
  }

  @Override
  public DefaultMQPushConsumer start(RocketMqSettings settings, DeliveryGuard guard)
      throws Exception {
    validate(settings);
    Objects.requireNonNull(guard);
    DefaultMQPushConsumer consumer;
    try (Credentials credentials = settings.credentials().resolve()) {
      consumer =
          new DefaultMQPushConsumer(
              settings.group(),
              new AclClientRPCHook(
                  new SessionCredentials(credentials.username(), credentials.passwordText())),
              new org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely());
    }
    consumer.setNamesrvAddr(settings.nameServer().authority());
    consumer.setInstanceName("driverkit-" + UUID.randomUUID());
    consumer.setUseTLS(false);
    consumer.setConsumeThreadMin(settings.consumerThreads());
    consumer.setConsumeThreadMax(settings.consumerThreads());
    consumer.setConsumeMessageBatchMaxSize(1);
    consumer.setPullBatchSize(16);
    consumer.setPullThresholdForQueue(256);
    consumer.setPullThresholdSizeForQueue(16);
    consumer.setMaxReconsumeTimes(16);
    consumer.subscribe(settings.topic(), "*");
    consumer.registerMessageListener(
        (MessageListenerConcurrently)
            (messages, context) ->
                guard.invoke(
                    () -> {
                      for (var message : messages) {
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
    } catch (Exception failure) {
      consumer.shutdown();
      throw failure;
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
  public UpdatePlan updatePlan(RocketMqSettings previous, RocketMqSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }

  @Override
  public CredentialsProvider credentials(RocketMqSettings settings) {
    return settings.credentials();
  }
}
