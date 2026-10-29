package io.github.samhom.driverkit.rocketmq.legacy;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.rocketmq.RocketMqSettings;
import java.time.Duration;
import java.util.UUID;
import org.apache.rocketmq.acl.common.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/** Remoting 4 migration path; native 4.x TLS cannot provide DriverKit's verified transport. */
public final class RocketMq4ProducerAdapter
    implements DriverAdapter<DefaultMQProducer, RocketMqSettings> {
  @Override
  public String id() {
    return "rocketmq-remoting4";
  }

  @Override
  public void validate(RocketMqSettings settings) {
    java.util.Objects.requireNonNull(settings);
    if (settings.nameServer().tls())
      throw new IllegalArgumentException(
          "Use the verified Remoting 5 transport or an audited factory for TLS");
  }

  @Override
  public void validateUpdate(RocketMqSettings previous, RocketMqSettings next) {
    if (!previous.group().equals(next.group()) || !previous.topic().equals(next.topic()))
      throw new IllegalArgumentException("Topic/group migration requires a new binding");
  }

  @Override
  public DefaultMQProducer create(RocketMqSettings settings) {
    validate(settings);
    try (Credentials credentials = settings.credentials().resolve()) {
      DefaultMQProducer producer =
          new DefaultMQProducer(
              settings.group(),
              new AclClientRPCHook(
                  new SessionCredentials(credentials.username(), credentials.passwordText())));
      producer.setNamesrvAddr(settings.nameServer().authority());
      producer.setInstanceName("driverkit-" + UUID.randomUUID());
      producer.setUseTLS(false);
      producer.setSendMsgTimeout(Math.toIntExact(settings.requestTimeout().toMillis()));
      producer.setMaxMessageSize(settings.maxMessageBytes());
      producer.setRetryTimesWhenSendFailed(0);
      producer.setRetryTimesWhenSendAsyncFailed(0);
      producer.setRetryAnotherBrokerWhenNotStoreOK(false);
      producer.setClientCallbackExecutorThreads(2);
      return producer;
    }
  }

  @Override
  public void warmup(DefaultMQProducer producer, Duration remaining) throws Exception {
    producer.start();
  }

  @Override
  public void close(DefaultMQProducer producer) {
    producer.shutdown();
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    return RemotingFailures.classify(error, operation);
  }

  @Override
  public UpdatePlan updatePlan(RocketMqSettings previous, RocketMqSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }

  @Override
  public java.time.Duration operationTimeout(RocketMqSettings settings, Operation operation) {
    return settings.requestTimeout();
  }
}
