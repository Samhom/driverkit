package io.github.samhom.driverkit.rocketmq;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.UUID;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

public final class RocketMqProducerAdapter
    implements DriverAdapter<DefaultMQProducer, RocketMqSettings> {
  @Override
  public String id() {
    return "rocketmq-remoting5";
  }

  @Override
  public void validate(RocketMqSettings settings) {
    RemotingAuthentication.validateTransport(settings);
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    return RemotingFailures.classify(error, operation);
  }

  @Override
  public void validateUpdate(RocketMqSettings previous, RocketMqSettings next) {
    RemotingAuthentication.validateUpdate(previous, next);
  }

  @Override
  public DefaultMQProducer create(RocketMqSettings settings) {
    validate(settings);
    DefaultMQProducer producer =
        new DefaultMQProducer(
            settings.group(), RemotingAuthentication.hook(settings.credentials()));
    producer.setNamesrvAddr(settings.nameServer().authority());
    producer.setUseTLS(settings.nameServer().tls());
    producer.setInstanceName("driverkit-" + UUID.randomUUID());
    producer.setSendMsgTimeout(Math.toIntExact(settings.requestTimeout().toMillis()));
    producer.setMqClientApiTimeout(Math.toIntExact(settings.requestTimeout().toMillis()));
    producer.setRetryTimesWhenSendFailed(0);
    producer.setRetryTimesWhenSendAsyncFailed(0);
    producer.setRetryAnotherBrokerWhenNotStoreOK(false);
    producer.setMaxMessageSize(settings.maxMessageBytes());
    producer.setClientCallbackExecutorThreads(2);
    producer.setEnableBackpressureForAsyncMode(true);
    producer.setBackPressureForAsyncSendNum(1024);
    producer.setBackPressureForAsyncSendSize(16 * 1024 * 1024);
    return producer;
  }

  @Override
  public void warmup(DefaultMQProducer client, Duration timeout) throws Exception {
    client.start();
  }

  @Override
  public void close(DefaultMQProducer client) {
    client.shutdown();
  }

  @Override
  public AdapterCapabilities capabilities() {
    return AdapterCapabilities.nativeApi(true);
  }
}
