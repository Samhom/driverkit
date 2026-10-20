package io.github.samhom.driverkit.rocketmq;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;

public final class ManagedProducer {
  private final ManagedClient<DefaultMQProducer, RocketMqSettings> client;
  private final Duration timeout;

  public ManagedProducer(
      ManagedClient<DefaultMQProducer, RocketMqSettings> client, Duration timeout) {
    this.client = java.util.Objects.requireNonNull(client);
    this.timeout = Limits.timeout(timeout);
  }

  public CompletableFuture<SendResult> send(byte[] body, String businessKey) {
    Limits.bytes(body, 4 * 1024 * 1024);
    Limits.messageKey(businessKey);
    byte[] copy = body.clone();
    return client.executeAsync(
        Operation.write("send", timeout),
        (producer, context) -> {
          RocketMqSettings settings = context.settings(RocketMqSettings.class);
          Limits.bytes(copy, settings.maxMessageBytes());
          Message message = new Message(settings.topic(), copy);
          message.setKeys(businessKey);
          CompletableFuture<SendResult> completion = new CompletableFuture<>();
          producer.send(
              message,
              new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                  if (result.getSendStatus() == SendStatus.SEND_OK) completion.complete(result);
                  else
                    completion.completeExceptionally(
                        new ClientException(
                            ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
                            "Broker returned a non-final send status"));
                }

                @Override
                public void onException(Throwable error) {
                  completion.completeExceptionally(error);
                }
              },
              Math.max(1, context.remaining().toMillis()));
          return completion;
        });
  }

  public ManagedClient<DefaultMQProducer, RocketMqSettings> handle() {
    return client;
  }
}
