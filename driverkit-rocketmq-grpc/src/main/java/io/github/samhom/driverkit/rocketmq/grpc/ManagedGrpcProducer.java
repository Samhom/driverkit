package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.*;

public final class ManagedGrpcProducer {
  private final ManagedClient<Producer, GrpcSettings> client;
  private final Duration timeout;

  public ManagedGrpcProducer(ManagedClient<Producer, GrpcSettings> client, Duration timeout) {
    this.client = java.util.Objects.requireNonNull(client);
    this.timeout = Limits.timeout(timeout);
  }

  public CompletableFuture<SendReceipt> send(byte[] body, String businessKey) {
    Limits.bytes(body, 4 * 1024 * 1024);
    Limits.messageKey(businessKey);
    byte[] copy = body.clone();
    return client.executeAsync(
        Operation.write("send", timeout),
        (producer, context) -> {
          GrpcSettings settings = context.settings(GrpcSettings.class);
          Limits.bytes(copy, settings.maxMessageBytes());
          return producer.sendAsync(
              ClientServiceProvider.loadService()
                  .newMessageBuilder()
                  .setTopic(settings.topic())
                  .setKeys(businessKey)
                  .setBody(copy)
                  .build());
        });
  }

  public ManagedClient<Producer, GrpcSettings> handle() {
    return client;
  }
}
