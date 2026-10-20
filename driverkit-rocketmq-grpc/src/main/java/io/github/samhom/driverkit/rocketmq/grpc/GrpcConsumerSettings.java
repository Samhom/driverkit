package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.Limits;
import java.util.Objects;

public record GrpcConsumerSettings(
    GrpcSettings client, String group, int threads, int maxCachedMessages, int maxCachedBytes) {
  public GrpcConsumerSettings {
    Objects.requireNonNull(client);
    Limits.name(group);
    Limits.count(threads, 256);
    Limits.count(maxCachedMessages, 100000);
    Limits.count(maxCachedBytes, 256 * 1024 * 1024);
  }
}
