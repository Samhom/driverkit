package io.github.samhom.driverkit.redis;

import io.github.samhom.driverkit.core.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.SetParams;

public final class ManagedRedis {
  private final ManagedClient<RedisClient, RedisSettings> client;
  private final Duration timeout;
  private final int maxValueBytes;

  public ManagedRedis(
      ManagedClient<RedisClient, RedisSettings> client, Duration timeout, int maxValueBytes) {
    this.client = java.util.Objects.requireNonNull(client);
    this.timeout = Limits.timeout(timeout);
    this.maxValueBytes = Limits.count(maxValueBytes, 16 * 1024 * 1024);
  }

  private static void key(String key) {
    if (key == null || key.getBytes(StandardCharsets.UTF_8).length > 4096)
      throw new IllegalArgumentException("Invalid Redis key size");
  }

  public String get(String key) {
    key(key);
    return client.execute(
        Operation.read("get", timeout), (nativeClient, context) -> nativeClient.get(key));
  }

  public void set(String key, String value, Duration ttl) {
    key(key);
    Limits.bytes(value == null ? null : value.getBytes(StandardCharsets.UTF_8), maxValueBytes);
    if (ttl == null
        || ttl.compareTo(Duration.ofMillis(1)) < 0
        || ttl.compareTo(Duration.ofDays(3650)) > 0)
      throw new IllegalArgumentException("TTL out of bounds");
    client.execute(
        Operation.write("set", timeout),
        (nativeClient, context) -> {
          Limits.bytes(
              value.getBytes(StandardCharsets.UTF_8),
              Math.min(maxValueBytes, context.settings(RedisSettings.class).maxValueBytes()));
          return nativeClient.set(key, value, SetParams.setParams().px(ttl.toMillis()));
        });
  }

  public long increment(String key) {
    key(key);
    return client.execute(
        Operation.write("incr", timeout), (nativeClient, context) -> nativeClient.incr(key));
  }

  public long delete(String key) {
    key(key);
    return client.execute(
        Operation.write("del", timeout), (nativeClient, context) -> nativeClient.del(key));
  }

  public ManagedClient<RedisClient, RedisSettings> handle() {
    return client;
  }
}
