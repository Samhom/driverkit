package io.github.samhom.driverkit.redis;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.Set;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

/** Retains RESP2 reply shapes without requiring HELLO support on older Redis servers. */
public final class JedisAdapter implements DriverAdapter<RedisClient, RedisSettings> {
  @Override
  public String id() {
    return "redis-jedis8";
  }

  @Override
  public void validate(RedisSettings settings) {
    java.util.Objects.requireNonNull(settings);
  }

  @Override
  public void validateUpdate(RedisSettings previous, RedisSettings next) {
    if (previous.database() != next.database())
      throw new IllegalArgumentException("Database migration requires a new resource binding");
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    if (error instanceof redis.clients.jedis.exceptions.JedisConnectionException)
      return operation.replaySafe()
          ? ClientException.Kind.UNAVAILABLE
          : ClientException.Kind.WRITE_OUTCOME_UNKNOWN;
    if (error instanceof redis.clients.jedis.exceptions.JedisDataException)
      return ClientException.Kind.INVALID_REQUEST;
    return ClientException.Kind.DRIVER_FAILURE;
  }

  @Override
  public RedisClient create(RedisSettings settings) {
    GenericObjectPoolConfig<Connection> pool = new GenericObjectPoolConfig<>();
    pool.setMaxTotal(settings.maxConnections());
    pool.setMaxIdle(settings.maxConnections());
    pool.setMinIdle(0);
    pool.setBlockWhenExhausted(true);
    pool.setMaxWait(settings.connectTimeout());
    try (Credentials credentials = settings.credentials().resolve()) {
      DefaultJedisClientConfig config =
          DefaultJedisClientConfig.builder()
              .serverDefaultProtocol()
              .connectionTimeoutMillis(Math.toIntExact(settings.connectTimeout().toMillis()))
              .socketTimeoutMillis(Math.toIntExact(settings.readTimeout().toMillis()))
              .ssl(settings.endpoint().tls())
              .user(credentials.username().isEmpty() ? null : credentials.username())
              .password(credentials.passwordText())
              .database(settings.database())
              .build();
      return RedisClient.builder()
          .hostAndPort(settings.endpoint().host(), settings.endpoint().port())
          .clientConfig(config)
          .poolConfig(pool)
          .build();
    }
  }

  @Override
  public void warmup(RedisClient client, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    try (Connection connection = client.getPool().getResource()) {
      int previous = connection.getSoTimeout();
      int remaining =
          (int)
              Math.max(
                  1,
                  Math.min(
                      Integer.MAX_VALUE,
                      (Math.max(1, deadline - System.nanoTime()) + 999999) / 1000000));
      connection.setSoTimeout(previous <= 0 ? remaining : Math.min(previous, remaining));
      try {
        connection.sendCommand(Protocol.Command.PING);
        if (!"PONG".equals(connection.getStatusCodeReply()))
          throw new IllegalStateException("Redis readiness check failed");
      } finally {
        connection.setSoTimeout(previous);
      }
    }
  }

  @Override
  public void close(RedisClient client) {
    client.close();
  }

  @Override
  public Set<String> capabilities() {
    return Set.of("KV", "PIPELINE", "TRANSACTION", "NATIVE_API");
  }
}
