package io.github.samhom.driverkit.redis;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
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
      String username = credentials.username().isEmpty() ? null : credentials.username();
      String password = credentials.passwordText();
      java.util.function.Supplier<DefaultJedisClientConfig> config =
          () -> {
            javax.net.ssl.SSLParameters ssl = new javax.net.ssl.SSLParameters();
            ssl.setEndpointIdentificationAlgorithm("HTTPS");
            return DefaultJedisClientConfig.builder()
                .serverDefaultProtocol()
                .connectionTimeoutMillis(RedisCallBudget.cap(settings.connectTimeout()))
                .socketTimeoutMillis(RedisCallBudget.cap(settings.readTimeout()))
                .ssl(settings.endpoint().tls())
                .sslParameters(ssl)
                .user(username)
                .password(password)
                .database(settings.database())
                .build();
          };
      HostAndPort host = new HostAndPort(settings.endpoint().host(), settings.endpoint().port());
      ConnectionFactory factory =
          new ConnectionFactory(host, config.get()) {
            @Override
            public org.apache.commons.pool2.PooledObject<Connection> makeObject() {
              return new org.apache.commons.pool2.impl.DefaultPooledObject<>(
                  new Connection(host, config.get()));
            }
          };
      redis.clients.jedis.providers.PooledConnectionProvider provider =
          new redis.clients.jedis.providers.PooledConnectionProvider(factory, pool);
      try {
        return RedisClient.builder()
            .hostAndPort(host)
            .clientConfig(config.get())
            .connectionProvider(provider)
            .build();
      } catch (RuntimeException | Error failure) {
        provider.close();
        throw failure;
      }
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
  public AdapterCapabilities capabilities() {
    return AdapterCapabilities.nativeApi(false);
  }
}
