package io.github.samhom.driverkit.redis.legacy;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.redis.RedisSettings;
import java.time.Duration;
import javax.net.ssl.SSLParameters;
import redis.clients.jedis.*;

/** Jedis 3 native pool; pin a session and borrow/close Jedis inside the call or session scope. */
public final class Jedis3Adapter implements DriverAdapter<JedisPool, RedisSettings> {
  @Override
  public String id() {
    return "redis-jedis3";
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
  public JedisPool create(RedisSettings settings) {
    validate(settings);
    JedisPoolConfig pool = new JedisPoolConfig();
    pool.setMaxTotal(settings.maxConnections());
    pool.setMaxIdle(settings.maxConnections());
    pool.setMinIdle(0);
    pool.setMaxWaitMillis(settings.connectTimeout().toMillis());
    pool.setBlockWhenExhausted(true);
    SSLParameters parameters = new SSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    try (Credentials credentials = settings.credentials().resolve()) {
      return new JedisPool(
          pool,
          settings.endpoint().host(),
          settings.endpoint().port(),
          Math.toIntExact(settings.connectTimeout().toMillis()),
          Math.toIntExact(settings.readTimeout().toMillis()),
          credentials.username().isEmpty() ? null : credentials.username(),
          credentials.passwordText(),
          settings.database(),
          null,
          settings.endpoint().tls(),
          null,
          parameters,
          null);
    }
  }

  @Override
  public void warmup(JedisPool pool, Duration remaining) {
    try (Jedis client = pool.getResource()) {
      int old = client.getClient().getSoTimeout();
      client
          .getClient()
          .setSoTimeout(
              (int)
                  Math.max(1, Math.min(old <= 0 ? Integer.MAX_VALUE : old, remaining.toMillis())));
      try {
        if (!"PONG".equals(client.ping()))
          throw new IllegalStateException("Redis readiness failed");
      } finally {
        client.getClient().setSoTimeout(old);
      }
    }
  }

  @Override
  public void close(JedisPool pool) {
    pool.close();
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    error = FailureClassifier.unwrap(error);
    if (error instanceof redis.clients.jedis.exceptions.JedisAccessControlException) {
      String message = error.getMessage();
      if (message != null && (message.startsWith("WRONGPASS ") || message.startsWith("NOAUTH ")))
        return ClientException.Kind.AUTHENTICATION;
      return ClientException.Kind.AUTHORIZATION;
    }
    if (error instanceof redis.clients.jedis.exceptions.JedisBusyException)
      return ClientException.Kind.SERVER_REJECTED;
    if (error instanceof redis.clients.jedis.exceptions.JedisDataException)
      return ClientException.Kind.INVALID_REQUEST;
    ClientException.Kind common = FailureClassifier.transport(error, operation);
    if (common != ClientException.Kind.DRIVER_FAILURE) return common;
    if (error instanceof redis.clients.jedis.exceptions.JedisConnectionException)
      return FailureClassifier.uncertain(operation, ClientException.Kind.UNAVAILABLE);
    return ClientException.Kind.DRIVER_FAILURE;
  }

  @Override
  public UpdatePlan updatePlan(RedisSettings previous, RedisSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }

  @Override
  public java.time.Duration operationTimeout(RedisSettings settings, Operation operation) {
    return settings.readTimeout();
  }
}
