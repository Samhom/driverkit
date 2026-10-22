package io.github.samhom.driverkit.redis;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.function.Function;
import redis.clients.jedis.*;

/** Per-borrow limits; never mutates the shared pool configuration. */
final class RedisCallBudget {
  private static final ThreadLocal<CallContext> CURRENT = new ThreadLocal<>();

  static int cap(Duration configured) {
    CallContext context = CURRENT.get();
    return context == null
        ? Math.toIntExact(configured.toMillis())
        : context.remainingMillis(configured);
  }

  static <T> T execute(RedisClient client, CallContext context, Function<Jedis, T> command)
      throws Exception {
    CallContext previous = CURRENT.get();
    CURRENT.set(context);
    redis.clients.jedis.util.Pool<Connection> pool = client.getPool();
    Connection connection = null;
    int original = 0;
    try {
      RedisSettings settings = context.settings(RedisSettings.class);
      connection =
          pool.borrowObject(Duration.ofMillis(context.remainingMillis(settings.connectTimeout())));
      original = connection.getSoTimeout();
      context.checkDeadline();
      connection.setSoTimeout(context.remainingMillis(settings.readTimeout()));
      return command.apply(new Jedis(connection));
    } finally {
      try {
        if (connection != null) {
          if (!connection.isBroken()) {
            try {
              connection.setSoTimeout(original);
            } catch (RuntimeException failure) {
              connection.setBroken();
            }
          }
          if (connection.isBroken()) pool.invalidateObject(connection);
          else pool.returnObject(connection);
        }
      } finally {
        if (previous == null) CURRENT.remove();
        else CURRENT.set(previous);
      }
    }
  }
}
