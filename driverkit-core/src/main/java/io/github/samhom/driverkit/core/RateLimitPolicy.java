package io.github.samhom.driverkit.core;

import java.util.HashMap;
import java.util.Map;

/** Token buckets exist only for statically configured resources, keeping memory bounded. */
public final class RateLimitPolicy implements AdmissionPolicy {
  public static final class Limit {
    final int perSecond, burst;

    public Limit(int perSecond, int burst) {
      this.perSecond = Limits.count(perSecond, 1000000);
      this.burst = Limits.count(burst, 1000000);
    }
  }

  private static final class Bucket {
    final Limit limit;
    long last = System.nanoTime();
    double tokens;

    Bucket(Limit limit) {
      this.limit = limit;
      tokens = limit.burst;
    }

    synchronized boolean acquire() {
      long now = System.nanoTime();
      tokens =
          Math.min(
              limit.burst, tokens + Math.max(0, now - last) / 1_000_000_000d * limit.perSecond);
      last = now;
      if (tokens < 1) return false;
      tokens -= 1;
      return true;
    }
  }

  private final Map<ResourceId, Bucket> buckets = new HashMap<>();

  public RateLimitPolicy(Map<ResourceId, Limit> limits) {
    if (limits.size() > 4096) throw new IllegalArgumentException("Too many rate-limited resources");
    limits.forEach(
        (resource, limit) ->
            buckets.put(
                java.util.Objects.requireNonNull(resource),
                new Bucket(java.util.Objects.requireNonNull(limit))));
  }

  @Override
  public void check(ResourceId resource, Operation operation) {
    Bucket bucket = buckets.get(resource);
    if (bucket != null && !bucket.acquire())
      throw new ClientException(ClientException.Kind.RATE_LIMITED, "Resource rate limit exceeded");
  }
}
