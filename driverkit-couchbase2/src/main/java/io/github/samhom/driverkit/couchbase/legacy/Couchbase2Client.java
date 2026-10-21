package io.github.samhom.driverkit.couchbase.legacy;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import io.github.samhom.driverkit.core.Credentials;
import io.github.samhom.driverkit.couchbase.CouchbaseSettings;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns all three SDK 2 resources; a bucket alone cannot release its environment. */
public final class Couchbase2Client {
  private final CouchbaseSettings settings;
  private final AtomicBoolean started = new AtomicBoolean();
  private volatile CouchbaseEnvironment environment;
  private volatile CouchbaseCluster cluster;
  private volatile Bucket bucket;

  Couchbase2Client(CouchbaseSettings settings) {
    this.settings = settings;
  }

  // The core owns this wrapper BEFORE any native resource is created. A failed or late
  // initialization is cleaned through that same ownership, including partial initialization.
  void initialize(Duration budget) {
    if (!started.compareAndSet(false, true))
      throw new IllegalStateException("SDK 2 initialization already attempted");
    long deadline = System.nanoTime() + budget.toNanos();
    try (Credentials credentials = settings.credentials().resolve()) {
      long connect =
          Math.max(1, Math.min(settings.connectTimeout().toMillis(), remaining(deadline)));
      environment =
          DefaultCouchbaseEnvironment.builder()
              .ioPoolSize(2)
              .computationPoolSize(2)
              .requestBufferSize(1024)
              .socketConnectTimeout(Math.toIntExact(connect))
              .connectTimeout(connect)
              .kvTimeout(settings.operationTimeout().toMillis())
              .queryTimeout(settings.operationTimeout().toMillis())
              .build();
      cluster = CouchbaseCluster.create(environment, settings.endpoint().host());
      cluster.authenticate(credentials.username(), credentials.passwordText());
      bucket =
          cluster.openBucket(
              settings.bucket(), Math.min(connect, remaining(deadline)), TimeUnit.MILLISECONDS);
    }
  }

  public Bucket bucket() {
    return Objects.requireNonNull(bucket, "SDK 2 is not ready");
  }

  public CouchbaseCluster cluster() {
    return Objects.requireNonNull(cluster, "SDK 2 is not ready");
  }

  void close(long millis) {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(millis);
    boolean[] completed = {true, true, true};
    try {
      if (bucket != null)
        completed[0] =
            Boolean.TRUE.equals(
                bucket.close(remaining(deadline), java.util.concurrent.TimeUnit.MILLISECONDS));
    } finally {
      try {
        if (cluster != null)
          completed[1] =
              Boolean.TRUE.equals(
                  cluster.disconnect(
                      remaining(deadline), java.util.concurrent.TimeUnit.MILLISECONDS));
      } finally {
        if (environment != null) completed[2] = Boolean.TRUE.equals(environment.shutdown());
      }
    }
    if (!completed[0] || !completed[1] || !completed[2])
      throw new IllegalStateException("SDK 2 resource cleanup did not complete");
  }

  private static long remaining(long deadline) {
    return Math.max(
        1,
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
            Math.max(1, deadline - System.nanoTime())));
  }
}
