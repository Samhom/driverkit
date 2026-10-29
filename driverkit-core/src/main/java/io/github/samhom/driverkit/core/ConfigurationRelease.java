package io.github.samhom.driverkit.core;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Objects;

/**
 * Authenticated deployment control supplies instance IDs. Rollout selection is not authorization.
 */
public final class ConfigurationRelease<S> {
  private final ConfigSnapshot<S> snapshot;
  private final long expectedRevision;
  private final String rollout;
  private final int percent;

  public ConfigurationRelease(
      ConfigSnapshot<S> snapshot, long expectedRevision, String rollout, int percent) {
    this.snapshot = Objects.requireNonNull(snapshot);
    this.expectedRevision = expectedRevision;
    if (expectedRevision < 1
        || snapshot.revision() <= expectedRevision
        || percent < 0
        || percent > 100) throw new IllegalArgumentException("Invalid release bounds");
    this.rollout = Limits.name(rollout);
    this.percent = percent;
  }

  public ConfigSnapshot<S> snapshot() {
    return snapshot;
  }

  public long expectedRevision() {
    return expectedRevision;
  }

  public boolean targets(String instanceId) {
    Limits.name(instanceId);
    try {
      byte[] bytes =
          MessageDigest.getInstance("SHA-256")
              .digest(
                  (rollout + "\n" + snapshot.resource() + "\n" + instanceId)
                      .getBytes(StandardCharsets.UTF_8));
      int bucket = ((bytes[0] & 255) << 8 | (bytes[1] & 255)) * 100 / 65536;
      return bucket < percent;
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
