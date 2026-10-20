package io.github.samhom.driverkit.core;

import java.util.Objects;

/** Settings must be immutable. Revisions are monotonically increasing, including rollback. */
public final class ConfigSnapshot<S> {
  private final ResourceId resource;
  private final long revision;
  private final S settings;

  public ConfigSnapshot(ResourceId resource, long revision, S settings) {
    if (revision < 1) throw new IllegalArgumentException("Revision must be positive");
    this.resource = Objects.requireNonNull(resource, "resource");
    this.revision = revision;
    this.settings = Objects.requireNonNull(settings, "settings");
  }

  public ResourceId resource() {
    return resource;
  }

  public long revision() {
    return revision;
  }

  public S settings() {
    return settings;
  }

  @Override
  public String toString() {
    return "ConfigSnapshot{resource=" + resource + ", revision=" + revision + "}";
  }
}
