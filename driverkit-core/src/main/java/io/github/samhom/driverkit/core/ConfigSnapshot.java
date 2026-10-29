package io.github.samhom.driverkit.core;

import java.util.Objects;

/** Settings must be immutable. Revisions are monotonically increasing, including rollback. */
public final class ConfigSnapshot<S> {
  private final ResourceId resource;
  private final long revision;
  private final S settings;
  private final SettingsSchema<S> schema;

  public ConfigSnapshot(ResourceId resource, long revision, S settings) {
    this(resource, revision, settings, SettingsSchema.inferred(settings));
  }

  public ConfigSnapshot(ResourceId resource, long revision, S settings, SettingsSchema<S> schema) {
    if (revision < 1) throw new IllegalArgumentException("Revision must be positive");
    this.resource = Objects.requireNonNull(resource, "resource");
    this.revision = revision;
    this.schema = Objects.requireNonNull(schema, "schema");
    this.settings = schema.copy(Objects.requireNonNull(settings, "settings"));
  }

  public ResourceId resource() {
    return resource;
  }

  public long revision() {
    return revision;
  }

  public S settings() {
    return schema.copy(settings);
  }

  public String schemaId() {
    return schema.id();
  }

  public int schemaVersion() {
    return schema.version();
  }

  /** Preserve normalization and defensive-copy rules when changing a revision. */
  public ConfigSnapshot<S> withRevision(long revision) {
    return new ConfigSnapshot<>(resource, revision, settings, schema);
  }

  @Override
  public String toString() {
    return "ConfigSnapshot{resource=" + resource + ", revision=" + revision + "}";
  }
}
