package io.github.samhom.driverkit.apollo;

import io.github.samhom.driverkit.core.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.function.Function;

/**
 * The authenticated application binds scope; documents cannot change application/environment/alias.
 */
public final class PropertiesSnapshotDecoder<S> implements Function<String, ConfigSnapshot<S>> {
  private final ResourceId resource;
  private final Set<String> allowed;
  private final Function<Map<String, String>, S> settings;

  public PropertiesSnapshotDecoder(
      ResourceId resource, Set<String> allowedSettings, Function<Map<String, String>, S> settings) {
    this.resource = Objects.requireNonNull(resource, "resource");
    this.allowed = new HashSet<>(allowedSettings);
    this.allowed.add("revision");
    this.settings = Objects.requireNonNull(settings, "settings");
  }

  @Override
  public ConfigSnapshot<S> apply(String document) {
    if (document == null || document.length() > 65536)
      throw new IllegalArgumentException("Invalid configuration document size");
    Properties properties =
        new Properties() {
          @Override
          public synchronized Object put(Object key, Object value) {
            if (containsKey(key))
              throw new IllegalArgumentException("Duplicate configuration property");
            return super.put(key, value);
          }
        };
    try {
      properties.load(new StringReader(document));
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid configuration document", e);
    }
    Map<String, String> values = new LinkedHashMap<>();
    for (String key : properties.stringPropertyNames()) {
      if (!allowed.contains(key))
        throw new IllegalArgumentException("Configuration contains an unsupported property");
      values.put(key, properties.getProperty(key));
    }
    long revision;
    try {
      revision = Long.parseLong(values.remove("revision"));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid configuration revision");
    }
    return new ConfigSnapshot<>(
        resource, revision, settings.apply(Collections.unmodifiableMap(values)));
  }
}
