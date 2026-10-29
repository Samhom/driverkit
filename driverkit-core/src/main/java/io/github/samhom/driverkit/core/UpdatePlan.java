package io.github.samhom.driverkit.core;

import java.util.*;

/** Value-free field effects, safe for configuration previews and audit records. */
public final class UpdatePlan {
  public enum Effect {
    LIVE,
    REBUILD,
    FORBIDDEN
  }

  private final Map<String, Effect> fields;
  private final Effect effect;

  private UpdatePlan(Map<String, Effect> fields, Effect minimum) {
    this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    Effect result = minimum;
    for (Effect value : fields.values()) if (value.ordinal() > result.ordinal()) result = value;
    effect = result;
  }

  public static UpdatePlan rebuild() {
    return new UpdatePlan(Collections.emptyMap(), Effect.REBUILD);
  }

  public static Builder builder() {
    return new Builder();
  }

  public Effect effect() {
    return effect;
  }

  public Map<String, Effect> fields() {
    return fields;
  }

  public void requireAllowed() {
    if (effect == Effect.FORBIDDEN)
      throw new IllegalArgumentException(
          "Configuration contains fields requiring a new resource binding");
  }

  public static final class Builder {
    private final Map<String, Effect> fields = new LinkedHashMap<>();

    public Builder field(String name, Object previous, Object next, Effect effect) {
      Limits.name(name);
      Objects.requireNonNull(effect);
      if (!Objects.equals(previous, next)) fields.put(name, effect);
      if (fields.size() > 128) throw new IllegalArgumentException("Too many configuration fields");
      return this;
    }

    public UpdatePlan build() {
      return new UpdatePlan(fields, fields.isEmpty() ? Effect.REBUILD : Effect.LIVE);
    }
  }
}
