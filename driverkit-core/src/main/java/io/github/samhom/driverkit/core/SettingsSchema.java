package io.github.samhom.driverkit.core;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Explicit type/version/normalization boundary; no reflection, serialization or dynamic loading.
 */
public final class SettingsSchema<S> {
  private final String id;
  private final int version;
  private final Class<S> type;
  private final UnaryOperator<S> copier;
  private final Consumer<S> validator;
  private final boolean independentCopy;

  private SettingsSchema(
      String id,
      int version,
      Class<S> type,
      UnaryOperator<S> copier,
      Consumer<S> validator,
      boolean independentCopy) {
    this.id = Limits.name(id);
    this.version = Limits.count(version, 1000000);
    this.type = Objects.requireNonNull(type);
    this.copier = Objects.requireNonNull(copier);
    this.validator = Objects.requireNonNull(validator);
    this.independentCopy = independentCopy;
  }

  /** Copier must deep-copy every mutable field, and must be safe for concurrent reads. */
  public static <S> SettingsSchema<S> copying(
      String id, int version, Class<S> type, UnaryOperator<S> copier, Consumer<S> validator) {
    return new SettingsSchema<>(id, version, type, copier, validator, true);
  }

  public static <S extends ImmutableSettings> SettingsSchema<S> immutable(
      String id, int version, Class<S> type, Consumer<S> validator) {
    return new SettingsSchema<>(id, version, type, value -> value, validator, false);
  }

  @SuppressWarnings("unchecked")
  static <S> SettingsSchema<S> inferred(S value) {
    Objects.requireNonNull(value, "settings");
    Class<?> type = value.getClass();
    if (!(value instanceof ImmutableSettings)
        && type != String.class
        && type != Boolean.class
        && type != Byte.class
        && type != Short.class
        && type != Integer.class
        && type != Long.class
        && type != Float.class
        && type != Double.class
        && type != Character.class
        && type != java.time.Duration.class
        && type != java.time.Instant.class
        && type != java.util.UUID.class)
      throw new IllegalArgumentException(
          "Custom settings require an explicit copying schema or ImmutableSettings contract");
    return new SettingsSchema<>("immutable", 1, (Class<S>) type, item -> item, item -> {}, false);
  }

  S copy(S source) {
    S result =
        type.cast(Objects.requireNonNull(copier.apply(type.cast(source)), "Normalized settings"));
    if (independentCopy && result == source)
      throw new IllegalArgumentException("Settings copier must return an independent value");
    validator.accept(result);
    return result;
  }

  public String id() {
    return id;
  }

  public int version() {
    return version;
  }
}
