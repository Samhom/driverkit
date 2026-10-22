package io.github.samhom.driverkit.core;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** A declaration describes the SDK contract, never every feature present in a native library. */
public final class AdapterCapabilities {
  public enum Feature {
    NATIVE_API,
    ASYNC,
    TRANSACTION,
    STREAMING_RESULTS,
    NATIVE_CANCELLATION
  }

  public enum Support {
    UNSUPPORTED,
    NATIVE_SESSION_ONLY,
    MANAGED
  }

  private final Map<Feature, Support> features;

  private AdapterCapabilities(Map<Feature, Support> features) {
    this.features = Collections.unmodifiableMap(new EnumMap<>(features));
  }

  public static AdapterCapabilities nativeApi(boolean asynchronous) {
    EnumMap<Feature, Support> map = new EnumMap<>(Feature.class);
    map.put(Feature.NATIVE_API, Support.NATIVE_SESSION_ONLY);
    if (asynchronous) map.put(Feature.ASYNC, Support.MANAGED);
    return new AdapterCapabilities(map);
  }

  public static AdapterCapabilities none() {
    return new AdapterCapabilities(new EnumMap<Feature, Support>(Feature.class));
  }

  public Support support(Feature feature) {
    return features.getOrDefault(Objects.requireNonNull(feature), Support.UNSUPPORTED);
  }

  public void requireManaged(Feature feature) {
    if (support(feature) != Support.MANAGED)
      throw new ClientException(
          ClientException.Kind.INVALID_REQUEST,
          "Adapter has no managed contract for " + feature.name());
  }

  public Map<Feature, Support> declared() {
    return features;
  }
}
