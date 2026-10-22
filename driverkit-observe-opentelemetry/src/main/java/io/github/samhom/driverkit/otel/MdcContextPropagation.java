package io.github.samhom.driverkit.otel;

import io.github.samhom.driverkit.core.*;
import java.util.*;
import org.slf4j.MDC;
import org.slf4j.spi.MDCAdapter;

/** Only explicitly selected keys cross threads; the application's logging provider owns MDC. */
public final class MdcContextPropagation implements ContextPropagation {
  private final Set<String> keys;
  private final MDCAdapter adapter;

  public MdcContextPropagation(Set<String> keys) {
    this(keys, MDC.getMDCAdapter());
  }

  public MdcContextPropagation(Set<String> keys, MDCAdapter adapter) {
    this.adapter = Objects.requireNonNull(adapter);
    if (keys.size() > 32) throw new IllegalArgumentException("Too many MDC fields");
    this.keys = Collections.unmodifiableSet(new HashSet<>(keys));
    for (String key : this.keys) Limits.name(key);
  }

  @Override
  public Snapshot capture() {
    Map<String, String> selected = new HashMap<>();
    for (String key : keys) {
      String value = adapter.get(key);
      if (value != null) {
        if (value.length() > 512 || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0)
          throw new IllegalArgumentException("Invalid MDC field");
        selected.put(key, value);
      }
    }
    return () -> {
      Map<String, String> previous = adapter.getCopyOfContextMap();
      adapter.setContextMap(selected);
      return () -> {
        if (previous == null) adapter.clear();
        else adapter.setContextMap(previous);
      };
    };
  }
}
