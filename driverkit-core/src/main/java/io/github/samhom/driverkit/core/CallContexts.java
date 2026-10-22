package io.github.samhom.driverkit.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Server-supplied metadata only. This propagation facility never authenticates or authorizes. */
public final class CallContexts {
  private CallContexts() {}

  private static final ThreadLocal<Frame> CURRENT = new ThreadLocal<>();

  private static final class Frame {
    final Map<String, String> values;
    final Frame previous;

    Frame(Map<String, String> values, Frame previous) {
      this.values = values;
      this.previous = previous;
    }
  }

  public static Map<String, String> current() {
    Frame frame = CURRENT.get();
    return frame == null ? Collections.emptyMap() : frame.values;
  }

  public static ContextPropagation.Scope open(Map<String, String> trustedMetadata) {
    Objects.requireNonNull(trustedMetadata);
    if (trustedMetadata.size() > 32) throw new IllegalArgumentException("Too many context fields");
    Map<String, String> copy = new LinkedHashMap<>();
    trustedMetadata.forEach(
        (key, value) -> {
          if (key == null
              || !key.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}")
              || value == null
              || value.length() > 512
              || value.indexOf('\r') >= 0
              || value.indexOf('\n') >= 0)
            throw new IllegalArgumentException("Invalid context field");
          copy.put(key, value);
        });
    return enter(Collections.unmodifiableMap(copy));
  }

  private static ContextPropagation.Scope enter(Map<String, String> values) {
    Thread owner = Thread.currentThread();
    Frame frame = new Frame(values, CURRENT.get());
    CURRENT.set(frame);
    return new ContextPropagation.Scope() {
      boolean closed;

      public void close() {
        if (Thread.currentThread() != owner)
          throw new IllegalStateException("Context scope belongs to another thread");
        if (closed) return;
        if (CURRENT.get() != frame)
          throw new IllegalStateException("Context scopes must close in reverse order");
        closed = true;
        if (frame.previous == null) CURRENT.remove();
        else CURRENT.set(frame.previous);
      }
    };
  }

  public static ContextPropagation.Snapshot capture() {
    Map<String, String> values = current();
    return () -> enter(values);
  }
}
