package io.github.samhom.driverkit.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import io.github.samhom.driverkit.core.*;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/** One atomic document per resource avoids torn reads of independently updated property keys. */
public final class ApolloConfigurationSource<S> implements ConfigurationSource<S>, AutoCloseable {
  private final Config config;
  private final String key;
  private final Function<String, ConfigSnapshot<S>> decoder;
  private final PushConfigurationSource<S> source;
  private final ConfigChangeListener listener;
  private final AtomicLong rejected = new AtomicLong();
  private volatile boolean closed;

  public ApolloConfigurationSource(
      Config config, String key, Function<String, ConfigSnapshot<S>> decoder) {
    this.config = Objects.requireNonNull(config, "config");
    if (key == null || !key.matches("[A-Za-z0-9_.-]{1,128}"))
      throw new IllegalArgumentException("Invalid configuration key");
    this.key = key;
    this.decoder = Objects.requireNonNull(decoder, "decoder");
    source = new PushConfigurationSource<>(read());
    listener =
        event -> {
          if (!closed)
            try {
              source.publish(read());
            } catch (RuntimeException ignored) {
              rejected.incrementAndGet();
            }
        };
    config.addChangeListener(listener, Collections.singleton(key));
    try {
      source.publish(read());
    } catch (RuntimeException e) {
      config.removeChangeListener(listener);
      throw e;
    }
  }

  private ConfigSnapshot<S> read() {
    String document = config.getProperty(key, null);
    if (document == null || document.length() > 65536)
      throw new IllegalArgumentException("Missing or oversized configuration document");
    return Objects.requireNonNull(decoder.apply(document), "Decoded snapshot");
  }

  public long rejectedUpdates() {
    return rejected.get();
  }

  @Override
  public synchronized AutoCloseable subscribe(Consumer<ConfigSnapshot<S>> consumer) {
    if (closed) throw new IllegalStateException("Configuration source is closed");
    return source.subscribe(consumer);
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      config.removeChangeListener(listener);
    }
  }
}
