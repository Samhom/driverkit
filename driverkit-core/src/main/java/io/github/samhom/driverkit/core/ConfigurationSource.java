package io.github.samhom.driverkit.core;

import java.util.function.Consumer;

/** Each subscription MUST replay its current snapshot without a subscribe/read race. */
public interface ConfigurationSource<S> {
  AutoCloseable subscribe(Consumer<ConfigSnapshot<S>> listener);
}
