package io.github.samhom.driverkit.reactor;

import io.github.samhom.driverkit.core.*;
import java.util.Objects;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;

/**
 * Cold Mono bridge: each subscription owns one invocation, timeout and physical generation lease.
 */
public final class ReactorCalls {
  private ReactorCalls() {}

  private static final Object SNAPSHOT = new Object();

  /**
   * Capture once in the request scope; Reactor Context carries it across subscription schedulers.
   */
  public static java.util.function.Function<
          reactor.util.context.Context, reactor.util.context.Context>
      context(ContextPropagation propagation) {
    ContextPropagation.Snapshot snapshot =
        ContextPropagation.combine(CallContexts::capture, propagation).capture();
    return context -> context.put(SNAPSHOT, snapshot);
  }

  public static <C, S, T> Mono<T> mono(
      ManagedClient<C, S> client, Operation operation, BiFunction<C, CallContext, Mono<T>> call) {
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(call, "call");
    return Mono.deferContextual(
        view -> {
          ContextPropagation.Snapshot snapshot =
              view.getOrDefault(SNAPSHOT, CallContexts.capture());
          try (ContextPropagation.Scope ignored = snapshot.open()) {
            return Mono.fromFuture(
                client.executeAsync(
                    operation,
                    (nativeClient, context) ->
                        call.apply(nativeClient, context).contextWrite(view).toFuture()));
          }
        });
  }
}
