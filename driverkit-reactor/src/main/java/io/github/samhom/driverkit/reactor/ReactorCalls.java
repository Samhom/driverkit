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

  public static <C, S, T> Mono<T> mono(
      ManagedClient<C, S> client, Operation operation, BiFunction<C, CallContext, Mono<T>> call) {
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(call, "call");
    return Mono.defer(
        () ->
            Mono.fromFuture(
                client.executeAsync(
                    operation,
                    (nativeClient, context) -> call.apply(nativeClient, context).toFuture())));
  }
}
