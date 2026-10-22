package io.github.samhom.driverkit.reactor;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class ReactorCallsTest {
  @Test
  void reactorSubscriptionContextCrossesSchedulerAndWorkerBoundaries() {
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, String> client =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("reactor", "test", "context"), 1, "native"),
              new FactoryAdapter<>("fixture", s -> s, (c, t) -> {}, c -> {}),
              ClientPolicy.defaults());
      Mono<String> publisher;
      try (ContextPropagation.Scope ignored =
          CallContexts.open(java.util.Map.of("request", "captured"))) {
        publisher =
            ReactorCalls.mono(
                    client,
                    Operation.read("get", Duration.ofSeconds(2)),
                    (c, ctx) ->
                        Mono.deferContextual(
                            view -> {
                              assertEquals("value", view.get("reactor-field"));
                              return Mono.just(CallContexts.current().get("request"));
                            }))
                .contextWrite(context -> context.put("reactor-field", "value"))
                .contextWrite(ReactorCalls.context(ContextPropagation.NONE));
      }
      assertEquals(
          "captured",
          publisher
              .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
              .block(Duration.ofSeconds(3)));
      assertTrue(CallContexts.current().isEmpty());
    }
  }

  @Test
  void createsOneInvocationPerSubscriptionAndPreservesEmptyCompletion() {
    AtomicInteger subscriptions = new AtomicInteger(), observations = new AtomicInteger();
    FactoryAdapter<String, String> adapter =
        new FactoryAdapter<>(
            "example", settings -> settings, (client, timeout) -> {}, client -> {});
    try (ClientRuntime runtime =
        new ClientRuntime(
            2, 2, 4, (r, d, o, v) -> (result, elapsed) -> observations.incrementAndGet())) {
      ManagedClient<String, String> client =
          runtime.register(
              new ConfigSnapshot<>(new ResourceId("example", "test", "cache"), 1, "value"),
              adapter,
              ClientPolicy.defaults());
      Mono<String> operation =
          ReactorCalls.mono(
              client,
              Operation.read("get", Duration.ofSeconds(2)),
              (nativeClient, context) -> {
                subscriptions.incrementAndGet();
                return Mono.just(nativeClient);
              });
      assertEquals(0, subscriptions.get());
      assertEquals("value", operation.block());
      assertEquals("value", operation.block());
      assertEquals(2, subscriptions.get());
      assertNull(
          ReactorCalls.mono(
                  client,
                  Operation.read("empty", Duration.ofSeconds(2)),
                  (nativeClient, context) -> Mono.empty())
              .block());
      long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
      while (observations.get() < 3 && System.nanoTime() < deadline) Thread.yield();
      assertEquals(3, observations.get());
    }
  }
}
