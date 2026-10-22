package io.github.samhom.driverkit.otel;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.ContextPropagation;
import io.opentelemetry.context.*;
import java.util.Set;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.BasicMDCAdapter;

class PropagationTest {
  @Test
  void traceAndSelectedMdcRestoreForeignContextOnReusedThread() throws Exception {
    ContextKey<String> key = ContextKey.named("fixture-trace");
    BasicMDCAdapter mdc = new BasicMDCAdapter();
    mdc.put("request", "caller");
    mdc.put("unselected", "not-propagated");
    io.opentelemetry.api.trace.SpanContext parent =
        io.opentelemetry.api.trace.SpanContext.create(
            "00000000000000000000000000000001",
            "0000000000000001",
            io.opentelemetry.api.trace.TraceFlags.getSampled(),
            io.opentelemetry.api.trace.TraceState.getDefault());
    ContextPropagation.Snapshot captured;
    try (Scope ignored =
        Context.current()
            .with(key, "caller-trace")
            .with(io.opentelemetry.api.trace.Span.wrap(parent))
            .makeCurrent()) {
      captured =
          ContextPropagation.combine(
                  new OpenTelemetryContextPropagation(),
                  new MdcContextPropagation(Set.of("request"), mdc))
              .capture();
    }
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      worker
          .submit(
              () -> {
                mdc.put("request", "foreign");
                try (Scope ignored = Context.current().with(key, "foreign-trace").makeCurrent()) {
                  assertThrows(
                      IllegalStateException.class,
                      () ->
                          captured.run(
                              () -> {
                                assertEquals("caller-trace", Context.current().get(key));
                                assertEquals(
                                    parent,
                                    io.opentelemetry.api.trace.Span.current().getSpanContext());
                                assertEquals("caller", mdc.get("request"));
                                assertNull(mdc.get("unselected"));
                                throw new IllegalStateException("fixture");
                              }));
                  assertEquals("foreign-trace", Context.current().get(key));
                  assertEquals("foreign", mdc.get("request"));
                } finally {
                  mdc.clear();
                }
              })
          .get(3, TimeUnit.SECONDS);
      worker
          .submit(
              () -> {
                assertNull(Context.current().get(key));
                assertNull(mdc.get("request"));
              })
          .get(3, TimeUnit.SECONDS);
    } finally {
      worker.shutdownNow();
    }
  }
}
