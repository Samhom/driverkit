package io.github.samhom.driverkit.apollo;

import static org.junit.jupiter.api.Assertions.*;

import com.ctrip.framework.apollo.*;
import io.github.samhom.driverkit.core.*;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class ApolloConfigurationSourceTest {
  private static final ResourceId RESOURCE = new ResourceId("example", "test", "cache");

  private static PropertiesSnapshotDecoder<String> decoder() {
    return new PropertiesSnapshotDecoder<>(
        RESOURCE, Set.of("endpoint"), values -> Objects.requireNonNull(values.get("endpoint")));
  }

  @Test
  void rejectsUnknownPropertiesDuplicatesAndOversizedDocuments() {
    assertThrows(
        IllegalArgumentException.class,
        () -> decoder().apply("revision=1\napplication=other\nendpoint=local"));
    assertThrows(
        IllegalArgumentException.class,
        () -> decoder().apply("revision=1\nendpoint=one\nendpoint=two"));
    assertThrows(IllegalArgumentException.class, () -> decoder().apply("x".repeat(65537)));
    assertThrows(
        IllegalArgumentException.class, () -> decoder().apply("revision=-1\nendpoint=local"));
  }

  @Test
  void readsOneAtomicDocumentKeepsLastGoodAndUnsubscribes() {
    AtomicReference<String> document = new AtomicReference<>("revision=1\nendpoint=one");
    AtomicReference<ConfigChangeListener> listener = new AtomicReference<>();
    AtomicInteger detached = new AtomicInteger();
    Config config =
        (Config)
            Proxy.newProxyInstance(
                Config.class.getClassLoader(),
                new Class<?>[] {Config.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getProperty")) return document.get();
                  if (method.getName().equals("addChangeListener")) {
                    listener.set((ConfigChangeListener) args[0]);
                    return null;
                  }
                  if (method.getName().equals("removeChangeListener")) {
                    detached.incrementAndGet();
                    return true;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    List<Long> revisions = new ArrayList<>();
    try (ApolloConfigurationSource<String> source =
        new ApolloConfigurationSource<>(config, "driverkit.cache", decoder())) {
      source.subscribe(snapshot -> revisions.add(snapshot.revision()));
      document.set("revision=2\nendpoint=two");
      listener.get().onChange(null);
      document.set("revision=3\nendpoint=bad\nunsupported=true");
      listener.get().onChange(null);
      document.set("revision=1\nendpoint=one");
      listener.get().onChange(null);
      assertEquals(List.of(1L, 2L), revisions);
      assertEquals(1, source.rejectedUpdates());
    }
    assertEquals(1, detached.get());
  }
}
