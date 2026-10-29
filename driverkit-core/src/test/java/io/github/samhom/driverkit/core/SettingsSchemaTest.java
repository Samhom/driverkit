package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class SettingsSchemaTest {
  static final class Mutable {
    final List<String> nodes;

    Mutable(List<String> nodes) {
      this.nodes = nodes;
    }
  }

  static final SettingsSchema<Mutable> SCHEMA =
      SettingsSchema.copying(
          "fixture.settings",
          2,
          Mutable.class,
          value -> new Mutable(new ArrayList<>(value.nodes)),
          value -> {
            if (value.nodes.isEmpty() || value.nodes.size() > 4)
              throw new IllegalArgumentException("Node count out of bounds");
          });

  @Test
  void freezesPublisherInputAndReturnsIndependentViews() {
    Mutable input = new Mutable(new ArrayList<>(List.of("one")));
    ConfigSnapshot<Mutable> snapshot =
        new ConfigSnapshot<>(new ResourceId("schema", "test", "db"), 1, input, SCHEMA);
    input.nodes.clear();
    snapshot.settings().nodes.clear();
    assertEquals(List.of("one"), snapshot.settings().nodes);
    assertEquals(2, snapshot.schemaVersion());
    assertEquals("fixture.settings", snapshot.schemaId());
    assertEquals(List.of("one"), snapshot.withRevision(2).settings().nodes);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ConfigSnapshot<>(snapshot.resource(), 2, input, SCHEMA));
    assertThrows(
        IllegalArgumentException.class, () -> new ConfigSnapshot<>(snapshot.resource(), 2, input));
  }

  @Test
  void changingInputAfterEnqueueCannotChangePublishedConfiguration() throws Exception {
    Mutable input = new Mutable(new ArrayList<>(List.of("original")));
    ResourceId id = new ResourceId("schema", "test", "db");
    ConfigSnapshot<Mutable> snapshot = new ConfigSnapshot<>(id, 1, input, SCHEMA);
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<String, Mutable> handle =
          runtime.register(
              snapshot,
              new FactoryAdapter<>("fixture", s -> s.nodes.get(0), (c, t) -> {}, c -> {}),
              ClientPolicy.defaults());
      input.nodes.set(0, "mutated");
      try (ClientSession<String> session = handle.session()) {
        session.settings(Mutable.class).nodes.clear();
      }
      assertEquals(
          "original",
          handle.execute(
              Operation.read("get", Duration.ofSeconds(1)),
              (c, ctx) -> ctx.settings(Mutable.class).nodes.get(0)));
    }
  }

  @Test
  void rejectsIdentityCopierInsteadOfPretendingItFreezesMutableData() {
    SettingsSchema<Mutable> invalid =
        SettingsSchema.copying("fixture", 1, Mutable.class, value -> value, value -> {});
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ConfigSnapshot<>(
                new ResourceId("schema", "test", "db"),
                1,
                new Mutable(new ArrayList<>(List.of("node"))),
                invalid));
  }
}
