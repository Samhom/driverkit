package io.github.samhom.driverkit.spring;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

class DriverKitAutoConfigurationTest {
  @Test
  void createsConfiguredRuntimeAndClosesItWithApplicationContext() {
    ClientRuntime runtime;
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      context
          .getEnvironment()
          .getPropertySources()
          .addFirst(
              new MapPropertySource(
                  "test",
                  Map.of(
                      "driverkit.max-clients",
                      "1",
                      "driverkit.call-threads",
                      "1",
                      "driverkit.queued-calls",
                      "2")));
      context.register(DriverKitAutoConfiguration.class);
      context.refresh();
      assertEquals(1, context.getBean(DriverKitProperties.class).getMaxClients());
      runtime = context.getBean(ClientRuntime.class);
      FactoryAdapter<String, String> adapter =
          new FactoryAdapter<>("test", value -> value, (value, timeout) -> {}, value -> {});
      runtime.register(
          new ConfigSnapshot<>(new ResourceId("app", "test", "one"), 1, "value"),
          adapter,
          ClientPolicy.defaults());
      assertThrows(
          ClientException.class,
          () ->
              runtime.register(
                  new ConfigSnapshot<>(new ResourceId("app", "test", "two"), 1, "value"),
                  adapter,
                  ClientPolicy.defaults()));
    }
    FactoryAdapter<String, String> adapter =
        new FactoryAdapter<>("test", value -> value, (value, timeout) -> {}, value -> {});
    assertThrows(
        ClientException.class,
        () ->
            runtime.register(
                new ConfigSnapshot<>(new ResourceId("app", "test", "later"), 1, "value"),
                adapter,
                ClientPolicy.defaults()));
  }
}
