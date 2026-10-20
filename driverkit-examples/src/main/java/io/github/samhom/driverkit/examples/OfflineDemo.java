package io.github.samhom.driverkit.examples;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/** Executable lifecycle example; the in-memory adapter intentionally needs no external service. */
public final class OfflineDemo {
  private OfflineDemo() {}

  public static void main(String[] args) {
    ResourceId resource = new ResourceId("example", "development", "cache");
    FactoryAdapter<ConcurrentHashMap<String, String>, String> adapter =
        new FactoryAdapter<>(
            "memory",
            settings -> new ConcurrentHashMap<>(),
            (client, timeout) -> {},
            ConcurrentHashMap::clear);
    try (ClientRuntime runtime = new ClientRuntime()) {
      ManagedClient<ConcurrentHashMap<String, String>, String> client =
          runtime.register(
              new ConfigSnapshot<>(resource, 1, "initial"), adapter, ClientPolicy.defaults());
      client.execute(
          Operation.write("set", Duration.ofSeconds(1)),
          (nativeClient, context) -> nativeClient.put("example", "value"));
      String value =
          client.execute(
              Operation.read("get", Duration.ofSeconds(1)),
              (nativeClient, context) -> nativeClient.get("example"));
      System.out.println("read=" + value);
      client.reload(new ConfigSnapshot<>(resource, 2, "replacement"));
      System.out.println("activeRevision=" + client.status().revision());
    }
  }
}
