package io.github.samhom.driverkit.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BoundaryTest {
  @Test
  void configurationDoesNotRenderItsSettings() {
    assertFalse(
        new ConfigSnapshot<>(new ResourceId("app", "test", "db"), 1, "sensitive-material")
            .toString()
            .contains("sensitive-material"));
  }

  @Test
  void credentialsAreRedactedAndBecomeUnreadableAfterClose() {
    Credentials credentials = new Credentials("operator", new char[] {'a', 'b'});
    assertEquals("Credentials{redacted}", credentials.toString());
    credentials.close();
    assertThrows(IllegalStateException.class, credentials::passwordText);
  }

  @Test
  void identityAndEndpointRejectInjection() {
    assertThrows(IllegalArgumentException.class, () -> new ResourceId("app\r\n", "test", "db"));
    for (String host :
        new String[] {
          "user@host", "example.com/other", "example.com.", "example..com", "example.com\r\n"
        }) assertThrows(IllegalArgumentException.class, () -> new Endpoint(host, 443, true));
    assertEquals(
        "https://[::1]:443", new Endpoint("::1", 443, true).uri("https", "http").toString());
  }

  @Test
  void timeoutsAndResourceLimitsAreBounded() {
    assertThrows(IllegalArgumentException.class, () -> Operation.read("get", Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> Operation.read("get", Duration.ofDays(1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ClientPolicy(Integer.MAX_VALUE, Duration.ofSeconds(1), Duration.ofSeconds(1)));
  }

  @Test
  void configurationSubscriptionReplaysAndRejectsStaleEvents() throws Exception {
    ResourceId resource = new ResourceId("app", "test", "db");
    PushConfigurationSource<String> source =
        new PushConfigurationSource<>(new ConfigSnapshot<>(resource, 2, "two"));
    AtomicInteger delivered = new AtomicInteger();
    AutoCloseable subscription = source.subscribe(snapshot -> delivered.incrementAndGet());
    assertEquals(1, delivered.get());
    assertFalse(source.publish(new ConfigSnapshot<>(resource, 1, "one")));
    source.publish(new ConfigSnapshot<>(resource, 3, "three"));
    assertEquals(2, delivered.get());
    subscription.close();
    source.publish(new ConfigSnapshot<>(resource, 4, "four"));
    assertEquals(2, delivered.get());
  }
}
