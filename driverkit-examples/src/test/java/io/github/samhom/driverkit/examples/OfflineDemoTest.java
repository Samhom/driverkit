package io.github.samhom.driverkit.examples;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class OfflineDemoTest {
  @Test
  void exampleRunsWithoutExternalServices() {
    assertDoesNotThrow(() -> OfflineDemo.main(new String[0]));
  }
}
