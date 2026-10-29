package io.github.samhom.driverkit.couchbase;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class PatchedCoreTest {
  @Test
  void nativeCoreUsesTheRebuiltRelocatedGuava() throws Exception {
    Class<?> type =
        Class.forName("com.couchbase.client.core.deps.com.google.common.collect.ImmutableList");
    Object list = type.getMethod("of", Object.class).invoke(null, "fixture");
    assertEquals(List.of("fixture"), list);
    assertTrue(
        type.getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString()
            .contains("driverkit-couchbase-core"));
    try (java.util.jar.JarFile jar =
        new java.util.jar.JarFile(
            new java.io.File(type.getProtectionDomain().getCodeSource().getLocation().toURI()))) {
      Properties props = new Properties();
      try (java.io.InputStream input =
          jar.getInputStream(
              jar.getJarEntry("META-INF/maven/com.google.guava/guava/pom.properties"))) {
        props.load(input);
      }
      assertEquals("33.7.2-jre", props.getProperty("version"));
    }
  }
}
