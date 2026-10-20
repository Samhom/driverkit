package io.github.samhom.driverkit.core;

import java.util.Arrays;
import java.util.regex.Pattern;

/** Providers resolve credentials on generation creation, enabling credential rotation. */
@FunctionalInterface
public interface CredentialsProvider {
  Credentials resolve();

  static CredentialsProvider environment(String usernameVariable, String passwordVariable) {
    Pattern name = Pattern.compile("[A-Z][A-Z0-9_]{0,127}");
    if (usernameVariable == null
        || passwordVariable == null
        || !name.matcher(usernameVariable).matches()
        || !name.matcher(passwordVariable).matches())
      throw new IllegalArgumentException("Invalid credential environment variable name");
    return () -> {
      String username = System.getenv(usernameVariable);
      String password = System.getenv(passwordVariable);
      if (username == null || password == null || password.isEmpty())
        throw new IllegalStateException("Required credentials are unavailable");
      char[] material = password.toCharArray();
      try {
        return new Credentials(username, material);
      } finally {
        Arrays.fill(material, '\0');
      }
    };
  }
}
