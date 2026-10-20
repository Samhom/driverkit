package io.github.samhom.driverkit.core;

import java.util.Arrays;
import java.util.Objects;

/** Short-lived credential material. Native drivers may retain their own password copies. */
public final class Credentials implements AutoCloseable {
  private final String username;
  private final char[] password;
  private boolean closed;

  public Credentials(String username, char[] password) {
    this.username = Objects.requireNonNull(username, "username");
    this.password = Objects.requireNonNull(password, "password").clone();
  }

  public synchronized String username() {
    check();
    return username;
  }

  public synchronized String passwordText() {
    check();
    return new String(password);
  }

  private void check() {
    if (closed) throw new IllegalStateException("Credentials are closed");
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      Arrays.fill(password, '\0');
      closed = true;
    }
  }

  @Override
  public String toString() {
    return "Credentials{redacted}";
  }
}
