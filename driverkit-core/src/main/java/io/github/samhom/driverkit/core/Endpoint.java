package io.github.samhom.driverkit.core;

import java.net.URI;

/** Endpoints must come from an authorized resource catalog. This is not a generic URL fetcher. */
public final class Endpoint {
  private final String host;
  private final int port;
  private final boolean tls;

  public Endpoint(String host, int port, boolean tls) {
    if (host == null
        || host.length() > 253
        || host.endsWith(".")
        || host.contains("..")
        || !(host.matches("[A-Za-z0-9][A-Za-z0-9.-]*") || host.matches("[0-9A-Fa-f:]{2,45}")))
      throw new IllegalArgumentException("Invalid endpoint host");
    if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid endpoint port");
    this.host = host;
    this.port = port;
    this.tls = tls;
    uri("https", "http");
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public boolean tls() {
    return tls;
  }

  public URI uri(String secureScheme, String plaintextScheme) {
    try {
      return new URI(tls ? secureScheme : plaintextScheme, null, host, port, null, null, null);
    } catch (java.net.URISyntaxException e) {
      throw new IllegalArgumentException("Invalid endpoint", e);
    }
  }

  public String authority() {
    return (host.contains(":") ? "[" + host + "]" : host) + ":" + port;
  }

  @Override
  public String toString() {
    return "Endpoint{tls=" + tls + "}";
  }
}
