package io.github.samhom.driverkit.core;

import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

/** Stores provider references and lifetime metadata, never caches credential strings or arrays. */
public final class RotatingCredentialsProvider implements CredentialSource {
  private CredentialsProvider provider;
  private CredentialState current;
  private final Runnable refresh;
  private final Map<Long, Consumer<CredentialState>> listeners = new LinkedHashMap<>();
  private long sequence;

  public RotatingCredentialsProvider(
      CredentialsProvider provider, CredentialState state, Runnable refresh) {
    this.provider = Objects.requireNonNull(provider);
    this.current = Objects.requireNonNull(state);
    this.refresh = Objects.requireNonNull(refresh);
  }

  @Override
  public synchronized CredentialState state() {
    return current;
  }

  public boolean rotate(CredentialsProvider provider, CredentialState state) {
    List<Consumer<CredentialState>> copy;
    synchronized (this) {
      if (current.revoked())
        throw new ClientException(ClientException.Kind.REVOKED, "Credential source is revoked");
      if (state.version() <= current.version()) return false;
      this.provider = Objects.requireNonNull(provider);
      current = Objects.requireNonNull(state);
      copy = new ArrayList<>(listeners.values());
    }
    for (Consumer<CredentialState> listener : copy)
      try {
        listener.accept(state);
      } catch (RuntimeException ignored) {
      }
    return true;
  }

  public void revoke(long version) {
    List<Consumer<CredentialState>> copy;
    CredentialState state;
    synchronized (this) {
      if (version <= current.version())
        throw new IllegalArgumentException("Revocation requires a higher version");
      state = new CredentialState(version, current.refreshAt(), current.expiresAt(), true);
      current = state;
      copy = new ArrayList<>(listeners.values());
    }
    for (Consumer<CredentialState> listener : copy)
      try {
        listener.accept(state);
      } catch (RuntimeException ignored) {
      }
  }

  @Override
  public Credentials resolve() {
    CredentialState before;
    CredentialsProvider resolver;
    synchronized (this) {
      before = current;
      resolver = provider;
      check(before);
    }
    Credentials credentials = Objects.requireNonNull(resolver.resolve());
    synchronized (this) {
      try {
        check(current);
        if (current.version() != before.version())
          throw new ClientException(
              ClientException.Kind.UNAVAILABLE, "Credentials changed during resolution");
      } catch (RuntimeException failure) {
        credentials.close();
        throw failure;
      }
    }
    return credentials;
  }

  private static void check(CredentialState state) {
    if (state.revoked() || !Instant.now().isBefore(state.expiresAt()))
      throw new ClientException(ClientException.Kind.REVOKED, "Credentials are expired or revoked");
  }

  @Override
  public AutoCloseable subscribe(Consumer<CredentialState> listener) {
    Objects.requireNonNull(listener);
    long id;
    CredentialState state;
    synchronized (this) {
      if (listeners.size() >= 1024)
        throw new IllegalStateException("Credential listener limit reached");
      id = ++sequence;
      listeners.put(id, listener);
      state = current;
    }
    try {
      listener.accept(state);
    } catch (RuntimeException | Error failure) {
      synchronized (this) {
        listeners.remove(id);
      }
      throw failure;
    }
    final long key = id;
    return () -> {
      synchronized (this) {
        listeners.remove(key);
      }
    };
  }

  @Override
  public void refresh() {
    refresh.run();
  }
}
