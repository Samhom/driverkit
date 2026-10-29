package io.github.samhom.driverkit.core;

import java.util.function.Consumer;

/** A trusted credential-store integration owns refresh IO and authentication to its backend. */
public interface CredentialSource extends CredentialsProvider {
  CredentialState state();

  AutoCloseable subscribe(Consumer<CredentialState> listener);

  default void refresh() throws Exception {}
}
