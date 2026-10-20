package io.github.samhom.driverkit.core;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface AsyncCall<C, T> {
  CompletionStage<T> execute(C client, CallContext context) throws Exception;
}
