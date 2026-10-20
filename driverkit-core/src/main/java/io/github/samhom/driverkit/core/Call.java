package io.github.samhom.driverkit.core;

@FunctionalInterface
public interface Call<C, T> {
  T execute(C client, CallContext context) throws Exception;
}
