package io.github.samhom.driverkit.core;

/**
 * Explicit promise that configuration fields are immutable. Providers are external capabilities,
 * not embedded credential material. Implementors must not expose mutable collections or arrays.
 */
public interface ImmutableSettings {}
