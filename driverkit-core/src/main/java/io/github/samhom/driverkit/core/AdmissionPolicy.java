package io.github.samhom.driverkit.core;

/**
 * Fast, local governance extension. Authentication and resource authorization remain server
 * enforced.
 */
@FunctionalInterface
public interface AdmissionPolicy {
  void check(ResourceId resource, Operation operation);

  AdmissionPolicy ALLOW = (resource, operation) -> {};
}
