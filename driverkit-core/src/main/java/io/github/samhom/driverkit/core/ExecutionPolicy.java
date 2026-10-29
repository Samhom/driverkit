package io.github.samhom.driverkit.core;

import java.time.Duration;

/** Immutable opt-in resource isolation and circuit policy. */
public final class ExecutionPolicy {
  private final int threads, queue, failures;
  private final Duration openFor;

  private ExecutionPolicy(int threads, int queue, int failures, Duration openFor) {
    this.threads = threads;
    this.queue = queue;
    this.failures = failures;
    this.openFor = openFor;
  }

  public static ExecutionPolicy shared() {
    return new ExecutionPolicy(0, 0, 0, Duration.ofSeconds(1));
  }

  public static ExecutionPolicy isolated(int threads, int queue) {
    return new ExecutionPolicy(
        Limits.count(threads, 64), Limits.count(queue, 10000), 0, Duration.ofSeconds(1));
  }

  public ExecutionPolicy withCircuitBreaker(int failures, Duration openFor) {
    return new ExecutionPolicy(
        threads, queue, Limits.count(failures, 100000), Limits.timeout(openFor));
  }

  public int threads() {
    return threads;
  }

  public int queuedCalls() {
    return queue;
  }

  public int failureThreshold() {
    return failures;
  }

  public Duration openFor() {
    return openFor;
  }
}
