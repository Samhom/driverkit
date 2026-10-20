package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Owns bounded executors and a scoped registry. There is no process-global client cache. */
public final class ClientRuntime implements AutoCloseable {
  private final Map<ResourceId, ManagedClient<?, ?>> clients = new LinkedHashMap<>();
  private final Set<ResourceId> registrations = new HashSet<>();
  private final int maxClients;
  private final Observer observer;
  final AdmissionPolicy admission;
  final ThreadPoolExecutor calls;
  final ThreadPoolExecutor maintenance;
  final ScheduledThreadPoolExecutor timers;
  final CompletionDispatcher completions;
  final LifecycleCoordinator lifecycle;
  private boolean closed;

  public ClientRuntime() {
    this(128, 16, 256, Observer.NOOP);
  }

  public ClientRuntime(int maxClients, int callThreads, int queuedCalls, Observer observer) {
    this(maxClients, callThreads, queuedCalls, observer, AdmissionPolicy.ALLOW);
  }

  public ClientRuntime(
      int maxClients,
      int callThreads,
      int queuedCalls,
      Observer observer,
      AdmissionPolicy admission) {
    this(
        maxClients,
        callThreads,
        queuedCalls,
        observer,
        admission,
        Math.min(4, callThreads),
        Math.max(64, Math.min(100000, queuedCalls + callThreads)));
  }

  public ClientRuntime(
      int maxClients,
      int callThreads,
      int queuedCalls,
      Observer observer,
      AdmissionPolicy admission,
      int callbackThreads,
      int callbackCapacity) {
    if (maxClients < 1
        || maxClients > 4096
        || callThreads < 1
        || callThreads > 1024
        || queuedCalls < 1
        || queuedCalls > 100000) throw new IllegalArgumentException("Runtime limits out of bounds");
    this.maxClients = maxClients;
    this.observer = Objects.requireNonNull(observer, "observer");
    this.admission = Objects.requireNonNull(admission, "admission");
    completions = new CompletionDispatcher(callbackThreads, callbackCapacity);
    lifecycle = new LifecycleCoordinator(maxClients * 4);
    calls =
        new ThreadPoolExecutor(
            callThreads,
            callThreads,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queuedCalls),
            factory("driverkit-call"),
            new ThreadPoolExecutor.AbortPolicy());
    calls.allowCoreThreadTimeOut(true);
    maintenance =
        new ThreadPoolExecutor(
            2,
            2,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(maxClients * 4),
            factory("driverkit-maintenance"),
            new ThreadPoolExecutor.AbortPolicy());
    maintenance.allowCoreThreadTimeOut(true);
    timers = new ScheduledThreadPoolExecutor(1, factory("driverkit-timer"));
    timers.setRemoveOnCancelPolicy(true);
    timers.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  private static ThreadFactory factory(String prefix) {
    AtomicInteger ids = new AtomicInteger();
    return task -> {
      Thread thread = new Thread(task, prefix + "-" + ids.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  public <C, S> ManagedClient<C, S> register(
      ConfigSnapshot<S> initial, DriverAdapter<C, S> adapter, ClientPolicy policy) {
    Objects.requireNonNull(initial, "initial");
    Objects.requireNonNull(adapter, "adapter");
    Objects.requireNonNull(policy, "policy");
    synchronized (this) {
      if (closed) throw new ClientException(ClientException.Kind.CLOSED, "Runtime is closed");
      if (clients.containsKey(initial.resource()) || registrations.contains(initial.resource()))
        throw new IllegalArgumentException("Resource already registered");
      if (clients.size() + registrations.size() >= maxClients)
        throw new ClientException(ClientException.Kind.OVERLOADED, "Client registry limit reached");
      registrations.add(initial.resource());
    }
    // Network initialization must not hold the runtime monitor or delay a concurrent shutdown.
    ManagedClient<C, S> client;
    try {
      client = new ManagedClient<>(this, initial, adapter, policy, observer);
    } catch (RuntimeException | Error failure) {
      synchronized (this) {
        registrations.remove(initial.resource());
      }
      throw failure;
    }
    synchronized (this) {
      registrations.remove(initial.resource());
      if (!closed) {
        clients.put(initial.resource(), client);
        return client;
      }
    }
    client.beginClose();
    client.forceClose();
    throw new ClientException(ClientException.Kind.CLOSED, "Runtime closed during initialization");
  }

  public synchronized int clientCount() {
    return clients.size();
  }

  synchronized CompletionDispatcher.Reservation reserveCompletion() {
    if (closed) throw new ClientException(ClientException.Kind.CLOSED, "Runtime is closed");
    return completions.reserve();
  }

  @Override
  public void close() {
    close(Duration.ofSeconds(30));
  }

  public void close(Duration timeout) {
    if (timeout == null
        || timeout.isNegative()
        || timeout.isZero()
        || timeout.compareTo(Duration.ofMinutes(10)) > 0)
      throw new IllegalArgumentException("Invalid shutdown timeout");
    ArrayList<ManagedClient<?, ?>> snapshot;
    synchronized (this) {
      if (closed) return;
      closed = true;
      snapshot = new ArrayList<>(clients.values());
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    lifecycle.stopInitializing();
    for (ManagedClient<?, ?> client : snapshot) client.beginClose(deadline);
    calls.shutdown();
    for (ManagedClient<?, ?> client : snapshot) client.awaitClose(deadline);
    for (ManagedClient<?, ?> client : snapshot) client.forceClose(deadline);
    calls.shutdownNow();
    timers.shutdownNow();
    maintenance.shutdown();
    completions.close();
    try {
      maintenance.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      completions.awaitClose(deadline);
      lifecycle.awaitClose(deadline);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
