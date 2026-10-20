package io.github.samhom.driverkit.core;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Bounded ownership survives caller timeouts, including drivers that ignore interruption. */
final class LifecycleCoordinator {
  private final ThreadPoolExecutor initializers, cleanup;
  private final ScheduledThreadPoolExecutor watchdog;
  private final Set<Owned<?>> resources = new HashSet<>();
  private final int capacity;
  private int reserved;
  private boolean stopped;

  LifecycleCoordinator(int capacity) {
    this.capacity = capacity;
    initializers = pool("driverkit-initialize", capacity);
    cleanup = pool("driverkit-cleanup", capacity);
    watchdog = new ScheduledThreadPoolExecutor(1, factory("driverkit-lifecycle-timer"));
    watchdog.setRemoveOnCancelPolicy(true);
    watchdog.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  private static ThreadFactory factory(String name) {
    AtomicInteger ids = new AtomicInteger();
    return task -> {
      Thread thread = new Thread(task, name + "-" + ids.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  private static ThreadPoolExecutor pool(String name, int capacity) {
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(
            2,
            2,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(capacity),
            factory(name),
            new ThreadPoolExecutor.AbortPolicy());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  <C, S> Owned<C> initialize(
      DriverAdapter<C, S> adapter, S settings, Duration warmupTimeout, Duration shutdownTimeout) {
    Owned<C> owned = new Owned<>(adapter, settings, warmupTimeout, shutdownTimeout);
    synchronized (this) {
      if (stopped) throw new ClientException(ClientException.Kind.CLOSED, "Lifecycle is closed");
      if (reserved >= capacity)
        throw new ClientException(
            ClientException.Kind.OVERLOADED, "Lifecycle ownership limit reached");
      reserved++;
      resources.add(owned);
    }
    initializers.execute(owned.initialization);
    return owned;
  }

  void stopInitializing() {
    List<Owned<?>> snapshot;
    synchronized (this) {
      stopped = true;
      snapshot = new ArrayList<>(resources);
      shutdownIfIdle();
    }
    for (Owned<?> owned : snapshot) owned.cancelUnclaimed();
  }

  private void finished(Owned<?> owned, Throwable error) {
    synchronized (this) {
      resources.remove(owned);
      // Failed native cleanup quarantines its slot: retries cannot grow unknown live resources.
      if (error == null) reserved--;
      shutdownIfIdle();
    }
  }

  private void shutdownIfIdle() {
    if (stopped && resources.isEmpty()) {
      initializers.shutdown();
      cleanup.shutdown();
      watchdog.shutdown();
    }
  }

  void awaitClose(long deadline) throws InterruptedException {
    initializers.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    cleanup.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
  }

  <T> T control(Callable<T> action, Duration timeout) {
    FactoryAdapter<Object[], Boolean> adapter =
        new FactoryAdapter<>(
            "lifecycle-control",
            ignored -> new Object[] {action.call()},
            (value, remaining) -> {},
            value -> {});
    Owned<Object[]> owned = initialize(adapter, Boolean.TRUE, timeout, timeout);
    try {
      @SuppressWarnings("unchecked")
      T result = (T) owned.awaitReady()[0];
      return result;
    } finally {
      owned.retire(System.nanoTime() + timeout.toNanos(), null);
    }
  }

  private interface Creator<C> {
    C create(Duration remaining) throws Exception;
  }

  private interface Phase<C> {
    void run(C client, Duration remaining) throws Exception;
  }

  final class Owned<C> {
    private final Creator<C> creator;
    private final Phase<C> warmup, closer;
    private final long initializationDeadline;
    private final Duration shutdownTimeout;
    private final CompletableFuture<C> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final Runnable initialization = this::initialize;
    private C client;
    private Thread initializingThread, closingThread;
    private boolean initializationFinished, claimed, retireRequested, closeQueued, finished;
    private volatile long closeDeadline = Long.MAX_VALUE;
    private ScheduledFuture<?> alarm;

    <S> Owned(DriverAdapter<C, S> adapter, S settings, Duration timeout, Duration shutdownTimeout) {
      this.creator =
          remaining -> {
            adapter.validate(settings);
            return adapter.create(settings, remainingInitialization());
          };
      this.warmup = adapter::warmup;
      this.closer = adapter::close;
      this.initializationDeadline = System.nanoTime() + timeout.toNanos();
      this.shutdownTimeout = shutdownTimeout;
    }

    C awaitReady() {
      try {
        C value =
            ready.get(
                Math.max(1, initializationDeadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        synchronized (this) {
          if (retireRequested)
            throw new ClientException(ClientException.Kind.CLOSED, "Initialization was abandoned");
          checkInitializationDeadline();
          claimed = true;
        }
        return value;
      } catch (TimeoutException error) {
        ClientException failure =
            new ClientException(
                ClientException.Kind.TIMEOUT, "Initialization deadline exceeded", error);
        abandon(failure);
        throw failure;
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        ClientException failure =
            new ClientException(
                ClientException.Kind.UPDATE_REJECTED, "Initialization interrupted", error);
        abandon(failure);
        throw failure;
      } catch (ExecutionException error) {
        Throwable cause = error.getCause();
        if (cause instanceof ClientException) throw (ClientException) cause;
        if (cause instanceof TimeoutException)
          throw new ClientException(
              ClientException.Kind.TIMEOUT, "Initialization deadline exceeded", cause);
        throw new ClientException(
            ClientException.Kind.UPDATE_REJECTED, "Client initialization failed", cause);
      }
    }

    private void checkInitializationDeadline() throws TimeoutException {
      if (initializationDeadline - System.nanoTime() <= 0) throw new TimeoutException();
    }

    private Duration remainingInitialization() throws TimeoutException {
      synchronized (this) {
        if (retireRequested)
          throw new ClientException(ClientException.Kind.CLOSED, "Initialization was abandoned");
      }
      checkInitializationDeadline();
      return Duration.ofNanos(Math.max(1, initializationDeadline - System.nanoTime()));
    }

    private void initialize() {
      synchronized (this) {
        initializingThread = Thread.currentThread();
      }
      try {
        C value =
            Objects.requireNonNull(
                creator.create(remainingInitialization()), "Adapter returned no client");
        synchronized (this) {
          client = value;
        }
        warmup.run(value, remainingInitialization());
        synchronized (this) {
          remainingInitialization();
          ready.complete(value);
        }
      } catch (Throwable error) {
        synchronized (this) {
          retireRequested = true;
          closeDeadline = Math.min(closeDeadline, System.nanoTime() + shutdownTimeout.toNanos());
        }
        ready.completeExceptionally(error);
      } finally {
        synchronized (this) {
          initializingThread = null;
          initializationFinished = true;
        }
        scheduleClose();
      }
    }

    private void cancelUnclaimed() {
      synchronized (this) {
        if (claimed) return;
        abandonLocked(
            new ClientException(
                ClientException.Kind.CLOSED, "Runtime closed during initialization"));
      }
      scheduleClose();
    }

    private void abandon(Throwable failure) {
      synchronized (this) {
        abandonLocked(failure);
      }
      scheduleClose();
    }

    private void abandonLocked(Throwable failure) {
      retireRequested = true;
      closeDeadline = Math.min(closeDeadline, System.nanoTime() + shutdownTimeout.toNanos());
      ready.completeExceptionally(failure);
      if (initializingThread != null) initializingThread.interrupt();
      if (initializers.remove(initialization)) initializationFinished = true;
    }

    void retire(long deadline, Consumer<Throwable> completion) {
      if (completion != null) closed.whenComplete((value, error) -> completion.accept(error));
      synchronized (this) {
        retireRequested = true;
        closeDeadline = Math.min(closeDeadline, deadline);
        if (closingThread != null) armAlarm();
      }
      scheduleClose();
    }

    private void scheduleClose() {
      boolean noClient;
      synchronized (this) {
        if (!retireRequested || !initializationFinished || closeQueued || finished) return;
        closeQueued = true;
        noClient = client == null;
      }
      if (noClient) finish(null);
      else cleanup.execute(this::closeNative);
    }

    private void armAlarm() {
      if (alarm != null) alarm.cancel(false);
      alarm =
          watchdog.schedule(
              () -> {
                synchronized (Owned.this) {
                  if (closingThread != null) closingThread.interrupt();
                }
              },
              Math.max(1, closeDeadline - System.nanoTime()),
              TimeUnit.NANOSECONDS);
    }

    private void closeNative() {
      Throwable failure = null;
      synchronized (this) {
        closingThread = Thread.currentThread();
        armAlarm();
      }
      try {
        closer.run(client, Duration.ofNanos(Math.max(1, closeDeadline - System.nanoTime())));
      } catch (Throwable error) {
        failure = error;
      } finally {
        synchronized (this) {
          closingThread = null;
          if (alarm != null) alarm.cancel(false);
        }
        finish(failure);
      }
    }

    private void finish(Throwable error) {
      synchronized (this) {
        if (finished) return;
        finished = true;
      }
      LifecycleCoordinator.this.finished(this, error);
      if (error == null) closed.complete(null);
      else closed.completeExceptionally(error);
    }

    boolean isClosed() {
      return closed.isDone() && !closed.isCompletedExceptionally();
    }

    void awaitClosed(long deadline) throws Exception {
      closed.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
  }
}
