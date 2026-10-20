package io.github.samhom.driverkit.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "driverkit", ignoreUnknownFields = false)
public final class DriverKitProperties {
  private int maxClients = 128;
  private int callThreads = 16;
  private int queuedCalls = 256;

  public int getMaxClients() {
    return maxClients;
  }

  public void setMaxClients(int value) {
    maxClients = value;
  }

  public int getCallThreads() {
    return callThreads;
  }

  public void setCallThreads(int value) {
    callThreads = value;
  }

  public int getQueuedCalls() {
    return queuedCalls;
  }

  public void setQueuedCalls(int value) {
    queuedCalls = value;
  }
}
