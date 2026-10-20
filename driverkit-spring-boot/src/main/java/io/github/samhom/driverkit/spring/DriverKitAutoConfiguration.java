package io.github.samhom.driverkit.spring;

import io.github.samhom.driverkit.core.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(DriverKitProperties.class)
public class DriverKitAutoConfiguration {
  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean(ClientRuntime.class)
  public ClientRuntime driverKitRuntime(
      DriverKitProperties properties,
      ObjectProvider<Observer> observers,
      ObjectProvider<AdmissionPolicy> admission) {
    Observer observer = new CompositeObserver(observers.orderedStream().toArray(Observer[]::new));
    AdmissionPolicy policy =
        (resource, operation) ->
            admission.orderedStream().forEach(item -> item.check(resource, operation));
    return new ClientRuntime(
        properties.getMaxClients(),
        properties.getCallThreads(),
        properties.getQueuedCalls(),
        observer,
        policy);
  }
}
