package io.github.samhom.driverkit.rocketmq.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.rocketmq.RocketMqSettings;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.junit.jupiter.api.Test;

class RocketMq4AdapterTest {
  @Test
  void explicitRpcCredentialsSignWithPatchedYamlLinkage() {
    assertTrue(
        org.yaml.snakeyaml.Yaml.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString()
            .contains("snakeyaml-2.4.jar"));
    var hook =
        new org.apache.rocketmq.acl.common.AclClientRPCHook(
            new org.apache.rocketmq.acl.common.SessionCredentials(
                "fixture", java.util.UUID.randomUUID().toString()));
    var request =
        org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand(10, null);
    hook.doBeforeRequest("127.0.0.1", request);
    assertTrue(request.getExtFields().containsKey("Signature"));
  }

  @Test
  void rejectsUnverifiedTlsForBothRolesBeforeSecrets() {
    var settings =
        RocketMqSettings.defaults(
            new Endpoint("localhost", 9876, true),
            () -> {
              throw new AssertionError("Must reject before secrets");
            },
            "fixture-group",
            "fixture-topic");
    assertThrows(
        IllegalArgumentException.class, () -> new RocketMq4ProducerAdapter().create(settings));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RocketMq4ConsumerAdapter((m, c) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
                .start(settings, null));
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("io.github.samhom.driverkit.rocketmq.transport.VerifiedTls"));
  }
}
