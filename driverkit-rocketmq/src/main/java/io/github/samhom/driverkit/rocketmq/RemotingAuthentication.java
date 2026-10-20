package io.github.samhom.driverkit.rocketmq;

import io.github.samhom.driverkit.core.*;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

final class RemotingAuthentication {
  private RemotingAuthentication() {}

  static void validateTransport(RocketMqSettings settings) {
    java.util.Objects.requireNonNull(settings);
    if (settings.nameServer().tls())
      throw new IllegalArgumentException(
          "Remoting TLS requires an audited transport adapter with server identity verification");
  }

  static RPCHook hook(CredentialsProvider provider) {
    try (Credentials credentials = provider.resolve()) {
      return new AclClientRPCHook(
          new SessionCredentials(credentials.username(), credentials.passwordText()));
    }
  }

  static void validateUpdate(RocketMqSettings previous, RocketMqSettings next) {
    if (!previous.group().equals(next.group()) || !previous.topic().equals(next.topic()))
      throw new IllegalArgumentException("Topic/group migration requires a new resource binding");
  }
}
