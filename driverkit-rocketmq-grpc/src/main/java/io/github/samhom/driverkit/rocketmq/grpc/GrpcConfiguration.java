package io.github.samhom.driverkit.rocketmq.grpc;

import io.github.samhom.driverkit.core.Credentials;
import org.apache.rocketmq.client.apis.*;

final class GrpcConfiguration {
  private GrpcConfiguration() {}

  static ClientConfiguration create(GrpcSettings settings) {
    try (Credentials credentials = settings.credentials().resolve()) {
      return ClientConfiguration.newBuilder()
          .setEndpoints(settings.proxy().authority())
          .enableSsl(settings.proxy().tls())
          .setCredentialProvider(
              new StaticSessionCredentialsProvider(
                  credentials.username(), credentials.passwordText()))
          .setRequestTimeout(settings.requestTimeout())
          .setMaxStartupAttempts(1)
          .build();
    }
  }
}
