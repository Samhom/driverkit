package io.github.samhom.driverkit.rocketmq.grpc;

import static org.junit.jupiter.api.Assertions.*;

import apache.rocketmq.v2.*;
import io.github.samhom.driverkit.core.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcClientImpl;
import org.junit.jupiter.api.Test;

class GrpcTransportTest {
  @Test
  void patchedTlsRejectsUntrustedCertificate(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
    String password = UUID.randomUUID().toString();
    java.nio.file.Path store = directory.resolve("server.p12");
    String keytool =
        java.nio.file.Path.of(
                System.getProperty("java.home"),
                "bin",
                System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool")
            .toString();
    ProcessBuilder generator =
        new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias",
                "server",
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=IP:127.0.0.1",
                "-keyalg",
                "RSA",
                "-validity",
                "1",
                "-storetype",
                "PKCS12",
                "-keystore",
                store.toString(),
                "-storepass:env",
                "DRIVERKIT_FIXTURE_PASSWORD")
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD);
    generator.environment().put("DRIVERKIT_FIXTURE_PASSWORD", password);
    Process process = generator.start();
    if (!process.waitFor(20, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      fail("Certificate generation timed out");
    }
    assertEquals(0, process.exitValue());
    java.security.KeyStore keys = java.security.KeyStore.getInstance("PKCS12");
    try (var input = java.nio.file.Files.newInputStream(store)) {
      keys.load(input, password.toCharArray());
    }
    var managers =
        javax.net.ssl.KeyManagerFactory.getInstance(
            javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
    managers.init(keys, password.toCharArray());
    var context =
        io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.configure(
                io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder.forServer(managers),
                io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.JDK)
            .build();
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .sslContext(context)
            .addService(new MessagingServiceGrpc.MessagingServiceImplBase() {})
            .build()
            .start();
    RpcClientImpl client = new RpcClientImpl(new Endpoints("127.0.0.1:" + server.getPort()), true);
    try {
      ExecutionException failure = assertThrows(ExecutionException.class, () -> heartbeat(client));
      assertEquals(
          io.grpc.Status.Code.UNAVAILABLE, io.grpc.Status.fromThrowable(failure).getCode());
      Throwable cause = failure;
      while (cause != null && !(cause instanceof javax.net.ssl.SSLHandshakeException))
        cause = cause.getCause();
      assertNotNull(cause, "Failure must be a TLS handshake rejection");
    } finally {
      client.shutdown();
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void usesOfficialUnshadedProviderAndManagedPatchedTransport() {
    assertEquals(
        "org.apache.rocketmq.client.java.impl.ClientServiceProviderImpl",
        ClientServiceProvider.loadService().getClass().getName());
    assertTrue(
        RpcClientImpl.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString()
            .contains("driverkit-rocketmq-grpc"));
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("org.apache.rocketmq.shaded.io.grpc.ManagedChannel"));
  }

  @Test
  void authenticationDenialIsBoundedAndNotRetried() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(
                new MessagingServiceGrpc.MessagingServiceImplBase() {
                  @Override
                  public void queryRoute(
                      QueryRouteRequest request, StreamObserver<QueryRouteResponse> response) {
                    requests.incrementAndGet();
                    response.onError(io.grpc.Status.UNAUTHENTICATED.asRuntimeException());
                  }
                })
            .build()
            .start();
    try {
      GrpcSettings settings =
          new GrpcSettings(
              new Endpoint("127.0.0.1", server.getPort(), false),
              () -> new Credentials("fixture", UUID.randomUUID().toString().toCharArray()),
              "fixture-topic",
              Duration.ofSeconds(1),
              1024);
      assertTimeoutPreemptively(
          Duration.ofSeconds(15),
          () -> assertThrows(Exception.class, () -> new GrpcProducerAdapter().create(settings)));
      assertEquals(1, requests.get());
    } finally {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void nativeChannelRecoversAfterServerRestart() throws Exception {
    var service =
        new MessagingServiceGrpc.MessagingServiceImplBase() {
          @Override
          public void heartbeat(
              HeartbeatRequest request, StreamObserver<HeartbeatResponse> response) {
            response.onNext(HeartbeatResponse.getDefaultInstance());
            response.onCompleted();
          }
        };
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(service)
            .build()
            .start();
    int port = server.getPort();
    RpcClientImpl client = new RpcClientImpl(new Endpoints("127.0.0.1:" + port), false);
    try {
      heartbeat(client);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      assertThrows(ExecutionException.class, () -> heartbeat(client));
      server =
          NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
              .addService(service)
              .build()
              .start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
      boolean recovered = false;
      while (System.nanoTime() < deadline) {
        try {
          heartbeat(client);
          recovered = true;
          break;
        } catch (ExecutionException expectedDuringReconnect) {
          Thread.sleep(100);
        }
      }
      assertTrue(recovered, "Native channel must reconnect within the test deadline");
    } finally {
      client.shutdown();
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void heartbeat(RpcClientImpl client) throws Exception {
    client
        .heartbeat(
            new Metadata(),
            HeartbeatRequest.getDefaultInstance(),
            Runnable::run,
            Duration.ofMillis(300))
        .get(2, TimeUnit.SECONDS);
  }
}
