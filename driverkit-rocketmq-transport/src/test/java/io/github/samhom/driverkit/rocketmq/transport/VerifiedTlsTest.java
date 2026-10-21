package io.github.samhom.driverkit.rocketmq.transport;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.SslHandler;
import java.nio.file.*;
import java.security.KeyStore;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;
import org.apache.rocketmq.remoting.netty.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class VerifiedTlsTest {
  @TempDir static Path directory;
  static final String password = UUID.randomUUID().toString();

  @BeforeAll
  static void certificates() throws Exception {
    keytool(
        "-genkeypair",
        "-alias",
        "ca",
        "-dname",
        "CN=DriverKit ephemeral test CA",
        "-ext",
        "bc:c",
        "-keyalg",
        "RSA",
        "-validity",
        "2",
        "-keystore",
        file("ca.p12"));
    keytool("-exportcert", "-alias", "ca", "-keystore", file("ca.p12"), "-file", file("ca.cer"));
    keytool(
        "-importcert",
        "-noprompt",
        "-alias",
        "ca",
        "-file",
        file("ca.cer"),
        "-keystore",
        file("trust.p12"));
    for (String name : List.of("valid", "mismatch", "expired")) {
      keytool(
          "-genkeypair",
          "-alias",
          "server",
          "-dname",
          "CN=ephemeral",
          "-keyalg",
          "RSA",
          "-keystore",
          file(name + ".p12"));
      keytool(
          "-certreq",
          "-alias",
          "server",
          "-keystore",
          file(name + ".p12"),
          "-file",
          file(name + ".csr"));
      List<String> args =
          new ArrayList<>(
              List.of(
                  "-gencert",
                  "-alias",
                  "ca",
                  "-keystore",
                  file("ca.p12"),
                  "-infile",
                  file(name + ".csr"),
                  "-outfile",
                  file(name + ".cer"),
                  "-ext",
                  "SAN=dns:" + (name.equals("mismatch") ? "wrong.example.org" : "localhost"),
                  "-ext",
                  "EKU=serverAuth",
                  "-validity",
                  "1"));
      if (name.equals("expired")) args.addAll(List.of("-startdate", "-3d"));
      keytool(args.toArray(String[]::new));
      keytool(
          "-importcert",
          "-noprompt",
          "-alias",
          "ca",
          "-keystore",
          file(name + ".p12"),
          "-file",
          file("ca.cer"));
      keytool(
          "-importcert",
          "-alias",
          "server",
          "-keystore",
          file(name + ".p12"),
          "-file",
          file(name + ".cer"));
    }
  }

  static String file(String name) {
    return directory.resolve(name).toString();
  }

  static void keytool(String... args) throws Exception {
    String executable =
        Path.of(
                System.getProperty("java.home"),
                "bin",
                System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool")
            .toString();
    List<String> command = new ArrayList<>(List.of(executable));
    command.addAll(Arrays.asList(args));
    command.addAll(List.of("-storepass:env", "DRIVERKIT_FIXTURE_PASSWORD", "-storetype", "PKCS12"));
    ProcessBuilder builder =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD);
    builder.environment().put("DRIVERKIT_FIXTURE_PASSWORD", password);
    Process process = builder.start();
    if (!process.waitFor(20, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      fail("Certificate generation timed out");
    }
    assertEquals(0, process.exitValue(), "Ephemeral certificate generation failed");
  }

  @Test
  void acceptsTrustedCertificateAndOriginalDnsIdentity() throws Exception {
    handshake("valid", "localhost", true, true);
  }

  @Test
  void rejectsUntrustedChain() throws Exception {
    handshake("valid", "localhost", false, false);
  }

  @Test
  void rejectsWrongDnsIdentity() throws Exception {
    handshake("mismatch", "localhost", true, false);
  }

  @Test
  void rejectsExpiredLeaf() throws Exception {
    handshake("expired", "localhost", true, false);
  }

  @Test
  void rejectsIpWithoutIpSubjectAlternativeName() throws Exception {
    handshake("valid", "127.0.0.1", true, false);
  }

  void handshake(String certificate, String host, boolean trusted, boolean success)
      throws Exception {
    String oldStore = System.getProperty("javax.net.ssl.trustStore"),
        oldPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    ExecutorService acceptor = Executors.newSingleThreadExecutor();
    KeyStore keys = KeyStore.getInstance("PKCS12");
    try (var stream = Files.newInputStream(Path.of(file(certificate + ".p12")))) {
      keys.load(stream, password.toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keys, password.toCharArray());
    SSLContext serverContext = SSLContext.getInstance("TLS");
    serverContext.init(kmf.getKeyManagers(), null, null);
    try (SSLServerSocket server =
        (SSLServerSocket)
            serverContext
                .getServerSocketFactory()
                .createServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(5000);
      Future<?> accepted =
          acceptor.submit(
              () -> {
                try (SSLSocket socket = (SSLSocket) server.accept()) {
                  socket.setSoTimeout(5000);
                  socket.startHandshake();
                  socket.getInputStream().read();
                } catch (Exception expectedForRejectedPeer) {
                  /* client result is asserted below */
                }
              });
      if (trusted) {
        System.setProperty("javax.net.ssl.trustStore", file("trust.p12"));
        System.setProperty("javax.net.ssl.trustStorePassword", password);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
      }
      NettyClientConfig config = new NettyClientConfig();
      config.setUseTLS(true);
      config.setScanAvailableNameSrv(false);
      config.setConnectTimeoutMillis(2000);
      config.setClientWorkerThreads(1);
      class Client extends NettyRemotingClient {
        Client() {
          super(config);
        }

        ChannelFuture connect(String target) {
          return doConnect(target);
        }
      }
      Client client = new Client();
      client.start();
      try {
        ChannelFuture connection = client.connect(host + ":" + server.getLocalPort()).sync();
        SslHandler ssl = connection.channel().pipeline().get(SslHandler.class);
        assertNotNull(ssl, "Patched native pipeline must contain TLS");
        assertEquals("HTTPS", ssl.engine().getSSLParameters().getEndpointIdentificationAlgorithm());
        assertTrue(ssl.handshakeFuture().await(5, TimeUnit.SECONDS), "Handshake must be bounded");
        assertEquals(success, ssl.handshakeFuture().isSuccess());
        if (!success) assertNotNull(ssl.handshakeFuture().cause());
        connection.channel().close().sync();
      } finally {
        client.shutdown();
      }
      accepted.get(6, TimeUnit.SECONDS);
    } finally {
      acceptor.shutdownNow();
      restore("javax.net.ssl.trustStore", oldStore);
      restore("javax.net.ssl.trustStorePassword", oldPassword);
    }
  }

  static void restore(String key, String value) {
    if (value == null) System.clearProperty(key);
    else System.setProperty(key, value);
  }
}
