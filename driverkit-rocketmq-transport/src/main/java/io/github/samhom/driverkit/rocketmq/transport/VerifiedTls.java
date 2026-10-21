package io.github.samhom.driverkit.rocketmq.transport;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLParameters;

/**
 * Internal, version-locked patch entry point; uses the JDK trust store and HTTPS identity rules.
 */
public final class VerifiedTls {
  public static final AttributeKey<String> PEER_HOST =
      AttributeKey.valueOf("driverkit.tls.peerHost");
  public static final AttributeKey<Integer> PEER_PORT =
      AttributeKey.valueOf("driverkit.tls.peerPort");

  private VerifiedTls() {}

  // Checked exceptions preserve the original constructor's catch blocks in the pinned source.
  public static SslContext context() throws IOException, CertificateException {
    try {
      return SslContextBuilder.forClient()
          .sslProvider(SslProvider.JDK)
          .protocols("TLSv1.3", "TLSv1.2")
          .build();
    } catch (javax.net.ssl.SSLException failure) {
      throw new IllegalStateException("Unable to initialize verified TLS", failure);
    }
  }

  public static SslHandler handler(Channel channel, SslContext context, int timeoutMillis) {
    String host = channel.attr(PEER_HOST).get();
    Integer port = channel.attr(PEER_PORT).get();
    if (host == null || host.isEmpty() || port == null || timeoutMillis <= 0)
      throw new IllegalStateException("Missing verified TLS peer or handshake deadline");
    // The original route/name-server host is attached BEFORE resolution. Never use reverse DNS.
    SslHandler handler =
        context.newHandler(channel.alloc(), host.replace("[", "").replace("]", ""), port);
    SSLParameters parameters = handler.engine().getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    handler.engine().setSSLParameters(parameters);
    handler.setHandshakeTimeoutMillis(timeoutMillis);
    return handler;
  }
}
