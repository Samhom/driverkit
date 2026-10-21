package io.github.samhom.driverkit.rocketmq.grpc;

import io.grpc.NameResolver;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import org.apache.rocketmq.client.java.route.Endpoints;

/** Internal compatibility patch for modern gRPC scheme lookup and literal IP TLS identity. */
public final class GrpcResolution {
  private GrpcResolution() {}

  @SuppressWarnings("deprecation")
  public static void configure(
      NettyChannelBuilder builder,
      Endpoints endpoints,
      List<InetSocketAddress> addresses,
      NameResolver.Factory delegate,
      boolean tls) {
    if (tls && addresses.size() != 1)
      throw new IllegalArgumentException("TLS requires one verified authority per native channel");
    String scheme = endpoints.getGrpcTarget().split(":", 2)[0];
    builder.nameResolverFactory(
        new NameResolver.Factory() {
          @Override
          public NameResolver newNameResolver(URI target, NameResolver.Args args) {
            return delegate.newNameResolver(target, args);
          }

          @Override
          public String getDefaultScheme() {
            return scheme;
          }
        });
    if (tls) {
      InetSocketAddress peer = addresses.get(0);
      String host = peer.getHostString();
      builder.overrideAuthority(
          (host.contains(":") ? "[" + host + "]" : host) + ":" + peer.getPort());
    }
  }
}
