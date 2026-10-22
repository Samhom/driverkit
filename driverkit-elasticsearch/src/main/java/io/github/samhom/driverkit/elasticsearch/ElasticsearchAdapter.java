package io.github.samhom.driverkit.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.util.Timeout;

public final class ElasticsearchAdapter
    implements DriverAdapter<ElasticsearchClient, ElasticsearchSettings> {
  @Override
  public String id() {
    return "elasticsearch9";
  }

  @Override
  public void validate(ElasticsearchSettings settings) {
    java.util.Objects.requireNonNull(settings);
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    if (error instanceof co.elastic.clients.elasticsearch._types.ElasticsearchException) {
      int status =
          ((co.elastic.clients.elasticsearch._types.ElasticsearchException) error).status();
      if (status == 401) return ClientException.Kind.AUTHENTICATION;
      if (status == 403) return ClientException.Kind.AUTHORIZATION;
      if (status == 404) return ClientException.Kind.NOT_FOUND;
      if (status == 409) return ClientException.Kind.CONFLICT;
      if (status == 429) return ClientException.Kind.OVERLOADED;
      if (status >= 400 && status < 500) return ClientException.Kind.INVALID_REQUEST;
    }
    if (error instanceof java.io.IOException)
      return operation.replaySafe()
          ? ClientException.Kind.UNAVAILABLE
          : ClientException.Kind.WRITE_OUTCOME_UNKNOWN;
    return ClientException.Kind.DRIVER_FAILURE;
  }

  @Override
  public ElasticsearchClient create(ElasticsearchSettings settings) {
    Endpoint endpoint = settings.endpoint();
    HttpHost host =
        new HttpHost(endpoint.tls() ? "https" : "http", endpoint.host(), endpoint.port());
    Header authorization;
    try (Credentials credentials = settings.credentials().resolve()) {
      if (credentials.username().contains(":"))
        throw new IllegalArgumentException("Invalid HTTP authentication username");
      String encoded =
          java.util.Base64.getEncoder()
              .encodeToString(
                  (credentials.username() + ":" + credentials.passwordText())
                      .getBytes(java.nio.charset.StandardCharsets.UTF_8));
      authorization = new BasicHeader("Authorization", "Basic " + encoded);
    }
    Rest5Client low =
        Rest5Client.builder(host)
            .setDefaultHeaders(new Header[] {authorization})
            .setConnectionConfigCallback(
                connection ->
                    connection
                        .setConnectTimeout(
                            Timeout.ofMilliseconds(settings.connectTimeout().toMillis()))
                        .setSocketTimeout(
                            Timeout.ofMilliseconds(settings.readTimeout().toMillis())))
            .setRequestConfigCallback(
                request ->
                    request
                        .setConnectionRequestTimeout(
                            Timeout.ofMilliseconds(settings.connectTimeout().toMillis()))
                        .setResponseTimeout(
                            Timeout.ofMilliseconds(settings.readTimeout().toMillis()))
                        .setRedirectsEnabled(false))
            .setConnectionManagerCallback(
                manager ->
                    manager
                        .setMaxConnTotal(settings.maxConnections())
                        .setMaxConnPerRoute(settings.maxConnections()))
            .setHttpClientConfigCallback(http -> http.disableAutomaticRetries())
            .build();
    co.elastic.clients.transport.rest5_client.low_level.RequestOptions.Builder requestOptions =
        co.elastic.clients.transport.rest5_client.low_level.RequestOptions.DEFAULT.toBuilder();
    requestOptions.setHttpAsyncResponseConsumerFactory(
        new co.elastic.clients.transport.rest5_client.low_level.HttpAsyncResponseConsumerFactory
            .BasicAsyncResponseConsumerFactory(16 * 1024 * 1024));
    co.elastic.clients.transport.rest5_client.Rest5ClientOptions options =
        new co.elastic.clients.transport.rest5_client.Rest5ClientOptions.Builder(requestOptions)
            .build();
    try {
      return new ElasticsearchClient(
              new Rest5ClientTransport(low, new JacksonJsonpMapper(), options))
          .withTransportOptions(
              builder ->
                  builder
                      .retryConfig(co.elastic.clients.transport.RetryConfig.disabled())
                      .keepResponseBodyOnException(false));
    } catch (RuntimeException error) {
      try {
        low.close();
      } catch (java.io.IOException ignored) {
      }
      throw error;
    }
  }

  @Override
  public void warmup(ElasticsearchClient client, Duration timeout) throws Exception {
    co.elastic.clients.transport.rest5_client.Rest5ClientOptions.Builder options =
        (co.elastic.clients.transport.rest5_client.Rest5ClientOptions.Builder)
            client._transportOptions().toBuilder();
    options
        .restClientRequestOptionsBuilder()
        .setRequestConfig(
            org.apache.hc.client5.http.config.RequestConfig.custom()
                .setResponseTimeout(
                    Timeout.ofMilliseconds(Math.max(1, (timeout.toNanos() + 999999) / 1000000)))
                .setConnectionRequestTimeout(
                    Timeout.ofMilliseconds(Math.max(1, (timeout.toNanos() + 999999) / 1000000)))
                .setRedirectsEnabled(false)
                .build());
    if (!client.withTransportOptions(options.build()).ping().value())
      throw new IllegalStateException("Elasticsearch readiness check failed");
  }

  @Override
  public void close(ElasticsearchClient client) throws Exception {
    client.close();
  }

  @Override
  public AdapterCapabilities capabilities() {
    return AdapterCapabilities.nativeApi(false);
  }
}
