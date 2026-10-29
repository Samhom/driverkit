package io.github.samhom.driverkit.elasticsearch.legacy;

import io.github.samhom.driverkit.core.*;
import io.github.samhom.driverkit.elasticsearch.ElasticsearchSettings;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;

/** Retains the HLRC 7 request/result API; native dependency is supplied by the application. */
public final class Elasticsearch7Adapter
    implements DriverAdapter<RestHighLevelClient, ElasticsearchSettings> {
  @Override
  public String id() {
    return "elasticsearch-hlrc7";
  }

  @Override
  public void validate(ElasticsearchSettings settings) {
    java.util.Objects.requireNonNull(settings);
  }

  @Override
  public RestHighLevelClient create(ElasticsearchSettings settings) {
    validate(settings);
    try (Credentials credentials = settings.credentials().resolve()) {
      if (credentials.username().contains(":"))
        throw new IllegalArgumentException("Invalid basic authentication identity");
      String authorization =
          "Basic "
              + Base64.getEncoder()
                  .encodeToString(
                      (credentials.username() + ":" + credentials.passwordText())
                          .getBytes(StandardCharsets.UTF_8));
      return new RestHighLevelClient(
          RestClient.builder(
                  new HttpHost(
                      settings.endpoint().host(),
                      settings.endpoint().port(),
                      settings.endpoint().tls() ? "https" : "http"))
              .setDefaultHeaders(
                  new org.apache.http.Header[] {new BasicHeader("Authorization", authorization)})
              .setRequestConfigCallback(
                  config ->
                      config
                          .setConnectTimeout(Math.toIntExact(settings.connectTimeout().toMillis()))
                          .setSocketTimeout(Math.toIntExact(settings.readTimeout().toMillis()))
                          .setConnectionRequestTimeout(
                              Math.toIntExact(settings.connectTimeout().toMillis()))
                          .setRedirectsEnabled(false))
              .setHttpClientConfigCallback(
                  client ->
                      client
                          .setMaxConnTotal(settings.maxConnections())
                          .setMaxConnPerRoute(settings.maxConnections())
                          .setSSLContext(org.apache.http.ssl.SSLContexts.createSystemDefault())));
    }
  }

  @Override
  public void warmup(RestHighLevelClient client, Duration remaining) throws Exception {
    int millis = (int) Math.max(1, Math.min(Integer.MAX_VALUE, remaining.toMillis()));
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.setRequestConfig(
        org.apache.http.client.config.RequestConfig.custom()
            .setConnectTimeout(millis)
            .setSocketTimeout(millis)
            .setConnectionRequestTimeout(millis)
            .setRedirectsEnabled(false)
            .build());
    options.setHttpAsyncResponseConsumerFactory(
        new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(16 * 1024 * 1024));
    if (!client.ping(options.build()))
      throw new IllegalStateException("Elasticsearch readiness failed");
  }

  @Override
  public void close(RestHighLevelClient client) throws Exception {
    client.close();
  }

  @Override
  public ClientException.Kind classify(Throwable error, Operation operation) {
    error = FailureClassifier.unwrap(error);
    if (error instanceof org.elasticsearch.ElasticsearchStatusException)
      return FailureClassifier.http(
          ((org.elasticsearch.ElasticsearchStatusException) error).status().getStatus(), operation);
    if (error instanceof org.elasticsearch.client.ResponseException)
      return FailureClassifier.http(
          ((org.elasticsearch.client.ResponseException) error)
              .getResponse()
              .getStatusLine()
              .getStatusCode(),
          operation);
    return FailureClassifier.transport(error, operation);
  }

  @Override
  public UpdatePlan updatePlan(ElasticsearchSettings previous, ElasticsearchSettings next) {
    return io.github.samhom.driverkit.settings.SettingsUpdates.plan(previous, next);
  }

  @Override
  public java.time.Duration operationTimeout(ElasticsearchSettings settings, Operation operation) {
    return settings.readTimeout();
  }

  @Override
  public CredentialsProvider credentials(ElasticsearchSettings settings) {
    return settings.credentials();
  }
}
