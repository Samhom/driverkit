package io.github.samhom.driverkit.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.json.JsonData;
import io.github.samhom.driverkit.core.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Full query DSL remains typed in the native client; common document operations have a compact
 * facade.
 */
public final class ManagedSearch {
  public static final class PartialBulkFailure extends RuntimeException {
    private final BulkResponse response;

    PartialBulkFailure(BulkResponse response) {
      super("Bulk operation has failed items");
      this.response = response;
    }

    public BulkResponse response() {
      return response;
    }
  }

  private final ManagedClient<ElasticsearchClient, ElasticsearchSettings> client;
  private final Duration timeout;
  private final int maximumBatch, maximumDocument;

  public ManagedSearch(
      ManagedClient<ElasticsearchClient, ElasticsearchSettings> client,
      ElasticsearchSettings settings) {
    this.client = java.util.Objects.requireNonNull(client);
    timeout = settings.readTimeout();
    maximumBatch = settings.maxBatchItems();
    maximumDocument = settings.maxDocumentBytes();
  }

  private static void index(String value) {
    if (value == null || !value.matches("[a-z0-9][a-z0-9_.-]{0,127}"))
      throw new IllegalArgumentException("Invalid index or alias");
  }

  private static void id(String value) {
    if (value == null || value.isEmpty() || value.getBytes(StandardCharsets.UTF_8).length > 512)
      throw new IllegalArgumentException("Invalid document ID size");
  }

  public GetResponse<JsonData> get(String index, String id) {
    return client.executeAwait(Operation.read("get", timeout), getCall(index, id));
  }

  public CompletableFuture<GetResponse<JsonData>> getAsync(String index, String id) {
    return client.executeAsync(Operation.read("get", timeout), getCall(index, id));
  }

  private AsyncCall<ElasticsearchClient, GetResponse<JsonData>> getCall(String index, String id) {
    index(index);
    id(id);
    return (nativeClient, context) ->
        async(nativeClient, context).get(request -> request.index(index).id(id), JsonData.class);
  }

  public IndexResponse index(String index, String id, String json) {
    return client.executeAwait(Operation.write("index", timeout), indexCall(index, id, json));
  }

  public CompletableFuture<IndexResponse> indexAsync(String index, String id, String json) {
    return client.executeAsync(Operation.write("index", timeout), indexCall(index, id, json));
  }

  private AsyncCall<ElasticsearchClient, IndexResponse> indexCall(
      String index, String id, String json) {
    index(index);
    id(id);
    Limits.bytes(json == null ? null : json.getBytes(StandardCharsets.UTF_8), maximumDocument);
    JsonData document = JsonData.fromJson(json);
    return (nativeClient, context) -> {
      Limits.bytes(
          json.getBytes(StandardCharsets.UTF_8),
          Math.min(
              maximumDocument, context.settings(ElasticsearchSettings.class).maxDocumentBytes()));
      return async(nativeClient, context)
          .index(request -> request.index(index).id(id).document(document));
    };
  }

  public DeleteResponse delete(String index, String id) {
    return client.executeAwait(Operation.write("delete", timeout), deleteCall(index, id));
  }

  public CompletableFuture<DeleteResponse> deleteAsync(String index, String id) {
    return client.executeAsync(Operation.write("delete", timeout), deleteCall(index, id));
  }

  private AsyncCall<ElasticsearchClient, DeleteResponse> deleteCall(String index, String id) {
    index(index);
    id(id);
    return (nativeClient, context) ->
        async(nativeClient, context).delete(request -> request.index(index).id(id));
  }

  public BulkResponse bulk(BulkRequest request) {
    return client.executeAwait(Operation.write("bulk", timeout), bulkCall(request));
  }

  public CompletableFuture<BulkResponse> bulkAsync(BulkRequest request) {
    return client.executeAsync(Operation.write("bulk", timeout), bulkCall(request));
  }

  private AsyncCall<ElasticsearchClient, BulkResponse> bulkCall(BulkRequest request) {
    if (request == null
        || request.operations().isEmpty()
        || request.operations().size() > maximumBatch)
      throw new IllegalArgumentException("Bulk size out of bounds");
    return (nativeClient, context) -> {
      ElasticsearchSettings current = context.settings(ElasticsearchSettings.class);
      if (request.operations().size() > Math.min(maximumBatch, current.maxBatchItems()))
        throw new IllegalArgumentException("Bulk size out of bounds");
      int documentLimit = Math.min(maximumDocument, current.maxDocumentBytes());
      ByteBudget budget = new ByteBudget(16 * 1024 * 1024);
      for (co.elastic.clients.elasticsearch.core.bulk.BulkOperation operation :
          request.operations()) {
        java.util.Iterator<?> items = operation._serializables();
        while (items.hasNext()) {
          long before = budget.bytes();
          try (jakarta.json.stream.JsonGenerator generator =
              nativeClient._jsonpMapper().jsonProvider().createGenerator(budget)) {
            nativeClient._jsonpMapper().serialize(items.next(), generator);
          }
          if (budget.bytes() - before > documentLimit)
            throw new IllegalArgumentException("Bulk document exceeds byte budget");
          budget.write('\n');
        }
      }
      return async(nativeClient, context)
          .bulk(request)
          .thenApply(
              response -> {
                if (response.errors()) throw new PartialBulkFailure(response);
                return response;
              });
    };
  }

  public <T> SearchResponse<T> search(SearchRequest request, Class<T> type) {
    return client.executeAwait(Operation.read("search", timeout), searchCall(request, type));
  }

  public <T> CompletableFuture<SearchResponse<T>> searchAsync(
      SearchRequest request, Class<T> type) {
    return client.executeAsync(Operation.read("search", timeout), searchCall(request, type));
  }

  private <T> AsyncCall<ElasticsearchClient, SearchResponse<T>> searchCall(
      SearchRequest request, Class<T> type) {
    if (request == null
        || (request.size() != null && (request.size() < 0 || request.size() > 10000)))
      throw new IllegalArgumentException("Search size out of bounds");
    return (nativeClient, context) -> async(nativeClient, context).search(request, type);
  }

  private static ElasticsearchAsyncClient async(ElasticsearchClient client, CallContext context) {
    // This view borrows the generation's transport. Only the adapter closes that transport.
    ElasticsearchSettings settings = context.settings(ElasticsearchSettings.class);
    co.elastic.clients.transport.rest5_client.Rest5ClientOptions.Builder options =
        (co.elastic.clients.transport.rest5_client.Rest5ClientOptions.Builder)
            client._transportOptions().toBuilder();
    options
        .restClientRequestOptionsBuilder()
        .setRequestConfig(
            org.apache.hc.client5.http.config.RequestConfig.custom()
                .setConnectionRequestTimeout(
                    org.apache.hc.core5.util.Timeout.ofMilliseconds(
                        context.remainingMillis(settings.connectTimeout())))
                .setConnectTimeout(
                    org.apache.hc.core5.util.Timeout.ofMilliseconds(
                        context.remainingMillis(settings.connectTimeout())))
                .setResponseTimeout(
                    org.apache.hc.core5.util.Timeout.ofMilliseconds(
                        context.remainingMillis(settings.readTimeout())))
                .setRedirectsEnabled(false)
                .build());
    return new ElasticsearchAsyncClient(client._transport(), options.build());
  }

  public ManagedClient<ElasticsearchClient, ElasticsearchSettings> handle() {
    return client;
  }
}
