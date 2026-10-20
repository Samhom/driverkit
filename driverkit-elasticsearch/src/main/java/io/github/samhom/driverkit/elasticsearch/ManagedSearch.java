package io.github.samhom.driverkit.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.json.JsonData;
import io.github.samhom.driverkit.core.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

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
    index(index);
    id(id);
    return client.execute(
        Operation.read("get", timeout),
        (nativeClient, context) ->
            nativeClient.get(request -> request.index(index).id(id), JsonData.class));
  }

  public IndexResponse index(String index, String id, String json) {
    index(index);
    id(id);
    Limits.bytes(json == null ? null : json.getBytes(StandardCharsets.UTF_8), maximumDocument);
    JsonData document = JsonData.fromJson(json);
    return client.execute(
        Operation.write("index", timeout),
        (nativeClient, context) -> {
          Limits.bytes(
              json.getBytes(StandardCharsets.UTF_8),
              Math.min(
                  maximumDocument,
                  context.settings(ElasticsearchSettings.class).maxDocumentBytes()));
          return nativeClient.index(request -> request.index(index).id(id).document(document));
        });
  }

  public DeleteResponse delete(String index, String id) {
    index(index);
    id(id);
    return client.execute(
        Operation.write("delete", timeout),
        (nativeClient, context) -> nativeClient.delete(request -> request.index(index).id(id)));
  }

  public BulkResponse bulk(BulkRequest request) {
    if (request == null
        || request.operations().isEmpty()
        || request.operations().size() > maximumBatch)
      throw new IllegalArgumentException("Bulk size out of bounds");
    return client.execute(
        Operation.write("bulk", timeout),
        (nativeClient, context) -> {
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
          BulkResponse response = nativeClient.bulk(request);
          if (response.errors()) throw new PartialBulkFailure(response);
          return response;
        });
  }

  public <T> SearchResponse<T> search(SearchRequest request, Class<T> type) {
    if (request == null
        || (request.size() != null && (request.size() < 0 || request.size() > 10000)))
      throw new IllegalArgumentException("Search size out of bounds");
    return client.execute(
        Operation.read("search", timeout),
        (nativeClient, context) -> nativeClient.search(request, type));
  }

  public ManagedClient<ElasticsearchClient, ElasticsearchSettings> handle() {
    return client;
  }
}
