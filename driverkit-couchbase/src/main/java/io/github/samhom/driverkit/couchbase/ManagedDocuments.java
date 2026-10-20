package io.github.samhom.driverkit.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import io.github.samhom.driverkit.core.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class ManagedDocuments {
  public record Document(String json, long cas) {}

  private final ManagedClient<Cluster, CouchbaseSettings> client;
  private final String bucket, scope, collection;
  private final Duration timeout;
  private final int maximum;

  public ManagedDocuments(
      ManagedClient<Cluster, CouchbaseSettings> client, CouchbaseSettings settings) {
    this.client = java.util.Objects.requireNonNull(client);
    bucket = settings.bucket();
    scope = settings.scope();
    collection = settings.collection();
    timeout = settings.operationTimeout();
    maximum = settings.maxDocumentBytes();
  }

  private static void key(String key) {
    if (key == null || key.isEmpty() || key.getBytes(StandardCharsets.UTF_8).length > 250)
      throw new IllegalArgumentException("Invalid document key size");
  }

  public CompletableFuture<Document> getAsync(String key) {
    key(key);
    return client.executeAsync(
        Operation.read("get", timeout),
        (cluster, context) ->
            cluster
                .bucket(bucket)
                .scope(scope)
                .collection(collection)
                .async()
                .get(key, GetOptions.getOptions().timeout(context.remaining()))
                .thenApply(
                    result -> new Document(result.contentAsObject().toString(), result.cas())));
  }

  public CompletableFuture<Long> upsertAsync(String key, String json) {
    key(key);
    Limits.bytes(json == null ? null : json.getBytes(StandardCharsets.UTF_8), maximum);
    JsonObject value = JsonObject.fromJson(json);
    return client.executeAsync(
        Operation.write("upsert", timeout),
        (cluster, context) -> {
          Limits.bytes(
              json.getBytes(StandardCharsets.UTF_8),
              Math.min(maximum, context.settings(CouchbaseSettings.class).maxDocumentBytes()));
          return cluster
              .bucket(bucket)
              .scope(scope)
              .collection(collection)
              .async()
              .upsert(key, value, UpsertOptions.upsertOptions().timeout(context.remaining()))
              .thenApply(MutationResult::cas);
        });
  }

  public CompletableFuture<Long> replaceAsync(String key, String json, long cas) {
    key(key);
    Limits.bytes(json == null ? null : json.getBytes(StandardCharsets.UTF_8), maximum);
    JsonObject value = JsonObject.fromJson(json);
    return client.executeAsync(
        Operation.write("replace", timeout),
        (cluster, context) -> {
          Limits.bytes(
              json.getBytes(StandardCharsets.UTF_8),
              Math.min(maximum, context.settings(CouchbaseSettings.class).maxDocumentBytes()));
          return cluster
              .bucket(bucket)
              .scope(scope)
              .collection(collection)
              .async()
              .replace(
                  key, value, ReplaceOptions.replaceOptions().cas(cas).timeout(context.remaining()))
              .thenApply(MutationResult::cas);
        });
  }

  public ManagedClient<Cluster, CouchbaseSettings> handle() {
    return client;
  }
}
