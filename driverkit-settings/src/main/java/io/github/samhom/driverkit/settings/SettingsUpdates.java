package io.github.samhom.driverkit.settings;

import static io.github.samhom.driverkit.core.UpdatePlan.Effect.*;

import io.github.samhom.driverkit.core.*;

public final class SettingsUpdates {
  private SettingsUpdates() {}

  public static UpdatePlan plan(
      io.github.samhom.driverkit.redis.RedisSettings a,
      io.github.samhom.driverkit.redis.RedisSettings b) {
    return UpdatePlan.builder()
        .field("endpoint", a.endpoint(), b.endpoint(), REBUILD)
        .field("credentials", a.credentials(), b.credentials(), REBUILD)
        .field("connectTimeout", a.connectTimeout(), b.connectTimeout(), REBUILD)
        .field("readTimeout", a.readTimeout(), b.readTimeout(), REBUILD)
        .field("maxConnections", a.maxConnections(), b.maxConnections(), REBUILD)
        .field("database", a.database(), b.database(), FORBIDDEN)
        .field("maxValueBytes", a.maxValueBytes(), b.maxValueBytes(), LIVE)
        .build();
  }

  public static UpdatePlan plan(
      io.github.samhom.driverkit.elasticsearch.ElasticsearchSettings a,
      io.github.samhom.driverkit.elasticsearch.ElasticsearchSettings b) {
    return UpdatePlan.builder()
        .field("endpoint", a.endpoint(), b.endpoint(), REBUILD)
        .field("credentials", a.credentials(), b.credentials(), REBUILD)
        .field("connectTimeout", a.connectTimeout(), b.connectTimeout(), REBUILD)
        .field("readTimeout", a.readTimeout(), b.readTimeout(), REBUILD)
        .field("maxConnections", a.maxConnections(), b.maxConnections(), REBUILD)
        .field("maxBatchItems", a.maxBatchItems(), b.maxBatchItems(), LIVE)
        .field("maxDocumentBytes", a.maxDocumentBytes(), b.maxDocumentBytes(), LIVE)
        .build();
  }

  public static UpdatePlan plan(
      io.github.samhom.driverkit.couchbase.CouchbaseSettings a,
      io.github.samhom.driverkit.couchbase.CouchbaseSettings b) {
    return UpdatePlan.builder()
        .field("endpoint", a.endpoint(), b.endpoint(), REBUILD)
        .field("credentials", a.credentials(), b.credentials(), REBUILD)
        .field("connectTimeout", a.connectTimeout(), b.connectTimeout(), REBUILD)
        .field("operationTimeout", a.operationTimeout(), b.operationTimeout(), REBUILD)
        .field("bucket", a.bucket(), b.bucket(), FORBIDDEN)
        .field("scope", a.scope(), b.scope(), FORBIDDEN)
        .field("collection", a.collection(), b.collection(), FORBIDDEN)
        .field("maxDocumentBytes", a.maxDocumentBytes(), b.maxDocumentBytes(), LIVE)
        .build();
  }

  public static UpdatePlan plan(
      io.github.samhom.driverkit.rocketmq.RocketMqSettings a,
      io.github.samhom.driverkit.rocketmq.RocketMqSettings b) {
    return UpdatePlan.builder()
        .field("nameServer", a.nameServer(), b.nameServer(), REBUILD)
        .field("credentials", a.credentials(), b.credentials(), REBUILD)
        .field("group", a.group(), b.group(), FORBIDDEN)
        .field("topic", a.topic(), b.topic(), FORBIDDEN)
        .field("requestTimeout", a.requestTimeout(), b.requestTimeout(), REBUILD)
        .field("consumerThreads", a.consumerThreads(), b.consumerThreads(), REBUILD)
        .field("maxMessageBytes", a.maxMessageBytes(), b.maxMessageBytes(), REBUILD)
        .build();
  }
}
