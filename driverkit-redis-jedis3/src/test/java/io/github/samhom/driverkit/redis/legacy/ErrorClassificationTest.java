package io.github.samhom.driverkit.redis.legacy;

import static org.junit.jupiter.api.Assertions.*;

import io.github.samhom.driverkit.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ErrorClassificationTest {
  @Test
  void legacyNativeErrorsKeepTheSameGovernanceSemantics() {
    Jedis3Adapter adapter = new Jedis3Adapter();
    Operation read = Operation.read("get", Duration.ofSeconds(1));
    Operation write = Operation.write("set", Duration.ofSeconds(1));
    assertEquals(
        ClientException.Kind.AUTHENTICATION,
        adapter.classify(
            new redis.clients.jedis.exceptions.JedisAccessControlException("WRONGPASS fixture"),
            read));
    assertEquals(
        ClientException.Kind.AUTHORIZATION,
        adapter.classify(
            new redis.clients.jedis.exceptions.JedisAccessControlException("NOPERM fixture"),
            read));
    assertEquals(
        ClientException.Kind.WRITE_OUTCOME_UNKNOWN,
        adapter.classify(
            new redis.clients.jedis.exceptions.JedisConnectionException(
                new java.net.SocketTimeoutException()),
            write));
  }
}
