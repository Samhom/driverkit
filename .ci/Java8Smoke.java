import io.github.samhom.driverkit.core.*;
import java.time.Duration;

public class Java8Smoke {
    public static void main(String[] args) {
        FactoryAdapter<String, String> adapter = new FactoryAdapter<>("example", value -> value, (value, timeout) -> { }, value -> { });
        try (ClientRuntime runtime = new ClientRuntime()) {
            ManagedClient<String, String> client = runtime.register(new ConfigSnapshot<>(new ResourceId("example", "test", "cache"), 1, "value"), adapter, ClientPolicy.defaults());
            if (!"value".equals(client.execute(Operation.read("get", Duration.ofSeconds(1)), (value, context) -> value))) throw new AssertionError("Unexpected result");
        }
        System.out.println("Java 8 core smoke passed");
    }
}
