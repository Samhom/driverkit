"""Check effective dependency trees of applications that do not inherit the SDK parent."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def resolved(name):
    path = ROOT / ".ci/bom-consumers" / name / "target/dependencies.json"
    if path.stat().st_size > 10 * 1024 * 1024:
        raise ValueError("Oversized consumer dependency tree")
    found = {}

    def visit(node):
        if node.get("groupId") and node.get("artifactId"):
            found[(node["groupId"], node["artifactId"])] = node["version"]
        for child in node.get("children", []):
            visit(child)

    visit(json.loads(path.read_text(encoding="utf-8")))
    return found


redis = resolved("redis")
assert redis["redis.clients", "jedis"] == "3.10.0"
assert redis["com.couchbase.client", "java-client"] == "3.13.0"
search = resolved("search")
assert search["org.elasticsearch.client", "elasticsearch-rest-high-level-client"] == "7.17.29"
assert search["org.apache.rocketmq", "rocketmq-client"] == "5.5.1"
assert ("org.lz4", "lz4-java") not in search
assert search["at.yawk.lz4", "lz4-java"] == "1.12.0"
print("Product BOMs preserve independent driver generations and patched compression.")
