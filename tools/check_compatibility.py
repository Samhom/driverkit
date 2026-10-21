"""Validate the declared compatibility contract against POMs and render its public table.

No network calls. A passing unit build never upgrades a server combination to verified.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
START, END = "<!-- BEGIN COMPATIBILITY MATRIX -->", "<!-- END COMPATIBILITY MATRIX -->"


def validate():
    document = json.loads((ROOT / "docs/compatibility.json").read_text(encoding="utf-8"))
    pom = ET.parse(ROOT / "pom.xml").getroot()
    properties = {node.tag.split("}")[-1]: node.text for node in pom.find("m:properties", NS)}
    modules = {node.text for node in pom.findall("m:modules/m:module", NS)} | {node.text for node in pom.findall("m:profiles/m:profile/m:modules/m:module", NS)}
    if document["sdk_version"] != pom.findtext("m:version", namespaces=NS):
        raise ValueError("SDK version drift")
    seen, represented = set(), set()
    labels = {"verified": "已验证", "theoretical": "理论兼容 / 待验证", "unsupported": "不支持"}
    lines = [START, "", "| SDK 模块 | 原生驱动 | 服务端 / 范围 | JDK | 状态 | 证据或限制 |", "| --- | --- | --- | --- | --- | --- |"]
    for row in document["combinations"]:
        module, status = row["module"], row["status"]
        if module not in modules or status not in labels or row["jdk"] not in (8, 11, 17, 21):
            raise ValueError("Unknown module, support status or JDK")
        key = (module, row["driver"]["version"], row["server"], row["jdk"], row["scope"])
        if key in seen:
            raise ValueError("Duplicate compatibility combination")
        seen.add(key)
        represented.add(module)
        driver = row["driver"]
        if "property" in driver and properties.get(driver["property"]) != driver["version"]:
            raise ValueError("Native driver version drift: " + module)
        if driver["artifact"] != "JDK":
            mp = ET.parse(ROOT / module / "pom.xml").getroot()
            dependencies = mp.findall("m:dependencies/m:dependency", NS)
            versions = [node.findtext("m:version", namespaces=NS) for node in dependencies if node.findtext("m:artifactId", namespaces=NS) == driver["artifact"]]
            resolved = [properties.get(v[2:-1]) if v and v.startswith("${") else v for v in versions]
            if driver["version"] not in resolved:
                raise ValueError("Matrix does not match the adapter's actual native dependency: " + module)
        if not row.get("reason"):
            raise ValueError("Every combination needs an explicit scope/limitation")
        evidence = row.get("evidence")
        if status == "verified":
            if not evidence or not evidence.get("command") or not evidence.get("url", "").startswith("https://github.com/Samhom/driverkit/actions/runs/"):
                raise ValueError("Verified combinations require a reproducible command and immutable CI run")
        detail = row["reason"] + (" [CI](" + evidence["url"] + ")" if evidence else "")
        cells = [module, driver["artifact"] + " " + driver["version"], row["server"] + " / " + row["scope"], str(row["jdk"]), labels[status], detail]
        lines.append("| " + " | ".join(cell.replace("|", "\\|").replace("\n", " ") for cell in cells) + " |")
    required = {"driverkit-core", "driverkit-redis-jedis", "driverkit-elasticsearch", "driverkit-couchbase", "driverkit-rocketmq", "driverkit-rocketmq-grpc", "driverkit-rocketmq-transport"}
    if not required <= represented:
        raise ValueError("Missing a managed driver or transport in the matrix")
    legacy_profile = pom.find("m:profiles/m:profile[m:id='legacy-drivers']", NS)
    if legacy_profile is not None:
        legacy_bom = ET.parse(ROOT / "driverkit-legacy-bom/pom.xml").getroot()
        def managed_versions(node):
            return {(d.findtext("m:groupId", namespaces=NS), d.findtext("m:artifactId", namespaces=NS)): d.findtext("m:version", namespaces=NS) for d in node.findall("m:dependencyManagement/m:dependencies/m:dependency", NS)}
        build_versions, published_versions = managed_versions(legacy_profile), managed_versions(legacy_bom)
        if any(published_versions.get(key) != value for key, value in build_versions.items()):
            raise ValueError("Legacy build and published BOM security versions drift")
        for module in legacy_profile.findall("m:modules/m:module", NS):
            if module.text == "driverkit-legacy-bom":
                continue
            legacy_pom = ET.parse(ROOT / module.text / "pom.xml").getroot()
            for dependency in legacy_pom.findall("m:dependencies/m:dependency", NS):
                group = dependency.findtext("m:groupId", namespaces=NS)
                if group in {"redis.clients", "org.elasticsearch.client", "com.couchbase.client", "org.apache.rocketmq"}:
                    if dependency.findtext("m:scope", namespaces=NS) != "provided" or dependency.findtext("m:optional", namespaces=NS) != "true":
                        raise ValueError("Legacy native drivers must remain provided and optional")
                    product_bom = ET.parse(ROOT / (module.text + "-bom") / "pom.xml").getroot()
                    key = (group, dependency.findtext("m:artifactId", namespaces=NS))
                    if managed_versions(product_bom).get(key) != dependency.findtext("m:version", namespaces=NS):
                        raise ValueError("Legacy product BOM and compiled driver versions drift")
    return "\n".join(lines + ["", END])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--render", action="store_true")
    args = parser.parse_args()
    table = validate()
    path = ROOT / "docs/COMPATIBILITY.md"
    current = path.read_text(encoding="utf-8")
    start, end = current.index(START), current.index(END) + len(END)
    updated = current[:start] + table + current[end:]
    if args.render:
        path.write_text(updated, encoding="utf-8")
    elif updated != current:
        raise ValueError("Compatibility table drift: run python3 tools/check_compatibility.py --render")
    print("Compatibility matrix matches SDK/native versions and declared evidence.")


if __name__ == "__main__":
    main()
