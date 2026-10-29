"""Audit public Maven coordinates from resolved dependency trees using OSV.dev.

Only coordinates and versions are sent. Source files and configuration are not read.
Run Maven dependency:tree -DoutputType=json -DoutputFile=target/dependencies.json first.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def coordinates(root: Path, repository: Path, experimental: bool, legacy: bool = False):
    found = set()
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    project = ET.parse(root / "pom.xml").getroot()
    modules = [node.text for node in project.findall("m:modules/m:module", ns)]
    if experimental:
        modules += [node.text for node in project.findall("m:profiles/m:profile[m:id='experimental-grpc']/m:modules/m:module", ns)]
    if legacy:
        modules += [node.text for node in project.findall("m:profiles/m:profile[m:id='legacy-drivers']/m:modules/m:module", ns)]
    files = [root / module / "target/dependencies.json" for module in modules]
    jars = set()
    if not files or len(files) > 64:
        raise ValueError("Expected bounded Maven dependency trees")

    def visit(node):
        group, artifact, version = node.get("groupId"), node.get("artifactId"), node.get("version")
        if group and artifact and version and not group.startswith("io.github.samhom.driverkit"):
            found.add((group + ":" + artifact, version))
            if node.get("type", "jar") == "jar":
                classifier = "-" + node["classifier"] if node.get("classifier") else ""
                jars.add(repository / group.replace(".", "/") / artifact / version / (artifact + "-" + version + classifier + ".jar"))
        if group and group.startswith("io.github.samhom.driverkit") and node.get("type", "jar") == "jar":
            built = root / artifact / "target" / (artifact + "-" + version + ".jar")
            if not built.resolve().is_relative_to(root):
                raise ValueError("SDK artifact must stay inside the workspace")
            if not built.is_file():
                raise ValueError("Package SDK artifacts before auditing: " + artifact)
            jars.add(built)
        for child in node.get("children", []):
            visit(child)

    for path in files:
        if not path.resolve().is_relative_to(root):
            raise ValueError("Dependency tree must stay inside the workspace")
        if path.stat().st_mtime_ns < max((root / "pom.xml").stat().st_mtime_ns, (path.parent.parent / "pom.xml").stat().st_mtime_ns):
            raise ValueError("Dependency inventory is stale; regenerate all selected module trees: " + path.parent.parent.name)
        if path.stat().st_size > 10 * 1024 * 1024:
            raise ValueError("Dependency tree is oversized")
        visit(json.loads(path.read_text(encoding="utf-8")))
    embedded = 0
    for jar in jars:
        if not jar.is_file():
            raise ValueError("Resolved dependency JAR is missing: " + jar.name)
        if jar.stat().st_size > 128 * 1024 * 1024:
            raise ValueError("Dependency JAR is oversized")
        with zipfile.ZipFile(jar) as archive:
            metadata = [entry for entry in archive.infolist() if entry.filename.endswith("/pom.properties") or entry.filename == "META-INF/io.netty.versions.properties"]
            if len(metadata) > 1024:
                raise ValueError("Too many embedded components")
            for entry in metadata:
                if entry.file_size > 65536:
                    raise ValueError("Embedded metadata is oversized")
                props = dict(line.split("=", 1) for line in archive.read(entry).decode("utf-8", errors="strict").splitlines() if "=" in line and not line.startswith("#"))
                if all(key in props for key in ("groupId", "artifactId", "version")) and not props["groupId"].startswith("io.github.samhom.driverkit"):
                    coordinate = (props["groupId"] + ":" + props["artifactId"], props["version"])
                    if coordinate not in found:
                        embedded += 1
                        found.add(coordinate)
                if entry.filename.endswith("io.netty.versions.properties"):
                    for key, version in props.items():
                        if key.endswith(".version"):
                            coordinate = ("io.netty:" + key[:-8], version)
                            if coordinate not in found:
                                embedded += 1
                                found.add(coordinate)
    if len(found) > 4096:
        raise ValueError("Too many dependencies")
    return sorted(found), modules, embedded


def audit(root: Path, repository: Path, experimental: bool, legacy: bool = False):
    packages, modules, embedded = coordinates(root, repository, experimental, legacy)
    opener = urllib.request.build_opener(NoRedirect())
    findings = []
    for offset in range(0, len(packages), 100):
        batch = packages[offset:offset + 100]
        payload = {"queries": [{"package": {"ecosystem": "Maven", "name": name}, "version": version} for name, version in batch]}
        request = urllib.request.Request("https://api.osv.dev/v1/querybatch", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}, method="POST")
        with opener.open(request, timeout=30) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
            if len(raw) > 8 * 1024 * 1024:
                raise ValueError("Audit response is oversized")
            results = json.loads(raw)["results"]
        if len(results) != len(batch):
            raise ValueError("Incomplete audit response")
        for (name, version), result in zip(batch, results):
            vulnerabilities = result.get("vulns", [])
            if vulnerabilities:
                findings.append({"package": name, "version": version, "advisories": sorted(item["id"] for item in vulnerabilities)})
    return {"source": "https://osv.dev", "modules_checked": modules, "packages_checked": len(packages), "embedded_components_detected": embedded, "experimental_profiles_included": experimental, "legacy_profiles_included": legacy, "findings": findings}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--include-experimental", action="store_true")
    parser.add_argument("--include-legacy", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    result = audit(root, (args.repository or root / ".m2-cache").resolve(), args.include_experimental, args.include_legacy)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["findings"] else 0


if __name__ == "__main__":
    sys.exit(main())
