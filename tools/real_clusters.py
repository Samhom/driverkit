"""Disposable, loopback-only real-server fixtures. Requires a dedicated Linux Docker runner.

Never runs against an existing installation. Credentials are generated per run and only
passed to this run's containers and Maven process. Docker output is not logged.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

IMAGES = {
    "redis": "redis:8.2.3",
    "elasticsearch": "docker.elastic.co/elasticsearch/elasticsearch:9.5.5",
    "couchbase": "couchbase:community-7.6.2",
    "rocketmq": "apache/rocketmq:5.5.0",
}


def command(args, timeout=120):
    result = subprocess.run(args, capture_output=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError("Fixture command failed: " + " ".join(args[:2]))
    if len(result.stdout) > 1024 * 1024:
        raise RuntimeError("Oversized fixture command output")
    return result.stdout.decode().strip()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http(port, path, method="GET", data=None, username=None, password=None, json_body=False):
    headers = {}
    if username:
        headers["Authorization"] = "Basic " + base64.b64encode((username + ":" + password).encode()).decode()
    if isinstance(data, dict) and not json_body:
        data = urllib.parse.urlencode(data).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif data is not None:
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method)
    with urllib.request.build_opener(NoRedirect(), urllib.request.ProxyHandler({})).open(request, timeout=3) as response:
        if len(response.read(1024 * 1024 + 1)) > 1024 * 1024:
            raise RuntimeError("Oversized fixture response")


def ready(check, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            check()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Real server did not become ready within fixture deadline")


def main():
    product = sys.argv[1] if len(sys.argv) == 2 else ""
    if product not in IMAGES or sys.platform != "linux":
        raise ValueError("Select one known product on a dedicated Linux Docker runner")
    run = secrets.token_hex(16)
    name = f"driverkit-it-{run}-{product}"
    network = f"driverkit-it-{run}"
    admin_password, password = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    environment = os.environ.copy()
    environment.update(DRIVERKIT_IT_RUN=run, DRIVERKIT_IT_PRODUCT=product,
                       DRIVERKIT_IT_PASSWORD=password, DRIVERKIT_IT_ADMIN_PASSWORD=admin_password)
    containers = []
    image = IMAGES[product]
    command(["docker", "pull", image], 300)
    digest = command(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image])
    command(["docker", "network", "create", "--internal", network])
    with tempfile.TemporaryDirectory(prefix="driverkit-it-") as temporary:
        root = Path(temporary)

        def create(container, ports, args, env=None):
            options = ["docker", "create", "--name", container, "--network", network,
                       "--label", "driverkit.it.run=" + run, "--memory", "2g", "--cpus", "2"]
            for port in ports:
                options += ["-p", f"127.0.0.1:{port}:{port}"]
            for key, value in (env or {}).items():
                # An environment variable name is passed to Docker, not the credential value.
                os.environ[key] = value
                options += ["--env", key]
            command(options + [image] + args)
            containers.append(container)

        def config(container, destination, text):
            source = root / "fixture.conf"
            source.write_text(text, encoding="utf-8")
            # The private temporary directory is 0700; the container's native UID must read the copy.
            source.chmod(0o644)
            command(["docker", "cp", str(source), container + ":" + destination])

        try:
            if product == "redis":
                create(name, [6379], ["redis-server", "/tmp/driverkit.conf"])
                config(name, "/tmp/driverkit.conf", f"bind 0.0.0.0\nappendonly yes\nuser default on >{admin_password} ~* +@all\nuser driverkit on >{password} ~* +@all\n")
                command(["docker", "start", name])
                ready(lambda: command(["docker", "exec", name, "redis-cli", "PING"], 5))
            elif product == "elasticsearch":
                create(name, [9200], ["-Ediscovery.type=single-node", "-Expack.security.enabled=true", "-Expack.security.http.ssl.enabled=false", "-Expack.security.transport.ssl.enabled=false", "-Expack.ml.enabled=false"],
                       {"ELASTIC_PASSWORD": admin_password, "ES_JAVA_OPTS": "-Xms512m -Xmx512m"})
                command(["docker", "start", name])
                ready(lambda: http(9200, "/", username="elastic", password=admin_password))
                http(9200, "/_security/role/driverkit", "PUT", {"cluster": ["monitor"], "indices": [{"names": ["driverkit-it"], "privileges": ["read", "write", "create_index"]}]}, "elastic", admin_password, json_body=True)
                http(9200, "/_security/user/driverkit", "PUT", {"password": password, "roles": ["driverkit"]}, "elastic", admin_password, json_body=True)
            elif product == "couchbase":
                create(name, [8091, 11210], [])
                command(["docker", "start", name])
                ready(lambda: http(8091, "/pools"))
                http(8091, "/node/controller/rename", "POST", {"hostname": "127.0.0.1"})
                http(8091, "/pools/default", "POST", {"memoryQuota": "256"})
                http(8091, "/node/controller/setupServices", "POST", {"services": "kv"})
                http(8091, "/settings/web", "POST", {"username": "Administrator", "password": admin_password, "port": "SAME"})
                http(8091, "/pools/default/buckets", "POST", {"name": "driverkit-it", "ramQuotaMB": "128", "replicaNumber": "0", "bucketType": "couchbase"}, "Administrator", admin_password)
                http(8091, "/settings/rbac/users/local/driverkit", "PUT", {"password": password, "roles": "data_reader[driverkit-it],data_writer[driverkit-it]"}, "Administrator", admin_password)
            else:
                namesrv = f"driverkit-it-{run}-namesrv"
                create(namesrv, [9876], ["sh", "mqnamesrv"], {"JAVA_OPT_EXT": "-Xms128m -Xmx128m -Xmn64m"})
                command(["docker", "start", namesrv])
                create(name, [10909, 10911, 10912], ["sh", "mqbroker", "-n", namesrv + ":9876", "-c", "/tmp/driverkit.conf"], {"JAVA_OPT_EXT": "-Xms256m -Xmx256m -Xmn128m"})
                config(name, "/tmp/driverkit.conf", "brokerClusterName=DriverKit\nbrokerName=driverkit\nbrokerId=0\nbrokerIP1=127.0.0.1\nlistenPort=10911\n" +
                       "autoCreateTopicEnable=true\nauthenticationEnabled=true\nauthorizationEnabled=true\n" +
                       "authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider\n" +
                       "authorizationMetadataProvider=org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider\n" +
                       "userCacheExpiredSecond=1\nuserCacheRefreshSecond=1\n" +
                       "initAuthenticationUser=" + json.dumps({"username": "administrator", "password": admin_password}) + "\n" +
                       "innerClientAuthenticationCredentials=" + json.dumps({"accessKey": "administrator", "secretKey": admin_password}) + "\n")
                command(["docker", "start", name])
            # Maven controls actual protocol readiness and exercises the same managed handles.
            args = ["mvn", "-B", "-ntp", "-s", ".mvn/settings.xml", "-Dmaven.repo.local=.m2-cache", "-Preal-clusters", "-DskipTests", "package"]
            subprocess.run(args, env=environment, check=True, timeout=600)
            subprocess.run(["mvn", "-B", "-ntp", "-s", ".mvn/settings.xml", "-Dmaven.repo.local=.m2-cache", "-Preal-clusters", "verify", "-Dtest=NoUnitTests", "-Dsurefire.failIfNoSpecifiedTests=false"], env=environment, check=True, timeout=600)
            evidence = {"product": product, "image": image, "digest": digest, "jdk": os.environ.get("JAVA_VERSION", "17"),
                        "scenarios": ["authentication-denial", "network-pause-recovery", "restart-recovery", "credential-rotation"], "result": "passed"}
            output = Path("driverkit-integration-tests/target/real-cluster-evidence.json")
            output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        finally:
            # Only the freshly-created, labelled containers are ever removed.
            for container in reversed(containers):
                try:
                    owner = command(["docker", "inspect", "--format", '{{index .Config.Labels "driverkit.it.run"}}', container])
                    if owner == run:
                        command(["docker", "rm", "-f", container])
                except Exception:
                    pass
            command(["docker", "network", "rm", network])


if __name__ == "__main__":
    main()
