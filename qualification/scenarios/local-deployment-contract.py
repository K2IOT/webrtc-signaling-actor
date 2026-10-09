#!/usr/bin/env python3
"""Render and check local behavior; ensure production cannot inherit its exceptions."""
import pathlib
import subprocess
import sys
import yaml

root = pathlib.Path(__file__).resolve().parents[2]
base = ["helm", "template", "--kube-version", "1.33.5", "local", str(root / "deploy/helm/signaling"),
        "-f", str(root / "deploy/helm/signaling/values-production.yaml"),
        "-f", str(root / "qualification/scenarios/fixtures/deployment-values.yaml")]
overrides = ["deploymentMode=local-minikube", "localAcknowledgement=LOCAL_TEST_ONLY",
             "gateway.replicas=1", "gateway.minAvailable=1", "control.replicas=1", "control.minAvailable=1",
             "resources.requests.cpu=100m", "resources.limits.cpu=500m",
             "resources.requests.memory=320Mi", "resources.limits.memory=320Mi",
             "javaOptions=-Xms96m -Xmx96m -XX:MaxDirectMemorySize=64m -XX:+ExitOnOutOfMemoryError -XX:ActiveProcessorCount=1"]

def render(values):
    args = base[:]
    for value in values:
        args += ["--set-string", value] if value.startswith(("javaOptions=", "resources.")) else ["--set", value]
    return subprocess.run(args, capture_output=True, text=True)

result = render(overrides)
assert result.returncode == 0, result.stderr
objects = [o for o in yaml.safe_load_all(result.stdout) if o]
def one(kind, suffix):
    found = [o for o in objects if o["kind"] == kind and o["metadata"]["name"].endswith(suffix)]
    assert len(found) == 1, (kind, suffix)
    return found[0]["spec"]

for plane, replicas, quorum in [("actor", 6, 4), ("gateway", 1, 1), ("control", 1, 1)]:
    deployment = one("Deployment", "-" + plane)
    assert deployment["replicas"] == replicas
    pod = deployment["template"]["spec"]
    assert not pod.get("topologySpreadConstraints") and not pod.get("affinity")
    assert pod["securityContext"]["runAsNonRoot"] is True
    container = pod["containers"][0]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["SIGNALING_DEPLOYMENT_MODE"] == "local-minikube"
    assert env["SIGNALING_LOCAL_ACKNOWLEDGEMENT"] == "LOCAL_TEST_ONLY"
    assert env["SIGNALING_MAX_INGRESS_PRODUCERS"] == "7"
    assert env["SIGNALING_NATIVE_FENCE_REQUIRED"] == "true"
    assert env["SIGNALING_NATIVE_PROVIDER_CONTRACT"] == "/run/signaling/provider.json"
    assert any(e["name"] == "SIGNALING_POD_UID" and e["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid" for e in container["env"])
    assert one("PodDisruptionBudget", "-" + plane)["minAvailable"] == quorum

assert {p["port"] for p in one("Service", "-gateway")["ports"]} == {8443, 9443}
gateway = one("NetworkPolicy", "-gateway")
assert any(r["ports"] == [{"protocol": "TCP", "port": 9443}] for r in gateway["ingress"])
control = one("NetworkPolicy", "-control")
assert any(r["from"][0].get("namespaceSelector", {}).get("matchLabels", {}).get("kubernetes.io/metadata.name") == "test-only-edge" for r in control["ingress"] if r["ports"] == [{"protocol": "TCP", "port": 8443}])
pg = one("NetworkPolicy", "-cell-authority")
assert any(s.get("podSelector", {}).get("matchLabels", {}).get("plane") == "control" for r in pg["ingress"] for s in r["from"])

for change in ["deploymentMode=production", "localAcknowledgement=", "localAcknowledgement=production", "deploymentMode=unknown", "actor.replicas=5", "actor.minAvailable=3", "gateway.replicas=2"]:
    key = change.split("=", 1)[0]
    changed = [v for v in overrides if v.split("=", 1)[0] != key] + [change]
    assert render(changed).returncode != 0, "Unsafe local override accepted: " + change
print("PASS: explicit LOCAL_TEST_ONLY placement/resources, actual six-actor quorum, private gateway RPC/control SQL paths; production exceptions rejected")
