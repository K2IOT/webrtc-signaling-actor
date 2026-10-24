#!/usr/bin/env python3
"""Lint of the resolved compose model (deploy/compose/compose.yaml). LOCAL_TEST_ONLY / NOT_QUALIFIED.

The model is what `docker compose --env-file topology.env --env-file .env.example -f compose.yaml
config --format json` prints, so every check runs on interpolated values. `lint(model, topology)`
returns a list of problems (empty = clean); the optional keyword arguments add the checks that need
more input: `rendered` (an offline render tree whose env.list / launch.sh the compose environment
must agree with) and `test_db_text` (provisioner/tests/test_db.sh, whose `docker run` of the
PostgreSQL servers the compose PG services must mirror).

Run as a script for the Makefile: `python3 compose_lint.py [--no-render]` prints the problems and
exits 1 when there are any, 2 when docker compose (>= 2.26) is unavailable. Python 3 stdlib only.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
COMPOSE_DIR = HERE.parent
REPO = COMPOSE_DIR.parent.parent
COMPOSE_FILE = COMPOSE_DIR / "compose.yaml"
TOPOLOGY_ENV = COMPOSE_DIR / "topology.env"
ENV_EXAMPLE = COMPOSE_DIR / ".env.example"
TEST_DB = COMPOSE_DIR / "provisioner" / "tests" / "test_db.sh"
PROJECT = "signaling-compose-local"
MIN_COMPOSE = (2, 26)

CELLS = ("c001", "c002")


def _actors_per_cell() -> int:
    """ACTORS_PER_CELL of topology.env: the one place the cell size is declared."""
    for line in TOPOLOGY_ENV.read_text().splitlines():
        m = re.fullmatch(r'ACTORS_PER_CELL="?([2-6])"?', line.strip())
        if m:
            return int(m.group(1))
    raise SystemExit("topology.env must declare ACTORS_PER_CELL (2 to 6)")


ACTORS_PER_CELL = _actors_per_cell()
ACTORS = tuple(f"actor-{c}-{n}" for c in CELLS for n in range(1, ACTORS_PER_CELL + 1))
GATEWAYS = tuple(f"gateway-{c}" for c in CELLS)
PLANES = ACTORS + GATEWAYS + ("control",)  # the plane JVMs (signaling-app)
JVM_SERVICES = PLANES + ("identity", "local-platform")
PG_PRIMARIES = tuple(f"pg-{c}-primary" for c in CELLS)
PG_STANDBYS = tuple(f"pg-{c}-standby" for c in CELLS)
PG_SERVICES = PG_PRIMARIES + PG_STANDBYS + ("pg-directory",)
KUBE_APIS = tuple(f"kube-api-{c}" for c in CELLS)
PROVISIONERS = ("provision-1", "provision-2")
EXPECTED_SERVICES = frozenset(JVM_SERVICES + PG_SERVICES + KUBE_APIS + PROVISIONERS)
LONG_RUNNING = EXPECTED_SERVICES - frozenset(PROVISIONERS)  # all but the two one-shot provisioners

# Images: own images are built from this checkout with a `:local` tag (`make build`); third-party
# images must carry a digest. Nothing may use `latest`.
OWN_IMAGE_REPOS = {
    "signaling-app",
    "signaling-identity",
    "signaling-local-platform",
    "signaling-kube-api",
    "signaling-provisioner",
}
SHA256_REF = re.compile(r"@sha256:[0-9a-f]{64}$")

# Services that do not run as 10001, with the capabilities they may add and why.
ROOT_SERVICES = {
    # The official postgres image starts as root: start.sh copies the 0440 root:10001 mount
    # files to tmpfs for uid 999, then docker-entrypoint.sh drops to postgres through gosu
    # (setuid/setgid syscalls; no setuid binary, so no-new-privileges still holds).
    **{s: {"CHOWN", "DAC_OVERRIDE", "FOWNER", "SETGID", "SETUID"} for s in PG_SERVICES},
    # Stage 1 sets root:10001 ownership on the files it writes into the run volume.
    **{s: {"CHOWN", "FOWNER", "DAC_OVERRIDE"} for s in PROVISIONERS},
}
# Not noexec: the JVMs extract native libraries (netty epoll, lz4, lmdbjava/jnr-ffi, agrona) to
# java.io.tmpdir; noexec would make them fall back silently to a different transport.
JVM_TMPFS = "/tmp:size=128m,nosuid"
RUN_TARGET = "/run/signaling"
PROVISIONER_RUN_ROOT = "/run/signaling-src"
PG_DATA_TARGET = "/var/lib/postgresql/data"

# Environment keys that must never be in compose: launch.sh reads them from the mount.
FORBIDDEN_ENV_KEYS = {
    "SIGNALING_REMOTING_KEYSTORE_PASSWORD",
    "SIGNALING_REMOTING_KEY_PASSWORD",
    "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD",
}
SECRETISH_KEY = re.compile(r"PASSWORD|PASSWD|PASSPHRASE|SECRET|TOKEN|PRIVATE|CREDENTIAL|(^|_)KEY($|_)")
SECRETISH_VALUE = re.compile(
    r"-----BEGIN|PRIVATE KEY|(password|passwd|secret|token|apikey|api_key)\s*[=:]", re.IGNORECASE
)
# The committed TEST_ONLY identity accounts (signaling-identity application.yml). Setting the
# account list through the environment replaces the whole yml list, so compose repeats both
# entries; these two values, on exactly these keys of `identity`, are the only allowed literal
# passwords in compose.
ALLOWED_LITERAL_SECRETS = {
    ("identity", "SIGNALING_IDENTITY_ACCOUNTS_0_PASSWORD"): "admin123",
    ("identity", "SIGNALING_IDENTITY_ACCOUNTS_1_PASSWORD"): "mod123",
}

PUBLISHED = {  # service -> (topology key of the host port, container port)
    "gateway-c001": ("GATEWAY_PUBLISH_C001", "RPC_PORT"),
    "gateway-c002": ("GATEWAY_PUBLISH_C002", "RPC_PORT"),
    "control": ("CONTROL_PUBLISH", "RPC_PORT"),
    "identity": ("IDENTITY_PUBLISH", 8081),
}
INTERNAL_PORTS = {25520: "Artery", 8558: "management"}
JVM_HEALTH = {"interval": "5s", "timeout": "5s", "retries": 24, "start_period": "1m30s"}


# --- inputs ----------------------------------------------------------------------------------


def parse_topology(path: Path = TOPOLOGY_ENV) -> dict[str, str]:
    """KEY=value lines of topology.env (optional double quotes, comments, no expansion)."""
    out: dict[str, str] = {}
    for n, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.fullmatch(r"([A-Z_][A-Z0-9_]*)=(.*)", line)
        if not m:
            raise ValueError(f"{path.name}:{n}: not KEY=value")
        key, value = m.groups()
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1]
        if "$" in value or "`" in value:
            raise ValueError(f"{path.name}:{n}: {key} must be a literal")
        out[key] = value
    return out


def compose_version() -> tuple[int, ...] | None:
    if shutil.which("docker") is None:
        return None
    try:
        res = subprocess.run(
            ["docker", "compose", "version", "--short"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.match(r"v?(\d+)\.(\d+)", res.stdout.strip())
    if res.returncode != 0 or not m:
        return None
    return tuple(int(x) for x in m.groups())


def compose_unavailable_reason() -> str | None:
    v = compose_version()
    if v is None:
        return "docker compose is not available"
    if v < MIN_COMPOSE:
        return f"docker compose {'.'.join(map(str, v))} is older than 2.26 (volume subpath)"
    return None


def load_model(env_files=(TOPOLOGY_ENV, ENV_EXAMPLE), compose_file: Path = COMPOSE_FILE) -> dict:
    cmd = ["docker", "compose"]
    for f in env_files:
        cmd += ["--env-file", str(f)]
    cmd += ["-f", str(compose_file), "config", "--format", "json"]
    # The process environment must not leak into interpolation: compose prefers it over env files.
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONFIG")}
    res = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=120)
    if res.returncode != 0:
        raise RuntimeError(f"docker compose config failed: {res.stderr.strip()}")
    return json.loads(res.stdout)


def render_offline(workdir: Path) -> Path:
    """Stage 1 + offline render into workdir/run (in-container paths /run/signaling/...)."""
    run_root = workdir / "run"
    env = dict(os.environ)
    for k in list(env):
        if k.startswith(("PROVISION_", "RENDER_")) or k in ("RUN_ROOT", "TOPOLOGY_ENV", "JVM_OPTS"):
            del env[k]
    env.update(
        RUN_ROOT=str(run_root),
        IDENTITY_PUBLIC_KEY=str(REPO / "signaling-identity/src/main/resources/keys/public.pem"),
        PRODUCTION_DEFAULTS=str(REPO / "config/production-defaults.yaml"),
    )
    prov = COMPOSE_DIR / "provisioner"
    for cmd in (["bash", str(prov / "provision.sh"), "stage1"], ["bash", str(prov / "lib/render.sh"), "render", "--offline"]):
        res = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=600)
        if res.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd[1:])} failed: {res.stderr.strip()[-2000:]}")
    return run_root


def render_tools_missing() -> list[str]:
    return [t for t in ("bash", "openssl", "keytool", "jq") if shutil.which(t) is None]


# --- helpers ---------------------------------------------------------------------------------


def _services(model: dict) -> dict:
    return model.get("services") or {}


def _upper(cell: str) -> str:
    return cell.upper()


def _cell(service: str) -> str | None:
    m = re.search(r"(c00[12])", service)
    return m.group(1) if m else None


def _unescape(s: str) -> str:
    return s.replace("$$", "$")


def expected_networks(t: dict[str, str]) -> dict[str, dict[str, str | None]]:
    """service -> {network: static address (None = dynamic)}; provision-1 has no network."""
    m: dict[str, dict[str, str | None]] = {}
    for c in CELLS:
        C = _upper(c)
        for n in range(1, ACTORS_PER_CELL + 1):
            m[f"actor-{c}-{n}"] = {f"net-{c}": t[f"ACTOR_{C}_{n}_CELL_IP"], "net-shared": t[f"ACTOR_{C}_{n}_SHARED_IP"]}
        m[f"gateway-{c}"] = {"net-shared": t[f"GATEWAY_{C}_IP"]}
        m[f"pg-{c}-primary"] = {f"net-{c}": t[f"PG_{C}_PRIMARY_IP"]}
        m[f"pg-{c}-standby"] = {f"net-{c}": t[f"PG_{C}_STANDBY_IP"]}
        m[f"kube-api-{c}"] = {f"net-{c}": t[f"KUBE_{C}_IP"]}
    m["control"] = {"net-c001": t["CONTROL_C001_IP"], "net-c002": t["CONTROL_C002_IP"], "net-shared": t["CONTROL_IP"]}
    m["identity"] = {"net-shared": t["IDENTITY_IP"]}
    m["local-platform"] = {"net-shared": t["PLATFORM_IP"]}
    m["pg-directory"] = {"net-shared": t["PG_DIRECTORY_IP"]}
    m["provision-1"] = {}
    m["provision-2"] = {"net-c001": None, "net-c002": None, "net-shared": None}
    return m


def health_address(service: str, t: dict[str, str]) -> tuple[str, str, str]:
    """(ip, port, path) the healthcheck of a long-running non-PG service must probe."""
    c = _cell(service)
    if service.startswith("actor-"):
        n = service.rsplit("-", 1)[1]
        return t[f"ACTOR_{_upper(c)}_{n}_CELL_IP"], t["HEALTH_PORT"], "/ready"
    if service.startswith("gateway-"):
        return t[f"GATEWAY_{_upper(c)}_IP"], t["HEALTH_PORT"], "/ready"
    if service.startswith("kube-api-"):
        return t[f"KUBE_{_upper(c)}_IP"], t["HEALTH_PORT"], "/healthz"
    return {
        "control": (t["CONTROL_IP"], t["HEALTH_PORT"], "/ready"),
        "local-platform": (t["PLATFORM_IP"], t["HEALTH_PORT"], "/ready"),
        "identity": (t["IDENTITY_IP"], "8081", "/.well-known/jwks.json"),
    }[service]


def _all_strings(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _all_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _all_strings(v)


def _deps(svc: dict) -> dict[str, str]:
    return {k: (v or {}).get("condition", "service_started") for k, v in (svc.get("depends_on") or {}).items()}


def _image_repo(image: str) -> str:
    ref = image.split("@", 1)[0]
    last = ref.rsplit("/", 1)[-1]
    return ref[: len(ref) - len(last)] + last.split(":", 1)[0]


# --- checks ----------------------------------------------------------------------------------


def check_service_set(model: dict) -> list[str]:
    names = set(_services(model))
    p = []
    for s in sorted(EXPECTED_SERVICES - names):
        p.append(f"service set: {s} is missing")
    for s in sorted(names - EXPECTED_SERVICES):
        p.append(f"service set: {s} is not one of the expected services")
    if model.get("name") != PROJECT:
        p.append(f"project name must be {PROJECT}, got {model.get('name')!r}")
    return p


def check_hardening(model: dict) -> list[str]:
    p = []
    for name, svc in sorted(_services(model).items()):
        if svc.get("read_only") is not True:
            p.append(f"{name}: read_only must be true")
        if svc.get("cap_drop") != ["ALL"]:
            p.append(f"{name}: cap_drop must be [ALL]")
        if not any(o in ("no-new-privileges:true", "no-new-privileges") for o in svc.get("security_opt") or []):
            p.append(f"{name}: security_opt must contain no-new-privileges:true")
        tmpfs = svc.get("tmpfs") or []
        if not any(t == "/tmp" or t.startswith("/tmp:") for t in tmpfs):
            p.append(f"{name}: needs a tmpfs at /tmp")
        for key in ("privileged", "pid", "ipc", "userns_mode", "devices"):
            if svc.get(key):
                p.append(f"{name}: {key} is not allowed")
        if svc.get("network_mode") not in (None, "none"):
            p.append(f"{name}: network_mode {svc.get('network_mode')!r} is not allowed")
        caps = set(svc.get("cap_add") or [])
        if name in ROOT_SERVICES:
            if svc.get("user") not in (None, "0", "0:0", "root"):
                p.append(f"{name}: expected to start as root, got user {svc.get('user')!r}")
            if not caps <= ROOT_SERVICES[name]:
                p.append(f"{name}: cap_add {sorted(caps - ROOT_SERVICES[name])} beyond the allowed {sorted(ROOT_SERVICES[name])}")
        else:
            if svc.get("user") != "10001:10001":
                p.append(f"{name}: user must be 10001:10001, got {svc.get('user')!r}")
            if caps:
                p.append(f"{name}: no cap_add allowed, got {sorted(caps)}")
            if JVM_TMPFS not in tmpfs:
                p.append(f"{name}: tmpfs must be {JVM_TMPFS}")
    return p


def check_images(model: dict) -> list[str]:
    expected = {
        **{s: "signaling-app" for s in PLANES},
        "identity": "signaling-identity",
        "local-platform": "signaling-local-platform",
        **{s: "signaling-kube-api" for s in KUBE_APIS},
        **{s: "signaling-provisioner" for s in PROVISIONERS},
        **{s: "postgres" for s in PG_SERVICES},
    }
    p = []
    for name, svc in sorted(_services(model).items()):
        image = svc.get("image") or ""
        repo = _image_repo(image)
        tag = image.split("@", 1)[0][len(repo) + 1 :] if image.split("@", 1)[0] != repo else ""
        if not image:
            p.append(f"{name}: no image")
            continue
        if tag == "latest":
            p.append(f"{name}: image {image} uses latest")
        if repo in OWN_IMAGE_REPOS:
            if tag != "local" and not SHA256_REF.search(image):
                p.append(f"{name}: own image {image} must be :local (built here) or digest pinned")
        elif not SHA256_REF.search(image):
            p.append(f"{name}: third-party image {image} lacks an @sha256 digest")
        if name in expected and repo != expected[name]:
            p.append(f"{name}: image {image} is not {expected[name]}")
    return p


def check_secrets(model: dict) -> list[str]:
    p = []
    for name, svc in sorted(_services(model).items()):
        env = svc.get("environment") or {}
        for key, value in env.items():
            value = "" if value is None else str(value)
            if key in FORBIDDEN_ENV_KEYS:
                p.append(f"{name}: {key} must come from the mount through launch.sh, never compose")
                continue
            if SECRETISH_KEY.search(key):
                allowed = ALLOWED_LITERAL_SECRETS.get((name, key))
                is_file_ref = value.startswith(RUN_TARGET + "/") and "\n" not in value
                if not (is_file_ref or (allowed is not None and value == allowed)):
                    p.append(f"{name}: environment {key} looks like a secret (only file references under {RUN_TARGET}/ allowed)")
        for s in _all_strings({k: v for k, v in svc.items() if k != "environment"}):
            if SECRETISH_VALUE.search(_unescape(s)):
                p.append(f"{name}: a command/entrypoint/healthcheck string looks like it carries a secret")
        for value in env.values():
            if value is not None and SECRETISH_VALUE.search(str(value)):
                p.append(f"{name}: an environment value looks like a secret")
    return p


def check_networks(model: dict, t: dict[str, str]) -> list[str]:
    nets = model.get("networks") or {}
    p = []
    want = {
        "net-c001": (t["NET_C001"], t["NET_C001_DYNAMIC"], True),
        "net-c002": (t["NET_C002"], t["NET_C002_DYNAMIC"], True),
        "net-shared": (t["NET_SHARED"], t["NET_SHARED_DYNAMIC"], False),
    }
    if set(nets) != set(want):
        p.append(f"networks must be exactly {sorted(want)}, got {sorted(nets)}")
    for name, (subnet, dyn, internal) in want.items():
        n = nets.get(name) or {}
        if n.get("name") != f"{PROJECT}_{name}":
            p.append(f"network {name}: name must be {PROJECT}_{name}")
        if bool(n.get("internal")) is not internal:
            p.append(f"network {name}: internal must be {str(internal).lower()}")
        if n.get("external"):
            p.append(f"network {name}: must not be external")
        cfg = ((n.get("ipam") or {}).get("config")) or []
        if len(cfg) != 1 or cfg[0].get("subnet") != subnet or cfg[0].get("ip_range") != dyn:
            p.append(f"network {name}: ipam must be exactly subnet {subnet} with ip_range {dyn}")
        if name == "net-shared" and cfg and cfg[0].get("gateway") != t["NET_SHARED_GATEWAY"]:
            p.append(f"network {name}: ipam gateway must be {t['NET_SHARED_GATEWAY']} (NET_SHARED_GATEWAY)")
        if cfg and cfg[0].get("ip_range") and cfg[0].get("subnet"):
            try:
                if not ipaddress.ip_network(cfg[0]["ip_range"]).subnet_of(ipaddress.ip_network(cfg[0]["subnet"])):
                    p.append(f"network {name}: ip_range outside the subnet")
            except ValueError:
                p.append(f"network {name}: unparsable ipam values")
    return p


def check_platform_peer_names(model: dict, t: dict[str, str]) -> list[str]:
    """local-platform's JDK HttpsServer reverse-resolves every TLS peer: the host gateway needs a
    hosts entry, or each host-side handshake waits for Docker's DNS to time out."""
    gateway = t["NET_SHARED_GATEWAY"]
    hosts = _services(model).get("local-platform", {}).get("extra_hosts") or []
    if isinstance(hosts, dict):
        entries = {(k, v if isinstance(v, str) else (v or [""])[0]) for k, v in hosts.items()}
    else:
        entries = {tuple(e.replace("=", ":", 1).split(":", 1)) for e in hosts}
    if not any(ip == gateway for _, ip in entries):
        return [f"local-platform: extra_hosts must map a name to the net-shared gateway {gateway}"]
    return []


def check_addresses(model: dict, t: dict[str, str]) -> list[str]:
    p = []
    expected = expected_networks(t)
    subnets = {"net-c001": t["NET_C001"], "net-c002": t["NET_C002"], "net-shared": t["NET_SHARED"]}
    dynamic = {"net-c001": t["NET_C001_DYNAMIC"], "net-c002": t["NET_C002_DYNAMIC"], "net-shared": t["NET_SHARED_DYNAMIC"]}
    topology_ips = {v for k, v in t.items() if k.endswith("_IP")}
    for name, svc in sorted(_services(model).items()):
        nets = svc.get("networks") or {}
        want = expected.get(name)
        if want is None:
            continue
        if name == "provision-1":
            if nets or svc.get("network_mode") != "none":
                p.append("provision-1: must have network_mode none (stage 1 needs no network)")
            continue
        if set(nets) != set(want):
            p.append(f"{name}: networks must be {sorted(want)}, got {sorted(nets)}")
        for net, ip in want.items():
            got = (nets.get(net) or {}).get("ipv4_address")
            if ip is None:
                if got:
                    p.append(f"{name}: {net} must use a dynamic address, got {got}")
                continue
            if got != ip:
                p.append(f"{name}: {net} address must be {ip} (topology.env), got {got}")
        for net, cfg in nets.items():
            got = (cfg or {}).get("ipv4_address")
            if not got:
                continue
            if got not in topology_ips:
                p.append(f"{name}: {got} is not an address of topology.env")
            if net in subnets:
                ip = ipaddress.ip_address(got)
                if ip not in ipaddress.ip_network(subnets[net]):
                    p.append(f"{name}: {got} is outside {net} {subnets[net]}")
                if ip in ipaddress.ip_network(dynamic[net]):
                    p.append(f"{name}: {got} lies inside the dynamic range of {net}")
    return p


def check_actor_cell_isolation(model: dict) -> list[str]:
    p = []
    svcs = _services(model)
    for a in ACTORS:
        own = f"net-{_cell(a)}"
        other = "net-c002" if own == "net-c001" else "net-c001"
        nets = set((svcs.get(a) or {}).get("networks") or {})
        if other in nets:
            p.append(f"{a}: joins {other}, the other cell's network")
        if own not in nets:
            p.append(f"{a}: is not on its cell network {own}")
    for s in GATEWAYS + ("identity", "local-platform", "pg-directory"):
        nets = set((svcs.get(s) or {}).get("networks") or {})
        if nets & {"net-c001", "net-c002"}:
            p.append(f"{s}: must not join a cell network")
    return p


def check_internal_ports(model: dict, t: dict[str, str]) -> list[str]:
    """Artery 25520 and management 8558 are neither published nor exposed; their bind address
    (the actor podIp = cell-net address) is not on net-shared."""
    p = []
    shared = ipaddress.ip_network(t["NET_SHARED"])
    for name, svc in sorted(_services(model).items()):
        for port in svc.get("ports") or []:
            if int(port.get("target", 0)) in INTERNAL_PORTS or str(port.get("published")) in map(str, INTERNAL_PORTS):
                p.append(f"{name}: publishes the {INTERNAL_PORTS.get(int(port.get('target', 0)), 'internal')} port")
        for e in svc.get("expose") or []:
            if int(str(e).split("/")[0].split("-")[0]) in INTERNAL_PORTS:
                p.append(f"{name}: exposes internal port {e}")
    for a in ACTORS:
        ip = t[f"ACTOR_{_upper(_cell(a))}_{a.rsplit('-', 1)[1]}_CELL_IP"]
        if ipaddress.ip_address(ip) in shared:
            p.append(f"{a}: Artery/management address {ip} is on net-shared")
    return p


def check_ports(model: dict, t: dict[str, str]) -> list[str]:
    p = []
    for name, svc in sorted(_services(model).items()):
        ports = svc.get("ports") or []
        if name not in PUBLISHED:
            if ports:
                p.append(f"{name}: publishes ports; only gateways, control and identity may")
            continue
        host_key, target = PUBLISHED[name]
        target = int(t[target]) if isinstance(target, str) else target
        want = {"host_ip": "127.0.0.1", "published": t[host_key], "target": target, "protocol": "tcp"}
        got = [{k: (str(x.get(k)) if k == "published" else x.get(k)) for k in want} for x in ports]
        if got != [want]:
            p.append(f"{name}: ports must be exactly 127.0.0.1:{t[host_key]}->{target}/tcp, got {got}")
    if t.get("PUBLISH_HOST") != "127.0.0.1":
        p.append("topology PUBLISH_HOST must be 127.0.0.1 (the /etc/hosts names point there)")
    return p


def check_start_order(model: dict) -> list[str]:
    svcs = _services(model)
    done, healthy = "service_completed_successfully", "service_healthy"
    want: dict[str, dict[str, str]] = {"provision-1": {}}
    for s in PG_SERVICES:
        want[s] = {"provision-1": done}
    for c in CELLS:
        want[f"pg-{c}-standby"][f"pg-{c}-primary"] = healthy
    want["provision-2"] = {"provision-1": done, **{s: healthy for s in PG_SERVICES}}
    for s in KUBE_APIS + ("local-platform", "identity"):
        want[s] = {"provision-2": done}
    # Actors deliberately do NOT depend on identity: they never call it (the JWT public keys are
    # static in the contract mount). Deviation from the Task 7 ruling, accepted in review.
    for a in ACTORS:
        want[a] = {"provision-2": done, f"kube-api-{_cell(a)}": healthy, "local-platform": healthy}
    for c in CELLS:
        want[f"gateway-{c}"] = {
            "provision-2": done,
            "local-platform": healthy,
            **{f"actor-{c}-{n}": healthy for n in range(1, ACTORS_PER_CELL + 1)},
        }
    want["control"] = {"provision-2": done, "local-platform": healthy, **{a: healthy for a in ACTORS}}
    p = []
    for name in sorted(want):
        if name not in svcs:
            continue
        got = _deps(svcs[name])
        if got != want[name]:
            missing = {k: v for k, v in want[name].items() if got.get(k) != v}
            extra = {k: v for k, v in got.items() if k not in want[name]}
            p.append(f"{name}: depends_on differs from spec 6 (missing/wrong {missing}, unexpected {extra})")
        for dep, cfg in (svcs[name].get("depends_on") or {}).items():
            if (cfg or {}).get("required") is False:
                p.append(f"{name}: dependency on {dep} must be required")
    for a in ACTORS:
        if a in svcs and set(_deps(svcs[a])) & set(ACTORS):
            p.append(f"{a}: actors must not depend on each other (they start concurrently, spec 4.7)")
    return p


def check_restart(model: dict) -> list[str]:
    p = []
    for name, svc in sorted(_services(model).items()):
        r = svc.get("restart")
        want = "no" if name in GATEWAYS + PROVISIONERS else "on-failure:5"
        if r != want:
            why = " (gateway boot id is pinned per provision)" if name in GATEWAYS else ""
            p.append(f"{name}: restart must be {want!r}{why}, got {r!r}")
    for name in PLANES:
        svc = _services(model).get(name)
        if svc and svc.get("stop_grace_period") != "1m30s":
            p.append(f"{name}: stop_grace_period must be 90s (termination grace)")
    return p


def check_healthchecks(model: dict, t: dict[str, str]) -> list[str]:
    p = []
    for name, svc in sorted(_services(model).items()):
        hc = svc.get("healthcheck")
        if name in PROVISIONERS:
            if hc and not hc.get("disable"):
                p.append(f"{name}: one-shot service must not have a healthcheck")
            continue
        if not hc or hc.get("disable"):
            p.append(f"{name}: needs a healthcheck")
            continue
        test = [_unescape(x) for x in hc.get("test") or []]
        joined = " ".join(test)
        for bad in ("0.0.0.0", "localhost", "127.0.0.1"):
            if bad in joined:
                p.append(f"{name}: healthcheck uses {bad} instead of the service's concrete address")
        for k, v in JVM_HEALTH.items():
            if hc.get(k) != v:
                p.append(f"{name}: healthcheck {k} must be {v}, got {hc.get(k)!r}")
        if name in PG_SERVICES:
            ip = next(iter(expected_networks(t)[name].values()))
            if test[:2] != ["CMD", "pg_isready"] or "-h" not in test or test[test.index("-h") + 1] != ip:
                p.append(f"{name}: healthcheck must be pg_isready -h {ip}")
            elif "-p" not in test or test[test.index("-p") + 1] != t["PG_PORT"]:
                p.append(f"{name}: healthcheck must probe port {t['PG_PORT']}")
            continue
        ip, port, path = health_address(name, t)
        if len(test) != 8 or test[:3] != ["CMD", "bash", "-c"]:
            p.append(f"{name}: healthcheck must be [CMD, bash, -c, <probe>, <name>, ip, port, path]")
            continue
        script, args = test[3], test[5:]
        if args != [ip, port, path]:
            p.append(f"{name}: healthcheck must probe {ip}:{port}{path}, got {args}")
        if "/dev/tcp/$1/$2" not in script or '" 200"' not in script or "GET %s HTTP/1.0" not in script:
            p.append(f"{name}: healthcheck probe must open /dev/tcp/<ip>/<port>, send GET and require status 200")
    return p


def check_volumes(model: dict) -> list[str]:
    p = []
    vols = model.get("volumes") or {}
    if (vols.get("run") or {}).get("name") != f"{PROJECT}_run":
        p.append(f"volume run must be named {PROJECT}_run")
    for name, svc in sorted(_services(model).items()):
        mounts = svc.get("volumes") or []
        for m in mounts:
            if m.get("type") != "volume":
                p.append(f"{name}: only named volumes allowed, got a {m.get('type')} mount at {m.get('target')}")
        run = [m for m in mounts if m.get("source") == "run"]
        others = [m for m in mounts if m.get("source") != "run"]
        if name in PROVISIONERS:
            ok = (
                len(run) == 1
                and run[0].get("target") == PROVISIONER_RUN_ROOT
                and not run[0].get("read_only")
                and not (run[0].get("volume") or {}).get("subpath")
            )
            if not ok or others:
                p.append(f"{name}: must mount exactly the whole run volume read-write at {PROVISIONER_RUN_ROOT}")
            if (svc.get("environment") or {}).get("RUN_ROOT") != PROVISIONER_RUN_ROOT:
                p.append(f"{name}: RUN_ROOT must be {PROVISIONER_RUN_ROOT}")
            continue
        if name == "identity":
            if mounts:
                p.append("identity: has no stage-1 files and mounts nothing")
            continue
        want = {"type": "volume", "source": "run", "target": RUN_TARGET, "read_only": True, "volume": {"subpath": name}}
        if len(run) != 1 or {k: run[0].get(k) for k in want} != want:
            p.append(f"{name}: run volume must be mounted read-only at {RUN_TARGET} with subpath {name}")
        if name in PG_SERVICES:
            data = f"pgdata-{name}"
            if len(others) != 1 or others[0].get("source") != data or others[0].get("target") != PG_DATA_TARGET or others[0].get("read_only"):
                p.append(f"{name}: needs exactly its own volume {data} at {PG_DATA_TARGET}")
            elif (vols.get(data) or {}).get("name") != f"{PROJECT}_{data}":
                p.append(f"volume {data} must be named {PROJECT}_{data}")
        elif others:
            p.append(f"{name}: unexpected extra mounts {[m.get('target') for m in others]}")
    return p


def check_no_shared_or_ca(model: dict) -> list[str]:
    p = []
    for name, svc in sorted(_services(model).items()):
        for m in svc.get("volumes") or []:
            sub = ((m.get("volume") or {}).get("subpath")) or ""
            parts = [x for x in sub.split("/") if x]
            if any(x in ("_shared", ".ca", "..") for x in parts):
                p.append(f"{name}: mounts {sub} of the run volume (provisioner-only material)")
            if m.get("source") == "run" and not parts and name not in PROVISIONERS:
                p.append(f"{name}: mounts the whole run volume (includes _shared and .ca)")
    return p


def check_entrypoints(model: dict) -> list[str]:
    p = []
    svcs = _services(model)
    for name in PLANES:
        if name in svcs and svcs[name].get("entrypoint") != ["bash", f"{RUN_TARGET}/launch.sh"]:
            p.append(f"{name}: entrypoint must be [bash, {RUN_TARGET}/launch.sh]")
    for name in PG_SERVICES:
        if name in svcs and svcs[name].get("entrypoint") != ["bash", f"{RUN_TARGET}/start.sh"]:
            p.append(f"{name}: entrypoint must be [bash, {RUN_TARGET}/start.sh]")
    for name, stage in (("provision-1", "stage1"), ("provision-2", "stage2")):
        if name in svcs:
            svc = svcs[name]
            if svc.get("entrypoint") or svc.get("command") != ["provision.sh", stage, "--if-needed"]:
                p.append(f"{name}: command must be [provision.sh, {stage}, --if-needed] with the image entrypoint")
            if "PROVISION_FORCE" in (svc.get("environment") or {}):
                p.append(f"{name}: compose never sets PROVISION_FORCE (only make reset)")
    return p


def expected_env(service: str, t: dict[str, str]) -> dict[str, str]:
    """The compose environment of the JVM services, from topology.env only."""
    if service == "local-platform":
        return {
            "SIGNALING_LOCAL_ACKNOWLEDGEMENT": "LOCAL_TEST_ONLY",
            "SIGNALING_IDENTITY_ISSUER": t["ISSUER"],
            "SIGNALING_PLATFORM_CONFIG": f"{RUN_TARGET}/platform.json",
        }
    if service == "identity":
        return {
            "SIGNALING_IDENTITY_ISSUER": t["ISSUER"],
            "SIGNALING_IDENTITY_AUDIENCE": t["AUDIENCE"],
            "SIGNALING_IDENTITY_ACCOUNTS_0_USERNAME": "admin",
            "SIGNALING_IDENTITY_ACCOUNTS_0_PASSWORD": "admin123",
            "SIGNALING_IDENTITY_ACCOUNTS_0_USER_ID": t["ACCOUNT_ADMIN_USER_ID"],
            "SIGNALING_IDENTITY_ACCOUNTS_1_USERNAME": "mod",
            "SIGNALING_IDENTITY_ACCOUNTS_1_PASSWORD": "mod123",
            "SIGNALING_IDENTITY_ACCOUNTS_1_USER_ID": t["ACCOUNT_MOD_USER_ID"],
        }
    plane = service.split("-", 1)[0]
    env = {
        "SPRING_PROFILES_ACTIVE": plane,
        "SIGNALING_DEPLOYMENT_MODE": "local-minikube",
        "SIGNALING_LOCAL_ACKNOWLEDGEMENT": "LOCAL_TEST_ONLY",
        "SIGNALING_NATIVE_PROVIDER_CONTRACT": f"{RUN_TARGET}/provider.json",
        "SIGNALING_LOCAL_PROCESS_PROFILE": f"{RUN_TARGET}/process.json",
        "SIGNALING_RUNTIME_CONTRACT": f"{RUN_TARGET}/runtime.yaml",
        "SIGNALING_CELL_ID": _cell(service) or "c001",
        "SIGNALING_CLUSTER_FINGERPRINT": t["CLUSTER_FINGERPRINT"],
        "SIGNALING_LOCAL_CELL_ACTORS": t["ACTORS_PER_CELL"],
        "SIGNALING_IDENTITY_ISSUER": t["ISSUER"],
        "SIGNALING_IDENTITY_AUDIENCE": t["AUDIENCE"],
    }
    if plane == "actor":
        env["SIGNALING_FORMATION_PROFILE"] = "initial-six"
    return env


# JVM sizing: planes read JVM_OPTS in launch.sh; identity/local-platform keep their image
# entrypoint (java -jar), whose launcher reads JDK_JAVA_OPTIONS.
JVM_OPTION_KEY = {**{s: "JVM_OPTS" for s in PLANES}, "identity": "JDK_JAVA_OPTIONS", "local-platform": "JDK_JAVA_OPTIONS"}


def env_example_jvm_opts(path: Path = ENV_EXAMPLE) -> str:
    return parse_topology(path)["JVM_OPTS"]


def check_environment(model: dict, t: dict[str, str], jvm_opts: str | None = None) -> list[str]:
    """jvm_opts: the JVM_OPTS of .env.example (read from it when None)."""
    jvm_opts = env_example_jvm_opts() if jvm_opts is None else jvm_opts
    p = []
    svcs = _services(model)
    for name in JVM_SERVICES:
        if name not in svcs:
            continue
        env = {k: ("" if v is None else str(v)) for k, v in (svcs[name].get("environment") or {}).items()}
        want = expected_env(name, t)
        opt = JVM_OPTION_KEY[name]
        if env.get(opt) != jvm_opts:
            p.append(f"{name}: {opt} must be JVM_OPTS of .env.example ({jvm_opts!r}), got {env.get(opt)!r}")
        rest = {k: v for k, v in env.items() if k != opt}
        if rest != want:
            diff = sorted(set(rest.items()) ^ set(want.items()))
            p.append(f"{name}: environment differs from the topology-derived one: {diff}")
    return p


def _launch_exports(text: str) -> set[str]:
    out = set()
    for line in text.splitlines():
        m = re.match(r"\s*export\s+([A-Z_][A-Z0-9_]*)", line) or re.match(r"\s*([A-Z_][A-Z0-9_]*)=", line)
        if m:
            out.add(m.group(1))
    return out


def check_kube_api_env(model: dict, t: dict[str, str]) -> list[str]:
    """kube-api-<cell> serves exactly its own cell: a c001/c002 swap of any value fails."""
    p = []
    svcs = _services(model)
    for c in CELLS:
        name, C = f"kube-api-{c}", _upper(c)
        if name not in svcs:
            continue
        want = {
            "KUBE_CELL": c,
            "KUBE_NAMESPACE": t[f"KUBE_{C}_NAMESPACE"],
            "KUBE_POD_IPS": ",".join(t[f"ACTOR_{C}_{n}_CELL_IP"] for n in range(1, ACTORS_PER_CELL + 1)),
            "KUBE_TOKEN_FILE": f"{RUN_TARGET}/kube-token",
            "KUBE_TLS_CERT": f"{RUN_TARGET}/kube-api.crt",
            "KUBE_TLS_KEY": f"{RUN_TARGET}/kube-api.key",
            "KUBE_LISTEN": t[f"KUBE_{C}_IP"],
            "KUBE_HEALTH_LISTEN": t[f"KUBE_{C}_IP"],
            "KUBE_PORT": t["KUBE_API_PORT"],
            "KUBE_HEALTH_PORT": t["HEALTH_PORT"],
        }
        env = {k: ("" if v is None else str(v)) for k, v in (svcs[name].get("environment") or {}).items()}
        if env != want:
            diff = sorted(set(env.items()) ^ set(want.items()))
            p.append(f"{name}: environment differs from the cell's topology values: {diff}")
    return p


def check_rendered_env(model: dict, rendered: Path) -> list[str]:
    """Each key of a service's rendered env.list is either set by compose to the same value or
    exported by its launch.sh (and then absent from compose); compose sets nothing else."""
    p = []
    svcs = _services(model)
    for name in PLANES + ("local-platform",):
        env_list = rendered / name / "env.list"
        if not env_list.is_file():
            p.append(f"{name}: rendered env.list missing")
            continue
        listed = dict(line.split("=", 1) for line in env_list.read_text().splitlines() if line)
        launch = rendered / name / "launch.sh"
        exported = _launch_exports(launch.read_text()) if launch.is_file() else set()
        env = {k: ("" if v is None else str(v)) for k, v in ((svcs.get(name) or {}).get("environment") or {}).items()}
        for key, value in sorted(listed.items()):
            if key in exported:
                if key in env:
                    p.append(f"{name}: {key} is exported by launch.sh; compose must not set it")
            elif env.get(key) != value:
                p.append(f"{name}: {key}={env.get(key)!r} in compose, {value!r} in the rendered env.list")
        for key in sorted(set(env) - set(listed) - {JVM_OPTION_KEY[name]}):
            p.append(f"{name}: compose sets {key}, which the renderer's env.list does not know")
    return p


def parse_test_db_pg_run(text: str) -> dict:
    """The `docker run` flags of start_pg() in provisioner/tests/test_db.sh."""
    m = re.search(r"^start_pg\(\) \{\n(.*?)^\}", text, re.S | re.M)
    if not m:
        raise ValueError("start_pg() not found in test_db.sh")
    body = m.group(1)
    init = re.search(r"init_tmpfs=\(--tmpfs (\S+)\)", body)
    run = body[body.index("docker run") :].replace("\\\n", " ")
    caps_add = re.findall(r"--cap-add (\S+)", run)
    return {
        "tmpfs": re.findall(r"--tmpfs (\S+)", run),
        "init_tmpfs": init.group(1) if init else None,
        "standby_skips_init_tmpfs": bool(re.search(r"\*-standby\) ;;", body)),
        "read_only": "--read-only" in run,
        "cap_drop": re.findall(r"--cap-drop (\S+)", run),
        "cap_add": caps_add,
        "security_opt": re.findall(r"--security-opt (\S+)", run),
        "env": dict(x.split("=", 1) for x in re.findall(r"-e (\S+=\S+)", run)),
        "entrypoint": [re.search(r"--entrypoint (\S+)", run).group(1), re.search(r'"\$PG_IMAGE" (\S+)', run).group(1)],
        "run_mount": re.search(r"dst=(/run/signaling),volume-subpath=\$s,readonly", run) is not None,
        "data_target": re.search(r"src=\$PREFIX-data-\$s,dst=(\S+?)\"", run).group(1),
    }


def check_pg_matches_test_db(model: dict, test_db_text: str) -> list[str]:
    ref = parse_test_db_pg_run(test_db_text)
    p = []
    norm = lambda opts: sorted(o if ":" in o else f"{o}:true" for o in opts)  # noqa: E731
    for name in PG_SERVICES:
        svc = _services(model).get(name)
        if not svc:
            continue
        tmpfs = list(ref["tmpfs"])
        if ref["init_tmpfs"] and not (name.endswith("-standby") and ref["standby_skips_init_tmpfs"]):
            tmpfs.append(ref["init_tmpfs"])
        checks = {
            "tmpfs": (sorted(svc.get("tmpfs") or []), sorted(tmpfs)),
            "read_only": (svc.get("read_only") is True, ref["read_only"]),
            "cap_drop": (svc.get("cap_drop"), ref["cap_drop"]),
            "cap_add": (sorted(svc.get("cap_add") or []), sorted(ref["cap_add"])),
            "security_opt": (norm(svc.get("security_opt") or []), norm(ref["security_opt"])),
            "environment": ({k: str(v) for k, v in (svc.get("environment") or {}).items()}, ref["env"]),
            "entrypoint": (svc.get("entrypoint"), ref["entrypoint"]),
        }
        for what, (got, want) in checks.items():
            if got != want:
                p.append(f"{name}: {what} {got} differs from test_db.sh's docker run {want}")
        if not ref["run_mount"]:
            p.append("test_db.sh: run volume mount no longer at /run/signaling subpath readonly; update compose and lint")
        if not any(m.get("target") == ref["data_target"] for m in svc.get("volumes") or []):
            p.append(f"{name}: data volume target differs from test_db.sh ({ref['data_target']})")
    return p


ALL_CHECKS = (
    check_service_set,
    check_hardening,
    check_images,
    check_secrets,
    check_volumes,
    check_no_shared_or_ca,
    check_actor_cell_isolation,
    check_start_order,
    check_restart,
    check_entrypoints,
)
TOPOLOGY_CHECKS = (
    check_networks,
    check_platform_peer_names,
    check_addresses,
    check_internal_ports,
    check_ports,
    check_healthchecks,
    check_environment,
    check_kube_api_env,
)


def lint(model: dict, topology: dict[str, str], *, rendered: Path | None = None, test_db_text: str | None = None) -> list[str]:
    problems: list[str] = []
    for check in ALL_CHECKS:
        problems += check(model)
    for check in TOPOLOGY_CHECKS:
        problems += check(model, topology)
    if rendered is not None:
        problems += check_rendered_env(model, rendered)
    if test_db_text is not None:
        problems += check_pg_matches_test_db(model, test_db_text)
    return problems


def main(argv: list[str]) -> int:
    reason = compose_unavailable_reason()
    if reason:
        print(f"compose_lint: SKIP {reason}", file=sys.stderr)
        return 2
    model = load_model()
    topology = parse_topology()
    rendered = None
    with tempfile.TemporaryDirectory(prefix="compose-lint-") as tmp:
        if "--no-render" not in argv:
            missing = render_tools_missing()
            if missing:
                if os.environ.get("CI"):
                    print(f"compose_lint: FAIL {', '.join(missing)} missing and CI is set (env.list comparison)", file=sys.stderr)
                    return 1
                print(
                    f"compose_lint: SKIP env.list comparison ({', '.join(missing)} missing); every other check runs",
                    file=sys.stderr,
                )
            else:
                rendered = render_offline(Path(tmp))
        problems = lint(model, topology, rendered=rendered, test_db_text=TEST_DB.read_text())
    for line in problems:
        print(f"LINT {line}")
    print(f"compose_lint: {len(problems)} problem(s) (LOCAL_TEST_ONLY / NOT_QUALIFIED)", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
