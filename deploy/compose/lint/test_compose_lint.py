"""Tests of deploy/compose/compose.yaml through compose_lint. LOCAL_TEST_ONLY / NOT_QUALIFIED.

Run: python3 -m unittest discover -s deploy/compose/lint
The whole module is skipped when docker compose (>= 2.26) is unavailable, and fails instead when
CI is set. The env.list comparison renders stage 1 + an offline render into a temp dir (needs
bash, openssl, keytool, jq; about 15 s), skipped without them unless CI is set.
"""

from __future__ import annotations

import copy
import os
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import compose_lint as cl  # noqa: E402

MODEL: dict = {}
TOPOLOGY: dict = {}
RENDERED: Path | None = None
_TMP: tempfile.TemporaryDirectory | None = None


def setUpModule() -> None:  # noqa: N802
    global MODEL, TOPOLOGY, RENDERED, _TMP
    reason = cl.compose_unavailable_reason()
    if reason:
        if os.environ.get("CI"):
            raise RuntimeError(f"{reason} and CI is set")
        raise unittest.SkipTest(f"compose lint needs docker compose >= 2.26: {reason}")
    MODEL = cl.load_model()
    TOPOLOGY = cl.parse_topology()
    missing = cl.render_tools_missing()
    if not missing:
        _TMP = tempfile.TemporaryDirectory(prefix="compose-lint-test-")
        RENDERED = cl.render_offline(Path(_TMP.name))
    elif os.environ.get("CI"):
        raise RuntimeError(f"render tools missing under CI: {missing}")


def tearDownModule() -> None:  # noqa: N802
    if _TMP is not None:
        _TMP.cleanup()


def mutated(fn):
    m = copy.deepcopy(MODEL)
    fn(m)
    return m


class ComposeModel(unittest.TestCase):
    def assertClean(self, problems):  # noqa: N802
        self.assertEqual([], problems, "\n".join(problems))

    def assertCaught(self, problems, needle):  # noqa: N802
        self.assertTrue(any(needle in p for p in problems), f"{needle!r} not in {problems}")

    # --- brief names -------------------------------------------------------------------------

    def test_every_service_is_hardened(self):
        self.assertClean(cl.check_hardening(MODEL))
        m = mutated(lambda m: m["services"]["actor-c001-1"].pop("read_only"))
        self.assertCaught(cl.check_hardening(m), "actor-c001-1: read_only")
        m = mutated(lambda m: m["services"]["gateway-c001"].update(user="0:0"))
        self.assertCaught(cl.check_hardening(m), "gateway-c001: user must be 10001:10001")
        m = mutated(lambda m: m["services"]["pg-directory"].update(cap_add=["SYS_ADMIN"]))
        self.assertCaught(cl.check_hardening(m), "pg-directory: cap_add ['SYS_ADMIN']")
        m = mutated(lambda m: m["services"]["identity"].update(security_opt=[]))
        self.assertCaught(cl.check_hardening(m), "identity: security_opt")
        # /tmp must stay exec-able for the JVMs' native libraries (netty epoll, lz4, ...)
        self.assertNotIn("noexec", cl.JVM_TMPFS)
        m = mutated(lambda m: m["services"]["actor-c001-1"].update(tmpfs=["/tmp:size=128m,noexec,nosuid"]))
        self.assertCaught(cl.check_hardening(m), "actor-c001-1: tmpfs must be /tmp:size=128m,nosuid")

    def test_no_image_uses_latest_or_lacks_digest(self):
        self.assertClean(cl.check_images(MODEL))
        m = mutated(lambda m: m["services"]["actor-c001-1"].update(image="signaling-app:latest"))
        self.assertCaught(cl.check_images(m), "uses latest")
        m = mutated(lambda m: m["services"]["pg-c001-primary"].update(image="postgres:17.6"))
        self.assertCaught(cl.check_images(m), "lacks an @sha256 digest")

    def test_no_environment_value_looks_like_a_secret(self):
        self.assertClean(cl.check_secrets(MODEL))
        for svc in MODEL["services"].values():
            for key in cl.FORBIDDEN_ENV_KEYS:
                self.assertNotIn(key, svc.get("environment") or {})
        m = mutated(lambda m: m["services"]["actor-c001-1"]["environment"].update(SIGNALING_REMOTING_KEYSTORE_PASSWORD="x"))
        self.assertCaught(cl.check_secrets(m), "SIGNALING_REMOTING_KEYSTORE_PASSWORD must come from the mount")
        m = mutated(lambda m: m["services"]["control"]["environment"].update(DB_PASSWORD="0123abcd"))
        self.assertCaught(cl.check_secrets(m), "control: environment DB_PASSWORD looks like a secret")
        m = mutated(lambda m: m["services"]["identity"]["environment"].update(SIGNALING_IDENTITY_ACCOUNTS_0_PASSWORD="hunter2"))
        self.assertCaught(cl.check_secrets(m), "SIGNALING_IDENTITY_ACCOUNTS_0_PASSWORD looks like a secret")
        m = mutated(lambda m: m["services"]["gateway-c001"].update(command=["--db-password=abc"]))
        self.assertCaught(cl.check_secrets(m), "carries a secret")

    def test_every_address_matches_topology_env(self):
        self.assertClean(cl.check_addresses(MODEL, TOPOLOGY))
        m = mutated(lambda m: m["services"]["actor-c002-3"]["networks"]["net-c002"].update(ipv4_address="172.30.2.99"))
        self.assertCaught(cl.check_addresses(m, TOPOLOGY), "actor-c002-3: net-c002 address must be 172.30.2.13")
        m = mutated(lambda m: m["services"]["provision-2"]["networks"]["net-shared"].update(ipv4_address="172.30.0.10"))
        self.assertCaught(cl.check_addresses(m, TOPOLOGY), "provision-2: net-shared must use a dynamic address")

    def test_c001_and_c002_actors_share_no_cell_network(self):
        self.assertClean(cl.check_actor_cell_isolation(MODEL))
        m = mutated(lambda m: m["services"]["actor-c001-3"]["networks"].update({"net-c002": None}))
        self.assertCaught(cl.check_actor_cell_isolation(m), "actor-c001-3: joins net-c002")

    def test_artery_and_management_ports_are_not_published_or_on_net_shared(self):
        self.assertClean(cl.check_internal_ports(MODEL, TOPOLOGY))
        port = {"mode": "ingress", "host_ip": "127.0.0.1", "target": 25520, "published": "25520", "protocol": "tcp"}
        m = mutated(lambda m: m["services"]["actor-c001-1"].update(ports=[port]))
        self.assertCaught(cl.check_internal_ports(m, TOPOLOGY), "publishes the Artery port")
        m = mutated(lambda m: m["services"]["actor-c001-1"].update(expose=["8558"]))
        self.assertCaught(cl.check_internal_ports(m, TOPOLOGY), "exposes internal port 8558")

    def test_only_expected_ports_are_published(self):
        published = sorted(int(p["published"]) for s in MODEL["services"].values() for p in s.get("ports") or [])
        self.assertEqual([8081, 8443, 8444, 8445], published)

    def test_depends_on_order_matches_spec_section_6(self):
        self.assertClean(cl.check_start_order(MODEL))

    def test_gateways_do_not_restart(self):
        self.assertClean(cl.check_restart(MODEL))
        m = mutated(lambda m: m["services"]["gateway-c002"].update(restart="on-failure:5"))
        self.assertCaught(cl.check_restart(m), "gateway-c002: restart must be 'no'")

    def test_healthchecks_use_the_concrete_ip(self):
        self.assertClean(cl.check_healthchecks(MODEL, TOPOLOGY))

        def to_wildcard(m):
            m["services"]["actor-c001-2"]["healthcheck"]["test"][5] = "0.0.0.0"

        self.assertCaught(cl.check_healthchecks(mutated(to_wildcard), TOPOLOGY), "actor-c001-2: healthcheck uses 0.0.0.0")

        def to_shared(m):  # an actor's health server binds its podIp (the cell address)
            m["services"]["actor-c001-2"]["healthcheck"]["test"][5] = TOPOLOGY["ACTOR_C001_2_SHARED_IP"]

        self.assertCaught(cl.check_healthchecks(mutated(to_shared), TOPOLOGY), "actor-c001-2: healthcheck must probe 172.30.1.12")

    def test_volume_mounts_use_subpath_read_only(self):
        self.assertClean(cl.check_volumes(MODEL))
        m = mutated(lambda m: m["services"]["control"]["volumes"][0].update(read_only=False))
        self.assertCaught(cl.check_volumes(m), "control: run volume must be mounted read-only")
        m = mutated(lambda m: m["services"]["control"]["volumes"][0].update(volume={"subpath": "gateway-c001"}))
        self.assertCaught(cl.check_volumes(m), "control: run volume must be mounted read-only at /run/signaling with subpath control")

    def test_service_count_is_18_plus_provisioners(self):
        names = set(MODEL["services"])
        self.assertEqual(18, len(names - set(cl.PROVISIONERS)))
        self.assertEqual(set(cl.PROVISIONERS), names & set(cl.PROVISIONERS))

    # --- controller additions ----------------------------------------------------------------

    def test_service_set_is_exactly_the_20(self):
        self.assertEqual(20, len(cl.EXPECTED_SERVICES))
        self.assertClean(cl.check_service_set(MODEL))
        m = mutated(lambda m: m["services"].update(debug={"image": "busybox"}))
        self.assertCaught(cl.check_service_set(m), "debug is not one of the expected")

    def test_env_matches_rendered_env_list(self):
        self.assertClean(cl.check_environment(MODEL, TOPOLOGY))
        if RENDERED is None:
            self.skipTest("bash/openssl/keytool/jq missing: no offline render to compare with")
        self.assertClean(cl.check_rendered_env(MODEL, RENDERED))
        m = mutated(lambda m: m["services"]["actor-c001-1"]["environment"].update(SIGNALING_CELL_ID="c002"))
        self.assertCaught(cl.check_rendered_env(m, RENDERED), "actor-c001-1: SIGNALING_CELL_ID='c002' in compose")
        m = mutated(lambda m: m["services"]["actor-c001-1"]["environment"].update(SIGNALING_POD_UID="x"))
        self.assertCaught(cl.check_rendered_env(m, RENDERED), "SIGNALING_POD_UID is exported by launch.sh")
        m = mutated(lambda m: m["services"]["actor-c002-1"]["environment"].update(SIGNALING_KUBE_API_HOST="172.30.2.250"))
        self.assertCaught(cl.check_rendered_env(m, RENDERED), "SIGNALING_KUBE_API_HOST is exported by launch.sh")
        m = mutated(lambda m: m["services"]["gateway-c001"]["environment"].pop("SIGNALING_CLUSTER_FINGERPRINT"))
        self.assertCaught(cl.check_rendered_env(m, RENDERED), "gateway-c001: SIGNALING_CLUSTER_FINGERPRINT=None")

    def test_jvm_opts_come_from_env_example(self):
        jvm_opts = cl.env_example_jvm_opts()
        self.assertIn("-XX:+ExitOnOutOfMemoryError", jvm_opts)
        self.assertEqual(jvm_opts, MODEL["services"]["actor-c001-1"]["environment"]["JVM_OPTS"])
        self.assertEqual(jvm_opts, MODEL["services"]["identity"]["environment"]["JDK_JAVA_OPTIONS"])
        m = mutated(lambda m: m["services"]["control"]["environment"].update(JVM_OPTS="-Xmx64m"))
        self.assertCaught(cl.check_environment(m, TOPOLOGY), "control: JVM_OPTS must be JVM_OPTS of .env.example")
        m = mutated(lambda m: m["services"]["local-platform"]["environment"].update(JDK_JAVA_OPTIONS="-Xmx2g"))
        self.assertCaught(cl.check_environment(m, TOPOLOGY), "local-platform: JDK_JAVA_OPTIONS must be JVM_OPTS")

    def test_kube_api_env_is_per_cell(self):
        self.assertClean(cl.check_kube_api_env(MODEL, TOPOLOGY))
        env1 = MODEL["services"]["kube-api-c001"]["environment"]
        self.assertEqual("172.30.1.11,172.30.1.12,172.30.1.13", env1["KUBE_POD_IPS"])

        def swap(key):
            def fn(m):
                a, b = m["services"]["kube-api-c001"]["environment"], m["services"]["kube-api-c002"]["environment"]
                a[key], b[key] = b[key], a[key]
            return fn

        for key in ("KUBE_POD_IPS", "KUBE_LISTEN", "KUBE_HEALTH_LISTEN", "KUBE_NAMESPACE", "KUBE_CELL"):
            problems = cl.check_kube_api_env(mutated(swap(key)), TOPOLOGY)
            self.assertCaught(problems, "kube-api-c001: environment differs")
            self.assertCaught(problems, "kube-api-c002: environment differs")
        m = mutated(lambda m: m["services"]["kube-api-c002"]["environment"].update(KUBE_TOKEN_FILE="/run/signaling/kube-ca.crt"))
        self.assertCaught(cl.check_kube_api_env(m, TOPOLOGY), "kube-api-c002: environment differs")

    def test_cli_fails_under_ci_without_render_tools(self):
        with mock.patch.object(cl, "render_tools_missing", return_value=["keytool"]):
            with mock.patch.dict(os.environ, {"CI": "1"}):
                self.assertEqual(1, cl.main([]))
            env = {k: v for k, v in os.environ.items() if k != "CI"}
            with mock.patch.dict(os.environ, env, clear=True):
                self.assertEqual(0, cl.main([]))

    def test_no_service_mounts_shared_or_ca(self):
        self.assertClean(cl.check_no_shared_or_ca(MODEL))
        m = mutated(lambda m: m["services"]["gateway-c001"]["volumes"][0].update(volume={"subpath": "_shared"}))
        self.assertCaught(cl.check_no_shared_or_ca(m), "gateway-c001: mounts _shared")
        m = mutated(lambda m: m["services"]["local-platform"]["volumes"][0].update(volume={}))
        self.assertCaught(cl.check_no_shared_or_ca(m), "local-platform: mounts the whole run volume")

    def test_cell_networks_are_internal_and_shared_is_not(self):
        self.assertClean(cl.check_networks(MODEL, TOPOLOGY))
        m = mutated(lambda m: m["networks"]["net-c001"].pop("internal"))
        self.assertCaught(cl.check_networks(m, TOPOLOGY), "net-c001: internal must be true")
        m = mutated(lambda m: m["networks"]["net-shared"].update(internal=True))
        self.assertCaught(cl.check_networks(m, TOPOLOGY), "net-shared: internal must be false")

    def test_shared_gateway_is_pinned_and_named_for_the_platform(self):
        # The JDK HttpsServer of local-platform reverse-resolves every TLS peer. Callers on the host
        # (the smoke) come from the net-shared gateway, which has no PTR record in Docker's DNS:
        # without a hosts entry each handshake waits for the resolver to time out (~8 s).
        self.assertClean(cl.check_networks(MODEL, TOPOLOGY))
        self.assertClean(cl.check_platform_peer_names(MODEL, TOPOLOGY))
        m = mutated(lambda m: m["networks"]["net-shared"]["ipam"]["config"][0].pop("gateway"))
        self.assertCaught(cl.check_networks(m, TOPOLOGY), "net-shared: ipam gateway must be")
        m = mutated(lambda m: m["services"]["local-platform"].pop("extra_hosts"))
        self.assertCaught(cl.check_platform_peer_names(m, TOPOLOGY), "local-platform: extra_hosts must map")

    def test_only_published_ports_are_the_four_on_loopback(self):
        self.assertClean(cl.check_ports(MODEL, TOPOLOGY))
        m = mutated(lambda m: m["services"]["identity"]["ports"][0].update(host_ip="0.0.0.0"))
        self.assertCaught(cl.check_ports(m, TOPOLOGY), "identity: ports must be exactly 127.0.0.1:8081")
        extra = {"mode": "ingress", "host_ip": "127.0.0.1", "target": 9443, "published": "9443", "protocol": "tcp"}
        m = mutated(lambda m: m["services"]["local-platform"].update(ports=[extra]))
        self.assertCaught(cl.check_ports(m, TOPOLOGY), "local-platform: publishes ports")

    def test_start_order(self):
        self.assertClean(cl.check_start_order(MODEL))
        dep = {"condition": "service_healthy", "required": True}
        m = mutated(lambda m: m["services"]["actor-c001-2"]["depends_on"].update({"actor-c001-1": dep}))
        self.assertCaught(cl.check_start_order(m), "actor-c001-2: actors must not depend on each other")
        m = mutated(lambda m: m["services"]["gateway-c001"]["depends_on"].pop("actor-c001-3"))
        self.assertCaught(cl.check_start_order(m), "gateway-c001: depends_on differs")
        m = mutated(lambda m: m["services"]["control"]["depends_on"].pop("actor-c002-1"))
        self.assertCaught(cl.check_start_order(m), "control: depends_on differs")
        m = mutated(lambda m: m["services"]["provision-2"]["depends_on"].pop("pg-directory"))
        self.assertCaught(cl.check_start_order(m), "provision-2: depends_on differs")
        m = mutated(lambda m: m["services"]["identity"].pop("depends_on"))
        self.assertCaught(cl.check_start_order(m), "identity: depends_on differs")

    def test_pg_services_match_the_docker_test_settings(self):
        text = cl.TEST_DB.read_text()
        ref = cl.parse_test_db_pg_run(text)
        self.assertEqual(["/tmp", "/var/run/postgresql", "/run/pgtls"], ref["tmpfs"])
        self.assertEqual("/docker-entrypoint-initdb.d", ref["init_tmpfs"])
        self.assertEqual(["bash", "/run/signaling/start.sh"], ref["entrypoint"])
        self.assertClean(cl.check_pg_matches_test_db(MODEL, text))
        m = mutated(lambda m: m["services"]["pg-c001-standby"]["tmpfs"].append("/docker-entrypoint-initdb.d"))
        self.assertCaught(cl.check_pg_matches_test_db(m, text), "pg-c001-standby: tmpfs")
        drifted = text.replace("--cap-add SETUID", "--cap-add SETUID --cap-add NET_RAW", 1)
        self.assertCaught(cl.check_pg_matches_test_db(MODEL, drifted), "cap_add")

    def test_entrypoints_and_commands(self):
        self.assertClean(cl.check_entrypoints(MODEL))
        m = mutated(lambda m: m["services"]["provision-1"]["environment"].update(PROVISION_FORCE="1"))
        self.assertCaught(cl.check_entrypoints(m), "never sets PROVISION_FORCE")

    def test_whole_lint_is_clean(self):
        self.assertClean(cl.lint(MODEL, TOPOLOGY, rendered=RENDERED, test_db_text=cl.TEST_DB.read_text()))


class TopologyParser(unittest.TestCase):
    def test_parses_quotes_and_rejects_expansion(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "t.env"
            f.write_text('# c\nCELLS="c001 c002"\nA=1\n')
            self.assertEqual({"CELLS": "c001 c002", "A": "1"}, cl.parse_topology(f))
            f.write_text("A=$B\n")
            with self.assertRaises(ValueError):
                cl.parse_topology(f)


if __name__ == "__main__":
    unittest.main()
