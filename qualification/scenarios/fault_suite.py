#!/usr/bin/env python3
"""Run Task 22 and retain fresh LOCAL TEST ONLY evidence, never production gates."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

import yaml


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_identity(root):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args])
    digest = hashlib.sha256()
    for name in sorted(git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")):
        if not name:
            continue
        path = root / os.fsdecode(name)
        digest.update(name + b"\0")
        digest.update(bytes.fromhex(sha256(path)) if path.is_file() else b"DELETED")
    return {
        "gitCommit": git("rev-parse", "HEAD").decode().strip(),
        "sourceFingerprint": digest.hexdigest(),
        "dirty": bool(git("status", "--porcelain")),
    }


def collect(root, matrix, started, finished, modules=None):
    if not isinstance(matrix, dict) or matrix.get("matrixVersion") != 2 or not started < finished:
        raise ValueError("Invalid matrix or original run interval")
    faults = matrix.get("faults")
    if not isinstance(faults, list) or not 1 <= len(faults) <= 128:
        raise ValueError("Empty or excessive fault matrix")
    observed, invocations, reports, totals = {}, set(), [], {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for module in sorted(root.glob("signaling-*")):
        if not module.is_dir() or modules is not None and module.name not in modules:
            continue
        for kind in ("surefire-reports", "failsafe-reports"):
            for path in sorted((module / "target" / kind).glob("TEST-*.xml")):
                if path.is_symlink() or path.stat().st_size > 16 * 1024 * 1024:
                    raise ValueError("Invalid report file")
                if not started <= path.stat().st_mtime <= finished:
                    raise ValueError(f"Stale report: {path.relative_to(root)}")
                data = path.read_bytes()
                if b"<!DOCTYPE" in data or b"<!ENTITY" in data:
                    raise ValueError("XML declarations are forbidden")
                suite = ET.fromstring(data)
                cases = suite.findall("testcase")
                counts = {}
                for key in totals:
                    value = suite.get(key, "")
                    if not re.fullmatch(r"0|[1-9][0-9]*", value):
                        raise ValueError("Invalid report counts")
                    counts[key] = int(value)
                if counts["tests"] != len(cases) or not cases or any(counts[k] for k in ("failures", "errors", "skipped")):
                    raise ValueError(f"Failed, skipped or empty report: {path.name}")
                relative = str(path.relative_to(root))
                for case in cases:
                    if any(case.find(k) is not None for k in ("failure", "error", "skipped")):
                        raise ValueError("Test did not pass")
                    name = case.get("name", "").split("(", 1)[0]
                    cls = case.get("classname", "")
                    suite_name = suite.get("name", "")
                    # jqwik writes the simple testcase class but the qualified suite.
                    # Bind only an exact simple-name match, never an arbitrary suffix.
                    if cls and "." not in cls and suite_name.rsplit(".", 1)[-1] == cls and "." in suite_name:
                        cls = suite_name
                    key = (module.name, cls, name)
                    invocation = (module.name, key[1], case.get("name", ""))
                    if not name or not key[1] or invocation in invocations:
                        raise ValueError("Missing or duplicate method identity")
                    invocations.add(invocation)
                    observed[key] = relative
                reports.append({"path": relative, "sha256": hashlib.sha256(data).hexdigest(), **counts})
                for key in totals:
                    totals[key] += counts[key]
    if not reports:
        raise ValueError("No original test reports")
    results, ids = [], set()
    for fault in faults:
        if not isinstance(fault, dict):
            raise ValueError("Invalid fault record")
        identity, scope = fault.get("id"), fault.get("scope", "")
        if not isinstance(identity, str) or not re.fullmatch(r"[a-z0-9-]{1,96}", identity) or identity in ids:
            raise ValueError("Invalid or duplicate fault identity")
        ids.add(identity)
        allowed_scopes = {"LOCAL_NATIVE_POSTGRES", "LOCAL_RUNTIME", "LOCAL_PEKKO_CLUSTER", "LOCAL_NATIVE_POSTGRES_AND_MTLS", "LOCAL_AND_EXTERNAL", "EXTERNAL_THREE_AZ"}
        if not isinstance(scope, str) or scope not in allowed_scopes:
            raise ValueError("Unknown fault scope")
        local = scope.startswith("LOCAL")
        external = scope.startswith("EXTERNAL") or scope == "LOCAL_AND_EXTERNAL"
        if not local and not external:
            raise ValueError("Unknown fault scope")
        checks = fault.get("localChecks", [])
        if not isinstance(checks, list) or local and not checks:
            raise ValueError(f"Missing method-level checks: {identity}")
        matched = []
        for check in checks:
            if not isinstance(check, dict):
                raise ValueError("Invalid local check")
            module, cls, methods = check.get("module", ""), check.get("class"), check.get("methods")
            if not isinstance(module, str) or not re.fullmatch(r"signaling-[a-z-]+", module) or not isinstance(cls, str) or not re.fullmatch(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", cls) or not isinstance(methods, list) or not methods:
                raise ValueError("Invalid local check")
            for method in methods:
                if not isinstance(method, str) or not re.fullmatch(r"[A-Za-z_$][\w$]*", method):
                    raise ValueError("Invalid required method")
                key = (module, cls, method)
                if key not in observed:
                    raise ValueError(f"Missing execution: {key}")
                matched.append({"class": cls, "method": method, "report": observed[key]})
        receipts = []
        for required in fault.get("localReceipts", []):
            module, scenario = required.get("module", ""), required.get("scenario", "")
            if not local or not re.fullmatch(r"signaling-[a-z-]+", module) or not re.fullmatch(r"[a-z0-9-]{1,96}", scenario):
                raise ValueError("Invalid local receipt binding")
            path = root / module / "target/fault-receipts" / (scenario + ".json")
            if not path.is_file() or path.is_symlink() or path.stat().st_size > 1024 * 1024:
                raise ValueError("Missing or invalid original fault receipt")
            if not started <= path.stat().st_mtime <= finished:
                raise ValueError("Stale fault receipt")
            body = json.loads(path.read_bytes())
            if body.get("testOnly") is not True or body.get("scenario") != scenario or not str(body.get("scope", "")).startswith("LOCAL") or not isinstance(body.get("observations"), dict) or not body["observations"]:
                raise ValueError("Fault receipt does not describe this local execution")
            receipts.append({"path": str(path.relative_to(root)), "sha256": sha256(path)})
        results.append({"id": identity, "scope": scope, "localStatus": "PASS" if local else "NOT_APPLICABLE",
                        "externalStatus": "NOT_RUN" if external else "NOT_APPLICABLE", "checks": matched, "receipts": receipts,
                        "requiredExternalEvidence": fault.get("requiredEvidence", []) if external else []})
    return {"schemaVersion": 1, "testOnly": True, "status": "LOCAL_SUITE_PASSED",
            "productionQualification": "NOT_QUALIFIED", "totals": totals, "faults": results, "reports": reports}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--integration-only", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = args.output or root / ".superpowers/sdd/2026-10-03-webrtc-signaling-v1.11-production-implementation" / ("fault-run-" + stamp)
    output = output.resolve()
    if output.is_relative_to(root) and subprocess.run(["git", "-C", str(root), "check-ignore", "-q", str(output)]).returncode:
        parser.error("Evidence output inside the repository must be git-ignored")
    output.mkdir(parents=True, exist_ok=False)
    command = ["./mvnw", "-B"]
    if args.integration_only:
        command += ["-pl", "signaling-integration-tests", "-am"]
    command += ["clean", "verify"]
    identity = source_identity(root)
    matrix_path = root / "qualification/scenarios/fault-matrix.yaml"
    matrix_bytes = matrix_path.read_bytes()
    matrix = yaml.safe_load(matrix_bytes)
    run = {**identity, "testOnly": True, "command": command,
           "matrixSha256": hashlib.sha256(matrix_bytes).hexdigest(),
           "java": subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode(),
           "startedAt": time.time()}
    (output / "run.json").write_text(json.dumps(run, indent=2) + "\n")
    print(f"Task 22 run: {output}", flush=True)
    with (output / "maven.log").open("wb") as log:
        result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
    run.update(finishedAt=time.time(), exitCode=result.returncode)
    (output / "run.json").write_text(json.dumps(run, indent=2) + "\n")
    try:
        if result.returncode:
            raise ValueError("Maven verification failed; see maven.log")
        if source_identity(root) != identity:
            raise ValueError("Source changed during verification")
        modules = {p.name for p in root.glob("signaling-*") if p.is_dir()}
        if args.integration_only:
            modules.discard("signaling-loadgen")
        evidence = collect(root, matrix, run["startedAt"], run["finishedAt"], modules)
        evidence.update(source=identity, runSha256=sha256(output / "run.json"), matrixSha256=run["matrixSha256"],
                        mavenLogSha256=sha256(output / "maven.log"))
        artifacts = evidence["reports"] + [receipt for fault in evidence["faults"] for receipt in fault["receipts"]]
        for report in artifacts:
            dest = output / report["path"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / report["path"], dest)
        (output / "fault-matrix.yaml").write_bytes(matrix_bytes)
        (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps({"status": evidence["status"], "tests": evidence["totals"]["tests"], "output": str(output)}))
    except (ValueError, ET.ParseError) as failure:
        (output / "evidence.json").write_text(json.dumps({"testOnly": True, "status": "FAILED", "reason": str(failure)}) + "\n")
        raise SystemExit(str(failure))


if __name__ == "__main__":
    main()
