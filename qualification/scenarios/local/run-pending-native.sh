#!/usr/bin/env bash
# TEST ONLY: isolated Testcontainers PostgreSQL, native JVM faults and Chromium.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$root"
for tool in java docker node npm python3; do command -v "$tool" >/dev/null || { echo "Missing prerequisite: $tool" >&2; exit 1; }; done
docker info >/dev/null
java -version 2>&1 | head -1 | grep -Eq 'version "21\.' || { echo 'Java 21 required' >&2; exit 1; }
node -e 'if(Number(process.versions.node.split(".")[0])<20) process.exit(1)'
npm ci --prefix signaling-integration-tests/src/test/webrtc
if [[ -z ${CHROMIUM_PATH:-} ]]; then
  for candidate in google-chrome chromium chromium-browser; do
    if command -v "$candidate" >/dev/null; then export CHROMIUM_PATH; CHROMIUM_PATH=$(command -v "$candidate"); break; fi
  done
fi
if [[ -z ${CHROMIUM_PATH:-} ]]; then
  # Downloads the pinned Playwright browser into the user's cache, no shared service changes.
  (cd signaling-integration-tests/src/test/webrtc && npx playwright install chromium)
  export CHROMIUM_PATH
  CHROMIUM_PATH=$(node -e 'console.log(require("./signaling-integration-tests/src/test/webrtc/node_modules/playwright").chromium.executablePath())')
fi
[[ -x "$CHROMIUM_PATH" ]] || { echo "Browser is not executable: $CHROMIUM_PATH" >&2; exit 1; }
venv="$root/.superpowers/pending-native-python"
if [[ ! -x "$venv/bin/python" ]]; then python3 -m venv "$venv"; fi
"$venv/bin/pip" install -r qualification/requirements.txt
stamp=$(date -u +%Y%m%dT%H%M%S)-$$
output="$root/.superpowers/sdd/2026-10-12-pending-native-integrations/local-$stamp"
mkdir -p "$output"
export PENDING_NATIVE_EVIDENCE="$output"
trap 'run_status=$?; "$venv/bin/python" - "$run_status" <<"SUMMARY"
import hashlib, json, os, sys
from pathlib import Path
root = Path(os.environ["PENDING_NATIVE_EVIDENCE"])
files = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
         for p in root.rglob("*") if p.is_file() and p.name != "summary.json"}
(root / "summary.json").write_text(json.dumps({"testOnly": True,
    "status": "LOCAL_SUITE_PASSED" if int(sys.argv[1]) == 0 else "FAILED",
    "exitCode": int(sys.argv[1]), "productionQualification": "NOT_QUALIFIED",
    "sha256": files}, indent=2) + "\n")
SUMMARY
' EXIT
# Keep all original failures. Never promote missing production inputs to PASS.
node --test clients/native-browser/runtime-journal.test.cjs clients/native-browser/media-recovery-controller.test.mjs >"$output/browser.log" 2>&1
"$venv/bin/python" -m unittest discover -s qualification/scenarios/tests >"$output/qualification-tests.log" 2>&1
"$venv/bin/python" qualification/scenarios/fault_suite.py --output "$output/native"
./mvnw -B spotless:check >"$output/spotless.log" 2>&1
git diff --check
printf 'Local test evidence: %s\nProduction qualification: NOT_QUALIFIED\n' "$output"
