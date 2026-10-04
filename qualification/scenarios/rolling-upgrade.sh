#!/usr/bin/env bash
set -euo pipefail
SIGNALING_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "${1:-}" in
  --dry-run)
    for SIGNALING_REQUIRED_FILE in runbooks/gateway-drain.md runbooks/actor-drain.md signaling-app/src/main/java/io/webrtc/signaling/app/ShutdownCoordinator.java signaling-app/src/main/java/io/webrtc/signaling/app/CompatibilityWindow.java; do
      test -s "$SIGNALING_REPO_ROOT/$SIGNALING_REQUIRED_FILE" || { echo "Missing lifecycle contract: $SIGNALING_REQUIRED_FILE" >&2; exit 1; }
    done
    cd "$SIGNALING_REPO_ROOT"
    ./mvnw ${SIGNALING_MAVEN_REPO:+-Dmaven.repo.local="$SIGNALING_MAVEN_REPO"} -pl signaling-app -am test -Dtest=ShutdownCoordinatorTest,CompatibilityWindowTest -DfailIfNoTests=false
    echo 'PASS LOCAL_CONTRACT: ordered drain / N,N-1 gates / rollback. External Kubernetes rolling drill remains NOT_QUALIFIED.'
    ;;
  *) echo 'Usage: rolling-upgrade.sh --dry-run. External upgrade requires the reviewed runbook, target cluster and genuine candidate evidence; this local contract does not deploy.' >&2; exit 2;;
esac
