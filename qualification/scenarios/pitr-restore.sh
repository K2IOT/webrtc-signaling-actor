#!/usr/bin/env bash
set -euo pipefail
SIGNALING_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "${1:-}" in
  --dry-run)
    for SIGNALING_REQUIRED_FILE in runbooks/postgres-failover.md runbooks/cell-recovery.md runbooks/pitr-restore.md runbooks/privacy-after-restore.md signaling-storage/src/main/java/io/webrtc/signaling/storage/RecoveryEpochService.java; do
      test -s "$SIGNALING_REPO_ROOT/$SIGNALING_REQUIRED_FILE" || { echo "Missing restore contract: $SIGNALING_REQUIRED_FILE" >&2; exit 1; }
    done
    cd "$SIGNALING_REPO_ROOT"
    ./mvnw ${SIGNALING_MAVEN_REPO:+-Dmaven.repo.local="$SIGNALING_MAVEN_REPO"} -pl signaling-storage -am verify -Dtest=NoSelectedTests -Dit.test=RecoveryEpochIT -DfailIfNoTests=false
    echo 'PASS LOCAL_CONTRACT: PostgreSQL epoch recovery and stale-work rejection. Real backup/PITR/physical-fencing/privacy-replay drill remains NOT_QUALIFIED.'
    ;;
  *) echo 'Usage: pitr-restore.sh --dry-run. Real PITR requires the reviewed runbook, backup system and external fencing/high-water evidence; no database restore is performed here.' >&2; exit 2;;
esac
