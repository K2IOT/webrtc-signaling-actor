#!/usr/bin/env bash
set -euo pipefail
SIGNALING_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
python3 - "$SIGNALING_REPO_ROOT" <<'PY'
import pathlib,sys
root=pathlib.Path(sys.argv[1])
for path in ['deploy/helm/signaling/Chart.yaml','deploy/helm/signaling/values-production.yaml','deploy/helm/signaling/templates/gateway-deployment.yaml','deploy/helm/signaling/templates/actor-deployment.yaml','deploy/helm/signaling/templates/control-deployment.yaml','deploy/helm/signaling/templates/services.yaml','deploy/helm/signaling/templates/pdb.yaml','deploy/helm/signaling/templates/networkpolicy.yaml','deploy/postgres/cloudnativepg-cluster.yaml','deploy/directory/cloudnativepg-directory.yaml']:
 assert (root/path).is_file(),f'Missing deployment artifact: {path}'
PY
SIGNALING_RENDERED=$(mktemp)
trap 'rm -f "$SIGNALING_RENDERED"' EXIT
helm template --kube-version 1.33.5 signaling "$SIGNALING_REPO_ROOT/deploy/helm/signaling" -f "$SIGNALING_REPO_ROOT/deploy/helm/signaling/values-production.yaml" -f "$SIGNALING_REPO_ROOT/qualification/scenarios/fixtures/deployment-values.yaml" > "$SIGNALING_RENDERED"
python3 "$SIGNALING_REPO_ROOT/qualification/scenarios/deployment-contract.py" "$SIGNALING_RENDERED" "$SIGNALING_REPO_ROOT"
# Missing deployment inputs and unsafe overrides must be rejected, rather than silently rendered.
if helm template --kube-version 1.33.5 signaling "$SIGNALING_REPO_ROOT/deploy/helm/signaling" -f "$SIGNALING_REPO_ROOT/deploy/helm/signaling/values-production.yaml" >/dev/null 2>&1; then
  echo 'FAIL: incomplete production inputs accepted' >&2; exit 1
fi
for SIGNALING_UNSAFE_OVERRIDE in 'actor.replicas=5' 'actor.maxSurge=2' 'actor.minAvailable=3' 'actor.terminationGraceSeconds=89' 'gateway.terminationGraceSeconds=299' 'resources.limits.memory=2Gi' 'network.kubernetesApiCidr=0.0.0.0/0'; do
  if helm template --kube-version 1.33.5 signaling "$SIGNALING_REPO_ROOT/deploy/helm/signaling" -f "$SIGNALING_REPO_ROOT/deploy/helm/signaling/values-production.yaml" -f "$SIGNALING_REPO_ROOT/qualification/scenarios/fixtures/deployment-values.yaml" --set "$SIGNALING_UNSAFE_OVERRIDE" >/dev/null 2>&1; then
    echo "FAIL: unsafe override accepted: $SIGNALING_UNSAFE_OVERRIDE" >&2; exit 1
  fi
done
helm lint --kube-version 1.33.5 "$SIGNALING_REPO_ROOT/deploy/helm/signaling" -f "$SIGNALING_REPO_ROOT/deploy/helm/signaling/values-production.yaml" -f "$SIGNALING_REPO_ROOT/qualification/scenarios/fixtures/deployment-values.yaml"
