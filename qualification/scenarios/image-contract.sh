#!/usr/bin/env bash
set -euo pipefail
image=${1:?Usage: image-contract.sh LOCAL_IMAGE}
image_workspace=$(mktemp -d)
cleanup_image_smoke(){
  for cid_file in "$image_workspace"/*.cid; do
    [[ -f "$cid_file" ]] || continue
    if read -r candidate_container < "$cid_file" && [[ "$candidate_container" =~ ^[a-f0-9]{64}$ ]]; then
      docker rm -f "$candidate_container" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "$image_workspace"
}
trap cleanup_image_smoke EXIT
# Native Docker metadata and runtime, not a Dockerfile substring check.
docker image inspect "$image" > "$image_workspace/inspect.json"
python3 - "$image_workspace/inspect.json" <<'PY'
import json,re,sys
image=json.load(open(sys.argv[1]))[0]
assert image['Os']=='linux' and image['Architecture']=='amd64','Unqualified image architecture'
config=image['Config'];labels=config.get('Labels') or {}
assert config['User']=='10001:10001','Image must run as the declared unprivileged identity'
assert config['WorkingDir']=='/opt/signaling','Unexpected launcher working directory'
assert config['Entrypoint']==['java','-jar','/opt/signaling/app.jar'],'Unexpected executable launcher'
assert re.fullmatch('[a-f0-9]{40}',labels.get('org.opencontainers.image.revision','')),'Missing immutable source commit'
assert re.fullmatch('sha256:[a-f0-9]{64}',labels.get('org.opencontainers.image.base.digest','')),'Missing pinned native base digest'
assert not any(name.startswith(('SIGNALING_IDENTITY_ISSUER=','SIGNALING_IDENTITY_AUDIENCE=','SIGNALING_RUNTIME_CONTRACT=')) for name in config.get('Env',[])),'Image must not embed an environment enrollment'
PY
timeout 45s docker run --rm --cidfile "$image_workspace/java.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m --entrypoint java "$image" -version > "$image_workspace/java.log" 2>&1
python3 - "$image_workspace/java.log" <<'PY'
import sys
assert '21.0.8' in open(sys.argv[1]).read(),'Native JRE version differs from the pinned candidate'
PY
for plane in actor gateway control; do
  if timeout 45s docker run --rm --cidfile "$image_workspace/$plane.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m --env "SPRING_PROFILES_ACTIVE=$plane" "$image" > "$image_workspace/$plane.log" 2>&1; then
    echo "Unsafe image startup succeeded without enrolled identity: $plane" >&2
    exit 1
  else
    image_exit=$?
    if [[ "$image_exit" != 1 ]]; then
      echo "Image did not reject enrollment through its native launcher: $plane (exit $image_exit)" >&2
      exit 1
    fi
  fi
  python3 - "$image_workspace/$plane.log" <<'PY'
import sys
log=open(sys.argv[1]).read()
assert '"errorCode":"CONFIGURATION_REJECTED"' in log,'Failure did not originate from native configuration validation'
PY
done
for plane in actor gateway control; do
  if timeout 45s docker run --rm --cidfile "$image_workspace/$plane-native.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m --env "SPRING_PROFILES_ACTIVE=$plane" --env SIGNALING_IDENTITY_ISSUER=TEST_ONLY_ISSUER --env SIGNALING_IDENTITY_AUDIENCE=TEST_ONLY_AUDIENCE "$image" > "$image_workspace/$plane-native.log" 2>&1; then
    echo "Unsafe configuration-only launcher succeeded without its native plane: $plane" >&2
    exit 1
  else
    image_exit=$?
    if [[ "$image_exit" != 1 ]]; then
      echo "Native runtime installation rejection unproven: $plane (exit $image_exit)" >&2
      exit 1
    fi
  fi
  python3 - "$image_workspace/$plane-native.log" <<'PYCODE'
import sys
log=open(sys.argv[1]).read()
assert '"errorCode":"NATIVE_RUNTIME_NOT_INSTALLED"' in log,'Native plane did not fail closed through its installation guard'
assert 'TEST_ONLY_ISSUER' not in log and 'TEST_ONLY_AUDIENCE' not in log,'Startup diagnostic exposed identity values'
PYCODE
done
echo 'Native image contract PASS; missing enrollment and absent native plane are rejected. This does not prove complete native Main or qualification.'
