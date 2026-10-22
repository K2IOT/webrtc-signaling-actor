#!/usr/bin/env bash
set -euo pipefail
# image-contract.sh LOCAL_IMAGE [LOCAL_PLATFORM_IMAGE]   app image checks, then the optional platform image checks
# image-contract.sh --local-platform-only LOCAL_PLATFORM_IMAGE   only the local platform image checks
if [[ "${1:-}" == --local-platform-only ]]; then
  platform_only=true
  image=${2:?Usage: image-contract.sh --local-platform-only LOCAL_PLATFORM_IMAGE}
else
  platform_only=false
  image=${1:?Usage: image-contract.sh LOCAL_IMAGE [LOCAL_PLATFORM_IMAGE]}
fi
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
# LOCAL_TEST_ONLY / NOT_QUALIFIED: the local platform source image (Dockerfile.local-platform).
# Native Docker metadata and runtime; it never carries configuration, keys or credentials.
check_local_platform_image(){
  local platform_image=$1 platform_exit
  docker image inspect "$platform_image" > "$image_workspace/platform-inspect.json"
  python3 - "$image_workspace/platform-inspect.json" <<'PYPLATFORM'
import json,re,sys
image=json.load(open(sys.argv[1]))[0]
assert image['Os']=='linux' and image['Architecture']=='amd64','Unqualified platform image architecture'
config=image['Config'];labels=config.get('Labels') or {}
assert config['User']=='10001:10001','Platform image must run as the declared unprivileged identity'
assert config['WorkingDir']=='/opt/signaling','Unexpected platform launcher working directory'
assert config['Entrypoint']==['java','-jar','/opt/signaling/app.jar'],'Unexpected platform executable launcher'
assert labels.get('io.webrtc.signaling.release.status')=='NOT_QUALIFIED','Platform image must be labelled NOT_QUALIFIED'
assert re.fullmatch('[a-f0-9]{40}',labels.get('org.opencontainers.image.revision','')),'Missing immutable source commit'
assert re.fullmatch('sha256:[a-f0-9]{64}',labels.get('org.opencontainers.image.base.digest','')),'Missing pinned native base digest'
env_names={entry.split('=',1)[0] for entry in config.get('Env',[])}
for forbidden in ('SIGNALING_PLATFORM_CONFIG','SIGNALING_IDENTITY_ISSUER','SIGNALING_LOCAL_ACKNOWLEDGEMENT'):
    assert forbidden not in env_names,'Platform image must not embed an environment enrollment: '+forbidden
PYPLATFORM
  # Without the acknowledgement the process must refuse before reading anything.
  if timeout 45s docker run --rm --cidfile "$image_workspace/platform-ack.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m "$platform_image" > "$image_workspace/platform-ack.out" 2> "$image_workspace/platform-ack.err"; then
    echo 'Unsafe platform startup succeeded without the LOCAL_TEST_ONLY acknowledgement' >&2
    return 1
  else
    platform_exit=$?
    if [[ "$platform_exit" != 1 ]]; then
      echo "Platform image did not reject the missing acknowledgement through its launcher (exit $platform_exit)" >&2
      return 1
    fi
  fi
  python3 - "$image_workspace/platform-ack.out" "$image_workspace/platform-ack.err" ACKNOWLEDGEMENT <<'PYPLATFORM'
import sys
out=open(sys.argv[1]).read();err=open(sys.argv[2]).read()
assert err.splitlines()==['LOCAL_PLATFORM_REJECTED '+sys.argv[3]],'Platform did not print exactly one fixed rejection line'
assert out=='','Platform rejection must not write to stdout'
for text in (out,err):
    assert 'Exception' not in text and 'at io.webrtc' not in text,'Platform rejection exposed a stack trace'
PYPLATFORM
  # With the acknowledgement but a configuration path that does not exist in the container.
  if timeout 45s docker run --rm --cidfile "$image_workspace/platform-config.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m --env SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY --env SIGNALING_IDENTITY_ISSUER=TEST_ONLY_ISSUER --env SIGNALING_PLATFORM_CONFIG=/run/signaling/platform.json "$platform_image" > "$image_workspace/platform-config.out" 2> "$image_workspace/platform-config.err"; then
    echo 'Unsafe platform startup succeeded without a mounted configuration' >&2
    return 1
  else
    platform_exit=$?
    if [[ "$platform_exit" != 1 ]]; then
      echo "Platform image did not reject the missing configuration through its launcher (exit $platform_exit)" >&2
      return 1
    fi
  fi
  python3 - "$image_workspace/platform-config.out" "$image_workspace/platform-config.err" CONFIG <<'PYPLATFORM'
import sys
out=open(sys.argv[1]).read();err=open(sys.argv[2]).read()
assert err.splitlines()==['LOCAL_PLATFORM_REJECTED '+sys.argv[3]],'Platform did not print exactly one fixed rejection line'
assert out=='','Platform rejection must not write to stdout'
for text in (out,err):
    assert 'Exception' not in text and 'at io.webrtc' not in text,'Platform rejection exposed a stack trace'
    assert '/run/signaling' not in text and 'platform.json' not in text,'Platform rejection exposed the configuration path'
    assert 'TEST_ONLY_ISSUER' not in text,'Platform rejection exposed the identity value'
PYPLATFORM
}
if [[ "$platform_only" == true ]]; then
  check_local_platform_image "$image"
  echo 'Local platform image contract PASS; a missing acknowledgement and a missing configuration are rejected with fixed lines. This does not prove a started platform or qualification.'
  exit 0
fi
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
assert 'JDK_JAVA_OPTIONS=--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED' in config.get('Env',[]),'Missing required native Agrona JVM export'
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
# Actor and gateway metrics need a cell identity before the plane guard is reached.
for plane in actor gateway control; do
  if timeout 45s docker run --rm --cidfile "$image_workspace/$plane-native.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m --env "SPRING_PROFILES_ACTIVE=$plane" --env SIGNALING_CELL_ID=c001 --env SIGNALING_IDENTITY_ISSUER=TEST_ONLY_ISSUER --env SIGNALING_IDENTITY_AUDIENCE=TEST_ONLY_AUDIENCE "$image" > "$image_workspace/$plane-native.log" 2>&1; then
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
# The local gate must work from the packaged jar: initializer, spring.factories and the fixed
# failure rendering. The contract path is deliberately absent inside the container, so the specific
# MISSING rejection is expected and the path must not be echoed. A full valid-contract start from
# the packaged jar needs the provisioner and PKI, which are outside this script.
for plane in actor gateway control; do
  if timeout 45s docker run --rm --cidfile "$image_workspace/$plane-local.cid" --network none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m --cpus 1 --memory 512m --env "SPRING_PROFILES_ACTIVE=$plane" --env SIGNALING_IDENTITY_ISSUER=TEST_ONLY_ISSUER --env SIGNALING_IDENTITY_AUDIENCE=TEST_ONLY_AUDIENCE --env SIGNALING_DEPLOYMENT_MODE=local-minikube --env SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY --env SIGNALING_NATIVE_PROVIDER_CONTRACT=/run/signaling/provider.json "$image" > "$image_workspace/$plane-local.log" 2>&1; then
    echo "Unsafe local launcher succeeded without a mounted contract: $plane" >&2
    exit 1
  else
    image_exit=$?
    if [[ "$image_exit" != 1 ]]; then
      echo "Local gate rejection unproven: $plane (exit $image_exit)" >&2
      exit 1
    fi
  fi
  python3 - "$image_workspace/$plane-local.log" <<'PYCODE'
import sys
log=open(sys.argv[1]).read()
expected='{"component":"PLATFORM","level":"ERROR","errorCode":"LOCAL_RUNTIME_REJECTED","role":"contract","category":"MISSING"}'
assert expected in log.splitlines(),'Packaged image did not reject the missing local contract through the local gate'
for forbidden in ('STARTUP_FAILED','NATIVE_RUNTIME_NOT_INSTALLED','CONFIGURATION_REJECTED'):
    assert forbidden not in log,'Local gate rejection was masked by a generic diagnostic: '+forbidden
assert '/run/signaling' not in log and 'provider.json' not in log,'Startup diagnostic exposed the contract path'
assert 'TEST_ONLY_ISSUER' not in log and 'TEST_ONLY_AUDIENCE' not in log,'Startup diagnostic exposed identity values'
PYCODE
done
if [[ -n "${2:-}" ]]; then
  check_local_platform_image "$2"
  echo 'Local platform image contract PASS (LOCAL_TEST_ONLY, NOT_QUALIFIED).'
fi
echo 'Native image contract PASS; missing enrollment, absent native plane and a missing local contract are rejected. This does not prove complete native Main or qualification.'
