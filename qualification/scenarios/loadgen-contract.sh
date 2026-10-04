#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 - <<'PY'
from pathlib import Path
import json, yaml
root=Path('.')
for name in ('DistributedLoadGenerator','ScenarioRunner','VirtualClient','EvidenceWriter'):
    assert (root/'signaling-loadgen/src/main/java/io/webrtc/signaling/loadgen'/f'{name}.java').is_file(),f'missing executable {name}'
names=('p0-connections','p1-baseline','p2-production','p2-az-loss','p2-soak-24h','p2-skew-burst','security-abuse')
profiles={n:yaml.safe_load((root/'qualification/scenarios'/f'{n}.yaml').read_text()) for n in names}
p2=profiles['p2-production']['targets']
expected={'distinctUsers':8_000_000,'sockets':10_000_000,'callAttemptsPerSecond':10_000,'establishedCalls':3_000_000,'meanCallSeconds':300,'inboundSetupFramesPerSecond':500_000,'registrationsPerSecond':20_000,'crossCellRatio':0.98}
for key,value in expected.items(): assert p2[key]==value,(key,p2[key],value)
for profile in profiles.values():
    assert profile['transport']=={'scheme':'wss','tlsMinimum':'TLSv1.3','jwtAlgorithm':'RS256','hostnameVerification':True}
    assert profile['distributed']['uniqueSourceIps'] and profile['distributed']['deterministicRanges']
    assert profile['stages'][:2]==[10_000,100_000]
    assert profile['measurements']['latencyClock']=='intendedArrivalMonotonic'
    assert profile['measurements']['generatorHeadroom']==['cpu','nic','fd','eventLoop','pendingOperations','pendingBytes']
assert profiles['p2-soak-24h']['durationSeconds']>=86400
assert profiles['p2-az-loss']['fault']['kind']=='ONE_AZ_LOSS'
assert profiles['p2-skew-burst']['burst']=={'multiplier':2,'seconds':60,'hotDestinationMultiplier':5,'hotBucketMultiplier':5}
assert set(profiles['security-abuse']['abuse'])>={'malformed','slowConsumer','oversized','staleGeneration','revokedJti','retiredSigningKey'}
for name in ('qualification/loadgen/config.schema.json','qualification/evidence/evidence-schema.json'):
    schema=json.loads((root/name).read_text());assert schema['additionalProperties'] is False
assert 'rawHistogram' in json.loads((root/'qualification/evidence/evidence-schema.json').read_text())['required']
print('loadgen contract PASS; this checks the harness contract, not measured capacity')
PY
