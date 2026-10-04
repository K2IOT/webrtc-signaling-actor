#!/usr/bin/env python3
"""Integrity/completeness gate. Production observation requires an independently trusted collector."""
import argparse
import base64
import hashlib
import json
import math
import re
from datetime import datetime,timedelta,timezone
from pathlib import Path
import yaml
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

GATES=('functional','safety','capacity','burst-skew','soak','n-minus-one','dependency-faults','chaos-timing','security','dr-restore','deployment','operations')
BINDINGS=('configurationFingerprint','compatibilityFingerprint','identityContractFingerprint','topologyFingerprint','hardwareFingerprint')
CHECKS={
 'functional':('fullStateMachine','multiDeviceWinner','reconnectResume','orderedTrickleIce','iceRestart','authorization','revocation'),
 'safety':('duplicates','reordering','messageLoss','staleActors','delayedCommitAck','processPause','takeover','databaseFailover','crossCellSaga'),
 'capacity':('p0','simultaneousP2','retainedData','boundedQueues','staticCapacity','generatorHeadroom','rawHistograms'),
 'burst-skew':('doubleSetupBurst60s','hotDestination5x','hotBucket5x','multiDeviceFanout','slowConsumer','maliciousValidTraffic','explicitShedding'),
 'soak':('p2Equivalent','retainedData','cleanup','vacuum','export','reconciliation','boundedMemoryWalDiskQueueTombstones'),
 'n-minus-one':('oneAzLostAtP2','reconnect','shardRecovery','workflowConvergence','activeCallPreservation','renewalSlack','degradedAdmission','realMediaPreservation'),
 'dependency-faults':('postgresPrimary','postgresStandby','directory','revocation','redis','turnStun','kubernetesDiscovery','dns','internalRpcPartialFailure'),
 'chaos-timing':('jvmPause','cpuStarvation','packetLossReorder','clockUncertainty','shardCrash','coordinatorCrash','oldNodeRejoin','partialPartition','sbrInstability'),
 'security':('originAuthBypass','stolenReplayedToken','logoutRevocation','authorizationBypass','malformedOversizedFragmented','rateLimitDdos','dependencyCveScan'),
 'dr-restore':('independentBackupPitr','newRecoveryEpoch','oldAuthorityInvalidation','deletionReplay','checksumDataValidation','rtoRpo'),
 'deployment':('canary','rollingUpgrade','schemaExpandContract','forcedRollback','mixedVersion','drainReconnectStorm','irreversibleMigrationRecovery'),
 'operations':('dashboards','burnRateAlerts','revocationLeaseRecoveryLag','runbooks','onCallOwnership','headroom','costPerUnit','backupAgeRestore'),
}
REPORTS=('functional.md','safety.md','capacity.md','n-minus-one.md','security.md','dr-restore.md','deployment.md','release-decision.md')
STAGES=('10k','100k','200k-per-cell','multi-cell','p0-10m','p2','p2-n-minus-one','soak-24h')
IDENTITY_BINDINGS=('candidateId','gitCommit','imageDigests')+BINDINGS
RESOURCE_METRICS=('cpu','heap','nativeMemory','rss','fd','network','mailboxQueueAge','dbPools','rowRate','walRate','storageRate','revocationLag','leaseLag','recoveryLag','reconnect','activeCallPreservation')

class StrictYaml(yaml.SafeLoader):pass
def _mapping(loader,node,deep=False):
    result={}
    for key,value in node.value:
        parsed=loader.construct_object(key,deep=deep)
        if parsed in result:raise ValueError('Duplicate manifest key')
        result[parsed]=loader.construct_object(value,deep=deep)
    return result
StrictYaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,_mapping)

def number(value,minimum=0,maximum=None):
    return type(value) in (int,float) and math.isfinite(value) and value>=minimum and (maximum is None or value<=maximum)

def mapping(value):return value if isinstance(value,dict) else {}

def utc(value):
    if not isinstance(value,str):raise ValueError('Timestamp required')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('Timezone required')
    return parsed.astimezone(timezone.utc)

def artifact_path(root,name):
    if not isinstance(name,str) or not name or Path(name).is_absolute() or '..' in Path(name).parts:raise ValueError('Artifact escapes candidate')
    resolved=(root/name).resolve()
    if not resolved.is_relative_to(root.resolve()) or not resolved.is_file():raise ValueError('Artifact missing or escapes candidate')
    return resolved

def candidate_errors(root,manifest):
    errors=[];candidate=manifest.get('candidateId');git=manifest.get('gitCommit','');images=manifest.get('imageDigests',{})
    if not isinstance(candidate,str) or not re.fullmatch(r'\d{8}-\d{4}-[0-9a-f]{7}-[0-9a-f]{12}',candidate) or root.name!=candidate:return ['INVALID_CANDIDATE_ID']
    if not isinstance(git,str) or not re.fullmatch('[0-9a-f]{40}',git) or candidate.split('-')[2]!=git[:7]:errors.append('CANDIDATE_GIT_MISMATCH')
    if not isinstance(images,dict) or set(images)!={'actor','gateway','control'} or any(not isinstance(v,str) or not re.fullmatch('sha256:[0-9a-f]{64}',v) for v in images.values()):errors.append('INVALID_IMAGE_MANIFEST_DIGESTS')
    elif candidate.split('-')[3]!=images['actor'][7:19]:errors.append('CANDIDATE_IMAGE_MISMATCH')
    try:datetime.strptime('-'.join(candidate.split('-')[:2]),'%Y%m%d-%H%M')
    except ValueError:errors.append('INVALID_CANDIDATE_TIME')
    return errors

def gate_errors(name,gate,context,now):
    errors=[]
    if not isinstance(gate,dict):return ['MISSING_GATE:'+name]
    if gate.get('status')!='PASSED':errors.append('GATE_NOT_PASSED:'+name)
    if gate.get('testOnly') is not False:errors.append('TEST_ONLY_EVIDENCE:'+name)
    for key in IDENTITY_BINDINGS:
        if gate.get(key)!=context.get(key) or key not in gate:errors.append('BINDING_MISMATCH:'+name+':'+key)
    violations=gate.get('invariantViolations')
    if type(violations) is not int or violations<0:errors.append('MISSING_SAFETY_ACCOUNTING:'+name)
    elif violations:errors.append('SAFETY_INVARIANT_VIOLATION:'+name)
    elapsed=None
    try:
        began,finished=utc(gate.get('startedAt')),utc(gate.get('finishedAt'))
        elapsed=(finished-began).total_seconds()
        if finished<began or finished>now+timedelta(milliseconds=250):errors.append('INVALID_GATE_TIME:'+name)
        if now-finished>timedelta(days=7):errors.append('STALE_EVIDENCE:'+name)
    except (ValueError,TypeError):errors.append('INVALID_GATE_TIME:'+name)
    checks=gate.get('checks',{})
    if not isinstance(checks,dict) or any(checks.get(check) is not True for check in CHECKS[name]):errors.append('INCOMPLETE_CHECKS:'+name)
    if not isinstance(gate.get('artifacts'),list) or not gate['artifacts']:errors.append('MISSING_ORIGINAL_ARTIFACTS:'+name)
    metrics=mapping(gate.get('metrics'))
    if name=='soak':
        duration=metrics.get('durationSeconds');age=metrics.get('retainedDatasetAgeSeconds')
        if not number(duration,86400) or elapsed is None or not number(elapsed,86400) or duration>elapsed+.25:errors.append('SOAK_DURATION_INSUFFICIENT')
        if not number(age,86400):errors.append('EMPTY_OR_UNRETAINED_DATASET:soak')
    if name in ('n-minus-one','dependency-faults','chaos-timing','dr-restore','deployment') and not gate.get('faultTimeline'):errors.append('MISSING_FAULT_TIMELINE:'+name)
    if name=='n-minus-one' and (not number(metrics.get('eligibleReconnects'),1) or not number(metrics.get('p99ReconnectSeconds'),0,180)):errors.append('AZ_RECONNECT_TARGET_MISSED')
    if name=='dr-restore' and (not gate.get('physicalFenceReceipt') or not gate.get('acknowledgedWalReceipt') or not gate.get('outsideBackupEpochHighWaterReceipt')):errors.append('MISSING_INDEPENDENT_DR_RECEIPTS')
    return errors

def _read(path,limit=1048576):
    if not path.is_file() or path.stat().st_size>limit:raise ValueError('Evidence file missing or exceeds bound')
    return path.read_bytes()

def _sha(path):
    if path.stat().st_size>268435456:raise ValueError('Artifact exceeds verifier bound')
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        while chunk:=stream.read(1048576):digest.update(chunk)
    return digest.hexdigest()

def _signature(root,manifest,trust_file,now):
    if trust_file is None:return ['MISSING_EXTERNAL_TRUST_ROOT']
    try:
        trust_path=Path(trust_file).resolve()
        if trust_path.is_relative_to(root.resolve()):return ['SELF_AUTHORIZED_TRUST_ROOT']
        trust=json.loads(_read(trust_path,65536))
        if trust.get('environment')!='PRODUCTION' or trust.get('testOnly') is not False:return ['TEST_ONLY_TRUST_ROOT']
        signature=manifest.get('signature',{})
        if signature.get('algorithm')!='Ed25519':return ['INVALID_COLLECTOR_SIGNATURE']
        key=trust['keys'][signature['keyId']]
        if not utc(key['validFrom'])<=now<utc(key['validUntil']):return ['COLLECTOR_KEY_OUTSIDE_VALIDITY']
        public=load_pem_public_key(key['publicKeyPem'].encode())
        if not isinstance(public,Ed25519PublicKey):return ['INVALID_COLLECTOR_KEY']
        unsigned={k:v for k,v in manifest.items() if k!='signature'}
        payload=json.dumps(unsigned,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        public.verify(base64.b64decode(signature['value'],validate=True),payload)
        return []
    except Exception:return ['INVALID_COLLECTOR_SIGNATURE_OR_ROOT']

def _capacity(root,gate,manifest,records):
    errors=[];metrics=gate.get('metrics',{});stages=metrics.get('stages',{}) if isinstance(metrics,dict) else {}
    if not isinstance(stages,dict) or set(stages)!=set(STAGES):return ['MISSING_STAGED_CAPACITY_EVIDENCE']
    previous=None
    for name in STAGES:
        stage=stages[name]
        if not isinstance(stage,dict):errors.append('INVALID_CAPACITY_STAGE:'+name);continue
        if stage.get('status')!='PASSED' or stage.get('testOnly') is not False or stage.get('generatorLimited') is not False:errors.append('CAPACITY_STAGE_NOT_VALID:'+name)
        if any(stage.get(key)!=manifest.get(key) for key in IDENTITY_BINDINGS):errors.append('STAGE_BINDING_MISMATCH:'+name)
        try:
            begin,finish=utc(stage['startedAt']),utc(stage['finishedAt'])
            if finish<=begin or previous and begin<previous:errors.append('INVALID_STAGE_ORDER:'+name)
            if begin<utc(gate['startedAt']) or finish>utc(gate['finishedAt']):errors.append('STAGE_OUTSIDE_GATE_INTERVAL:'+name)
            if name=='soak-24h' and (finish-begin).total_seconds()<86400:errors.append('SOAK_DURATION_INSUFFICIENT')
            previous=finish
        except Exception:errors.append('INVALID_STAGE_ORDER:'+name)
        for kind in ('control','relay','activation','clientDelivery'):
            histogram=mapping(stage.get('histograms')).get(kind,{})
            if not isinstance(histogram,dict) or any(not number(histogram.get(p)) for p in ('p50','p95','p99','p999')) or not number(histogram.get('count'),1) or histogram.get('rawArtifact') not in records:errors.append('MISSING_RAW_PERCENTILES:'+name+':'+kind)
            elif not histogram['p50']<=histogram['p95']<=histogram['p99']<=histogram['p999']:errors.append('INVALID_PERCENTILE_ORDER:'+name+':'+kind)
            elif kind=='control' and (histogram['p95']>150 or histogram['p99']>500):errors.append('CONTROL_LATENCY_TARGET_MISSED:'+name)
            elif kind=='relay' and (histogram['p95']>50 or histogram['p99']>150):errors.append('RELAY_LATENCY_TARGET_MISSED:'+name)
            elif kind=='activation' and (histogram['p95']>150 or histogram['p99']>500):errors.append('ACTIVATION_LATENCY_TARGET_MISSED:'+name)
            elif kind=='clientDelivery' and (histogram['p95']>500 or histogram['p99']>1000):errors.append('DELIVERY_LATENCY_TARGET_MISSED:'+name)
        resource=stage.get('resourceMeasurements',{})
        if not isinstance(resource,dict) or any(k not in resource or resource[k] is None for k in RESOURCE_METRICS):errors.append('MISSING_RESOURCE_MEASUREMENTS:'+name)
        workers=stage.get('workers',[])
        if not isinstance(workers,list) or not workers:errors.append('MISSING_WORKER_EVIDENCE:'+name)
        else:
            end=0;count=len(workers);ips=set();hosts=set()
            for index,worker in enumerate(workers):
                if not isinstance(worker,dict) or worker.get('workerIndex')!=index or worker.get('workerCount')!=count or worker.get('testOnly') is not False or worker.get('summaryArtifact') not in records or worker.get('rawHistogramArtifact') not in records or worker.get('generatorSamplesArtifact') not in records:errors.append('INVALID_WORKER_EVIDENCE:'+name);continue
                start,stop=worker.get('socketStart'),worker.get('socketEnd')
                if type(start) is not int or type(stop) is not int or start!=end or stop<start:errors.append('WORKER_RANGE_GAP_OR_OVERLAP:'+name)
                else:end=stop
                if worker.get('sourceIp') in ips or not worker.get('sourceIp') or worker.get('hostId') in hosts or not worker.get('hostId'):errors.append('WORKER_SOURCE_NOT_DISTRIBUTED:'+name)
                ips.add(worker.get('sourceIp'));hosts.add(worker.get('hostId'))
                if worker.get('generatorLimited') is not False or any(mapping(worker.get('headroom')).get(k) is not True for k in ('cpu','nic','fd','eventLoop','pendingOperations','pendingBytes')):errors.append('GENERATOR_HEADROOM_NOT_PROVEN:'+name)
            if end!=stage.get('observedSockets'):errors.append('OBSERVED_SOCKETS_NOT_COVERED:'+name)
        minimum={'10k':10000,'100k':100000,'200k-per-cell':200000,'multi-cell':400000,'p0-10m':10000000,'p2':10000000,'p2-n-minus-one':10000000,'soak-24h':10000000}[name]
        if not number(stage.get('observedSockets'),minimum):errors.append('STAGE_SOCKET_TARGET_MISSED:'+name)
        if name in ('p2','p2-n-minus-one','soak-24h'):
            expected={'distinctUsers':8000000,'establishedCalls':3000000,'callAttemptsPerSecond':10000,'inboundSetupFramesPerSecond':500000,'registrationsPerSecond':20000}
            for field,target in expected.items():
                if not number(mapping(stage.get('observed')).get(field),target):errors.append('P2_TARGET_MISSED:'+name+':'+field)
            if not number(mapping(stage.get('observed')).get('crossCellRatio'),.97,.99):errors.append('P2_CROSS_CELL_TARGET_MISSED:'+name)
        if name=='soak-24h' and not number(stage.get('durationSeconds'),86400):errors.append('SOAK_DURATION_INSUFFICIENT')
    return errors

def _verify(root,trust_file=None,now=None):
    root=Path(root);now=now or datetime.now(timezone.utc);report={'decision':'NOT_QUALIFIED','candidateId':root.name,'errors':[]};errors=report['errors']
    manifest_file=root/'manifest.yaml'
    if not manifest_file.is_file():errors.append('MISSING_MANIFEST');return report
    try:manifest=yaml.load(_read(manifest_file),Loader=StrictYaml)
    except Exception:errors.append('INVALID_MANIFEST');return report
    if not isinstance(manifest,dict):errors.append('INVALID_MANIFEST');return report
    errors.extend(candidate_errors(root,manifest))
    if manifest.get('manifestVersion')!=1:errors.append('UNSUPPORTED_MANIFEST_VERSION')
    if manifest.get('testOnly') is not False:errors.append('TEST_ONLY_CANDIDATE')
    for key in BINDINGS:
        if not isinstance(manifest.get(key),str) or not re.fullmatch('[0-9a-f]{64}',manifest[key]):errors.append('MISSING_BINDING:'+key)
    try:
        built,valid_until=utc(manifest['builtAt']),utc(manifest['evidenceValidUntil'])
        if not built<=now<valid_until or valid_until-built>timedelta(days=7):errors.append('STALE_CANDIDATE_WINDOW')
    except Exception:errors.append('INVALID_CANDIDATE_WINDOW')
    errors.extend(_signature(root,manifest,trust_file,now))
    records=manifest.get('artifacts',{})
    if not isinstance(records,dict) or not records or len(records)>4096:errors.append('INVALID_ARTIFACT_INDEX');records={}
    for name,record in records.items():
        try:
            path=artifact_path(root,name)
            if not isinstance(record,dict) or record.get('sha256')!=_sha(path) or record.get('bytes')!=path.stat().st_size:errors.append('ARTIFACT_HASH_OR_SIZE_MISMATCH:'+name)
        except Exception:errors.append('MISSING_OR_UNSAFE_ARTIFACT:'+str(name))
    for name in REPORTS:
        if name not in records:errors.append('MISSING_REPORT:'+name)
    gates=manifest.get('gates',{})
    if not isinstance(gates,dict):gates={}
    if set(gates)!=set(GATES):errors.append('INCOMPLETE_GATE_SET')
    for name in GATES:
        gate=gates.get(name);errors.extend(gate_errors(name,gate,manifest,now))
        if not isinstance(gate,dict):continue
        for artifact in gate.get('artifacts',[]):
            if artifact not in records:errors.append('UNINDEXED_GATE_ARTIFACT:'+name)
        for receipt in ('faultTimeline','physicalFenceReceipt','acknowledgedWalReceipt','outsideBackupEpochHighWaterReceipt'):
            if gate.get(receipt) and gate[receipt] not in records:errors.append('UNINDEXED_RECEIPT:'+name+':'+receipt)
        if name=='capacity':errors.extend(_capacity(root,gate,manifest,records))
    metadata=manifest.get('environment',{})
    if not isinstance(metadata,dict) or any(not metadata.get(k) for k in ('instanceTypes','kernel','jvm','podRequestsLimits','topology','dependencyVersions','datasetCardinalities','generatorModel','seedScenario','originalFaultTimelines')):errors.append('MISSING_ENVIRONMENT_METADATA')
    envelope=manifest.get('declaredEnvelope',{})
    if not isinstance(envelope,dict) or not isinstance(envelope.get('name'),str) or not re.fullmatch('[A-Z0-9_:-]{1,96}',envelope['name']) or envelope.get('sockets')!=10000000:errors.append('DECLARED_ENVELOPE_NOT_MEASURED_10M')
    errors[:]=list(dict.fromkeys(errors))
    if not errors:report['decision']='10M_QUALIFIED:'+envelope['name']
    return report

def verify(root,trust_file=None,now=None):
    try:return _verify(root,trust_file,now)
    except Exception:
        # Untrusted nested evidence may be structurally invalid. Never turn parsing failure into qualification.
        return {'decision':'NOT_QUALIFIED','candidateId':Path(root).name,'errors':['INVALID_EVIDENCE_STRUCTURE']}

def main():
    parser=argparse.ArgumentParser(description='Fail closed unless every same-candidate production gate is proven')
    parser.add_argument('candidate',type=Path);parser.add_argument('--trust-file',type=Path)
    args=parser.parse_args();report=verify(args.candidate,args.trust_file);print(json.dumps(report,sort_keys=True,allow_nan=False));return 0 if not report['errors'] else 1
if __name__=='__main__':raise SystemExit(main())
