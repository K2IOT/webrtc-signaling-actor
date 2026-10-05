#!/usr/bin/env python3
"""Integrity/completeness gate. Production observation requires an independently trusted collector."""
import argparse
import base64
import hashlib
import json
import math
import re
import struct
import zlib
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

P2_ENVELOPE={'sockets':10000000,'distinctUsers':8000000,'establishedCalls':3000000,'callAttemptsPerSecond':10000,'meanCallSeconds':300,'inboundSetupFramesPerSecond':500000,'registrationsPerSecond':20000,'crossCellRatio':.98}

def envelope_decision(manifest,records):
    envelope=mapping(manifest.get('declaredEnvelope'));errors=[];name=envelope.get('name')
    if not isinstance(name,str) or not re.fullmatch('[A-Z0-9_:-]{1,96}',name):errors.append('INVALID_ENVELOPE_NAME')
    for key in P2_ENVELOPE:
        value=envelope.get(key)
        if key=='crossCellRatio':
            if not number(value,0,1):errors.append('INVALID_ENVELOPE:'+key)
        elif type(value) is not int or value<1:errors.append('INVALID_ENVELOPE:'+key)
    if errors:return errors,'NOT_QUALIFIED'
    if envelope['sockets']>10000000 or envelope['distinctUsers']>envelope['sockets'] or envelope['establishedCalls']*2>envelope['distinctUsers']:errors.append('INCONSISTENT_DECLARED_ENVELOPE')
    if envelope['sockets']==10000000:
        for key,expected in P2_ENVELOPE.items():
            if envelope[key]!=expected:errors.append('TEN_MILLION_ENVELOPE_MISMATCH:'+key)
        decision='10M_QUALIFIED:'+name
    else:
        if not isinstance(manifest.get('capacityAdrArtifact'),str) or manifest['capacityAdrArtifact'] not in records:errors.append('LOWER_ENVELOPE_REQUIRES_CAPACITY_ADR')
        decision='PRODUCTION_QUALIFIED:'+name
    return errors,decision if not errors else 'NOT_QUALIFIED'

def required_stages(manifest):
    sockets=mapping(manifest.get('declaredEnvelope')).get('sockets',10000000)
    if type(sockets) is not int or sockets<1 or sockets>10000000:return STAGES
    if sockets==10000000:return STAGES
    preparatory=tuple(name for name,minimum in (('10k',10000),('100k',100000),('200k-per-cell',200000),('multi-cell',400000)) if sockets>=minimum)
    return preparatory+('p0-envelope','p2','p2-n-minus-one','soak-24h')

def hdr_metrics(path):
    try:
        from hdrh.histogram import HdrHistogram
        raw=_read(Path(path),4194304)
        if len(raw)<8:raise ValueError('Truncated HDR')
        cookie,length=struct.unpack('>ii',raw[:8])
        if cookie!=0x1c849314 or length!=len(raw)-8:raise ValueError('Unsupported bounded HDR encoding')
        decompressor=zlib.decompressobj();payload=decompressor.decompress(raw[8:],16777217)
        if len(payload)>16777216 or not decompressor.eof or decompressor.unconsumed_tail or decompressor.unused_data or len(payload)<40:raise ValueError('Invalid or oversized HDR payload')
        encoding,size,offset,precision,lowest,highest,ratio=struct.unpack('>iiiiqqd',payload[:40])
        if encoding!=0x1c849313 or size!=len(payload)-40 or offset!=0 or not 1<=precision<=3 or not 1<=lowest<=highest<=86400000000 or ratio!=1.0:raise ValueError('Unsupported HDR bounds or units')
        histogram=HdrHistogram.decode(raw[8:],b64_wrap=False)
        return {'count':histogram.get_total_count(),**{name:histogram.get_value_at_percentile(percentile)/1000.0 for name,percentile in (('p50',50),('p95',95),('p99',99),('p999',99.9))}}
    except Exception as invalid:raise ValueError('Raw HDR unavailable or invalid') from None

def _json_value(value):
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError('Duplicate JSON key')
            result[key]=value
        return result
    return json.loads(value,object_pairs_hook=unique,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite JSON')))

def _json(path,limit=1048576):
    return _json_value(_read(path,limit))

def resource_headroom(sample):
    if not isinstance(sample,dict):return False
    cpu,rx,tx,nic,fd,fd_limit,lag,pending,max_pending,queued,max_bytes=(sample.get(key) for key in ('cpu','nicReceiveBytesPerSecond','nicTransmitBytesPerSecond','nicCapacityBytesPerSecond','fd','fdSoftLimit','eventLoopLagNanos','pendingOperations','maxPendingOperations','pendingBytes','maxPendingBytes'))
    return (number(cpu,0,.8) and number(nic,1) and number(rx,0,nic*.8) and number(tx,0,nic*.8)
        and number(fd_limit,1) and number(fd,0,fd_limit*.8) and number(lag,0,5000000)
        and number(max_pending,1) and number(pending,0,max_pending*.8) and number(max_bytes,1) and number(queued,0,max_bytes*.8)
        and number(sample.get('sampleIntervalNanos'),1,2000000000))

def worker_source_errors(root,worker,summary,manifest,records):
    errors=[]
    for artifact,digest in (('configArtifact','configHash'),('scenarioArtifact','scenarioHash')):
        path=worker.get(artifact)
        if not isinstance(path,str) or path not in records:errors.append('WORKER_SOURCE_INPUTS_NOT_RETAINED');continue
        if not isinstance(summary.get(digest),str) or not re.fullmatch(r'[a-f0-9]{64}',summary[digest]) or _sha(artifact_path(root,path))!=summary[digest]:errors.append('WORKER_SOURCE_INPUT_HASH_MISMATCH')
    if errors:return errors
    config=_json(artifact_path(root,worker['configArtifact']),524288)
    scenario=yaml.load(_read(artifact_path(root,worker['scenarioArtifact']),65536),Loader=StrictYaml)
    if not isinstance(config,dict) or not isinstance(scenario,dict):return ['WORKER_SOURCE_CONFIGURATION_MISMATCH']
    index,count,total,limit=(config.get(key) for key in ('workerIndex','workerCount','stageSockets','localSocketLimit'))
    if any(type(value) is not int for value in (index,count,total,limit)) or not 1<=count<=4096 or not 0<=index<count or not 1<=total<=10000000 or not 1<=limit<=200000:return ['WORKER_SOURCE_CONFIGURATION_MISMATCH']
    quotient,remainder=divmod(total,count);start=quotient*index+min(index,remainder);stop=start+quotient+(index<remainder)
    sources=config.get('sourceIps');seed=config.get('seed')
    valid=(all(config.get(key)==manifest.get(key) for key in IDENTITY_BINDINGS)
        and config.get('testOnly') is False and type(seed) is int and 0<=seed<=9223372036854775807 and summary.get('seed')==seed
        and summary.get('workerIndex')==index and summary.get('workerCount')==count and summary.get('workerHostId')==config.get('workerHostId')
        and worker.get('socketStart')==start and worker.get('socketEnd')==stop and stop-start<=limit
        and isinstance(sources,list) and len(sources)==count and all(isinstance(ip,str) for ip in sources) and len(set(sources))==count
        and config.get('scheduledStartAt')==mapping(summary.get('observed')).get('scheduledStartAt')
        and scenario.get('name')==summary.get('scenario') and scenario.get('targets')==summary.get('requestedTargets'))
    if valid:
        import ipaddress
        try:
            valid=(ipaddress.ip_address(sources[index])==ipaddress.ip_address(summary.get('sourceIp'))
                and utc(summary['startedAt'])<utc(config['scheduledStartAt'])<utc(summary['finishedAt']))
        except Exception:valid=False
    if not valid:errors.append('WORKER_SOURCE_CONFIGURATION_MISMATCH')
    return errors

def worker_errors(root,worker,manifest,records):
    errors=[]
    try:
        summary=_json(artifact_path(root,worker['summaryArtifact']))
        if not isinstance(summary,dict):return ['INVALID_WORKER_SUMMARY']
        errors.extend(worker_source_errors(root,worker,summary,manifest,records))
        if summary.get('status')!='PASSED' or summary.get('testOnly') is not False or summary.get('failures')!=[]:errors.append('WORKER_RUN_NOT_PASSED')
        if any(summary.get(key)!=manifest.get(key) or key not in summary for key in IDENTITY_BINDINGS):errors.append('WORKER_CANDIDATE_BINDING_MISMATCH')
        if summary.get('sourceIp')!=worker.get('sourceIp') or summary.get('workerHostId')!=worker.get('hostId') or summary.get('workerIndex')!=worker.get('workerIndex') or summary.get('workerCount')!=worker.get('workerCount'):errors.append('WORKER_SOURCE_BINDING_MISMATCH')
        declared=mapping(summary.get('socketRange'));start,stop=worker.get('socketStart'),worker.get('socketEnd')
        if type(start) is not int or type(stop) is not int or not 0<=stop-start<=200000 or declared!={'start':start,'end':stop}:errors.append('WORKER_RANGE_OR_LOCAL_BOUND_MISMATCH')
        base=Path(worker['summaryArtifact']).parent
        for field,target in (('rawHistogram','rawHistogramArtifact'),('generatorSamples','generatorSamplesArtifact')):
            if not isinstance(summary.get(field),str) or str(base/summary[field])!=worker.get(target) or worker.get(target) not in records:errors.append('WORKER_ARTIFACT_BINDING_MISMATCH')
        raw=hdr_metrics(artifact_path(root,worker['rawHistogramArtifact']));observed=mapping(summary.get('observed'));attempts=observed.get('attempts');successes=observed.get('successes');failures=observed.get('failures')
        if not number(attempts,1) or attempts!=raw['count']:errors.append('WORKER_RAW_SAMPLE_COUNT_MISMATCH')
        if not number(successes) or not number(failures) or successes+failures!=attempts or failures!=0 or observed.get('missedIntendedArrivals')!=0:errors.append('WORKER_FAILED_OR_MISSED_ARRIVALS')
        if observed.get('peakAuthenticatedSockets')!=stop-start:errors.append('WORKER_SOCKET_TARGET_NOT_OBSERVED')
        phases=mapping(observed.get('latencies'));phase_count=0
        for name,phase in phases.items():
            if not isinstance(phase,dict) or type(phase.get('samples')) is not int or phase['samples']<0:errors.append('INVALID_WORKER_PHASE');continue
            phase_count+=phase['samples']
            if phase['samples']==0:continue
            path=mapping(worker.get('phaseArtifacts')).get(name)
            if path not in records:errors.append('MISSING_WORKER_RAW_PHASE');continue
            decoded=hdr_metrics(artifact_path(root,path))
            if decoded['count']!=phase['samples'] or any(phase.get(key+'Ms')!=decoded[key] for key in ('p50','p95','p99','p999')):errors.append('WORKER_RAW_PHASE_MISMATCH')
        if phase_count!=attempts:errors.append('WORKER_PHASE_ACCOUNTING_MISMATCH')
        run_seconds=(utc(summary['finishedAt'])-utc(observed['scheduledStartAt'])).total_seconds()
        duration=observed.get('durationSeconds')
        if type(duration) is not int or duration<1 or not 0<=run_seconds-duration<=1.25:errors.append('WORKER_CLOCK_WINDOW_MISMATCH')
        samples=artifact_path(root,worker['generatorSamplesArtifact'])
        if samples.stat().st_size>134217728:errors.append('WORKER_SAMPLES_EXCEED_BOUND')
        else:
            count=0;previous=-1;last=0;previous_workload={}
            with samples.open() as stream:
                while line:=stream.readline(8193):
                    count+=1
                    if len(line)>8192 or count>100000:raise ValueError('Worker sample bounds')
                    sample=_json_value(line)
                    elapsed=sample.get('elapsedNanos')
                    if not resource_headroom(sample) or type(elapsed) is not int or elapsed<0 or elapsed<previous:errors.append('WORKER_RESOURCE_HEADROOM_NOT_PROVEN')
                    if type(elapsed) is not int:raise ValueError('Invalid sample clock')
                    if elapsed>run_seconds*1000000000+250000000:errors.append('WORKER_CLOCK_WINDOW_MISMATCH')
                    interval=sample.get('sampleIntervalNanos')
                    gap=elapsed-max(0,previous)
                    if gap>2250000000 or not number(interval,1,2000000000) or gap>interval+250000000:errors.append('WORKER_RESOURCE_COVERAGE_INCOMPLETE')
                    workload=mapping(sample.get('workload'))
                    for field,maximum in (('authenticatedSockets',stop-start),('establishedCallerCalls',observed.get('peakEstablishedCallerCalls'))):
                        value=workload.get(field)
                        if type(value) is not int or type(maximum) is not int or not 0<=value<=maximum:errors.append('WORKER_WORKLOAD_MEASUREMENT_INVALID')
                    for field in ('callAttempts','crossCellAttempts','relayFrames','registrations','reconnects'):
                        value=workload.get(field);maximum=observed.get(field)
                        if type(value) is not int or type(maximum) is not int or not previous_workload.get(field,0)<=value<=maximum:errors.append('WORKER_WORKLOAD_MEASUREMENT_INVALID')
                    if type(workload.get('crossCellAttempts')) is int and type(workload.get('callAttempts')) is int and workload['crossCellAttempts']>workload['callAttempts']:errors.append('WORKER_WORKLOAD_MEASUREMENT_INVALID')
                    previous_workload=workload
                    previous=elapsed;last=elapsed
            if count==0 or not number(observed.get('durationSeconds'),1) or last<max(0,observed['durationSeconds']-1)*1000000000:errors.append('WORKER_RESOURCE_COVERAGE_INCOMPLETE')
    except Exception:errors.append('INVALID_OR_MISSING_WORKER_ARTIFACT')
    return list(dict.fromkeys(errors))

def resource_metric_valid(value,maximum=None,depth=0):
    if depth>4:return False
    if isinstance(value,dict):
        return 0<len(value)<=256 and all(isinstance(label,str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}',label) and resource_metric_valid(metric,maximum,depth+1) for label,metric in value.items())
    return number(value,0) and (maximum is None or value<=maximum)

def p2_source_window_errors(root,stage,envelope,minimum_seconds=300):
    """Streaming original source observations; native collectors remain independently required."""
    import heapq
    from contextlib import ExitStack
    fields=('callAttempts','crossCellAttempts','relayFrames','registrations')
    try:
        workers=stage.get('workers')
        if not isinstance(workers,list) or not 1<=len(workers)<=4096:raise ValueError('Missing bounded workers')
        def samples(stream):
            count=0;previous=-1
            while line:=stream.readline(8193):
                count+=1
                if len(line)>8192 or count>100000:raise ValueError('Source bounds')
                sample=_json_value(line);elapsed=sample.get('elapsedNanos');workload=mapping(sample.get('workload'))
                if type(elapsed) is not int or elapsed<0 or elapsed<previous:raise ValueError('Source clock')
                if any(type(workload.get(field)) is not int or workload[field]<0 for field in fields+('authenticatedSockets','establishedCallerCalls')):raise ValueError('Source workload')
                previous=elapsed
                if elapsed>0:yield elapsed,workload
        with ExitStack() as owners:
            queue=[];iterators=[];latest=[None]*len(workers);finished=[None]*len(workers)
            for index,worker in enumerate(workers):
                path=artifact_path(root,worker['generatorSamplesArtifact'])
                if path.stat().st_size>134217728:raise ValueError('Source bytes')
                iterator=samples(owners.enter_context(path.open()));iterators.append(iterator)
                elapsed,workload=next(iterator);heapq.heappush(queue,(elapsed,index,workload))
            begin=None;initial=None;qualified=False
            while queue:
                instant=queue[0][0]
                while queue and queue[0][0]==instant:
                    _,index,workload=heapq.heappop(queue);latest[index]=(instant,workload)
                    try:
                        elapsed,following=next(iterators[index]);heapq.heappush(queue,(elapsed,index,following))
                    except StopIteration:finished[index]=instant
                if any(value is None or instant-value[0]>2250000000 or end is not None and instant>end for value,end in zip(latest,finished)):
                    begin=None;continue
                values=[value[1] for value in latest]
                if sum(value['authenticatedSockets'] for value in values)<envelope['sockets'] or sum(value['establishedCallerCalls'] for value in values)<envelope['establishedCalls']:
                    begin=None;continue
                totals={field:sum(value[field] for value in values) for field in fields}
                if begin is None:begin=instant;initial=totals;continue
                seconds=(instant-begin)/1000000000
                if seconds<minimum_seconds:continue
                deltas={field:totals[field]-initial[field] for field in fields}
                if all(deltas[field]>=seconds*envelope[target] for field,target in (('callAttempts','callAttemptsPerSecond'),('relayFrames','inboundSetupFramesPerSecond'),('registrations','registrationsPerSecond'))):
                    attempts=deltas['callAttempts'];ratio=deltas['crossCellAttempts']/attempts if attempts>0 else -1
                    if max(0,envelope['crossCellRatio']-.01)<=ratio<=min(1,envelope['crossCellRatio']+.01):qualified=True
            return [] if qualified else ['NO_SIMULTANEOUS_P2_SOURCE_WINDOW']
    except Exception:return ['INVALID_P2_SOURCE_MEASUREMENTS']

def _capacity(root,gate,manifest,records):
    errors=[];metrics=gate.get('metrics',{});stages=metrics.get('stages',{}) if isinstance(metrics,dict) else {}
    if not isinstance(stages,dict) or set(stages)!=set(required_stages(manifest)):return ['MISSING_STAGED_CAPACITY_EVIDENCE']
    previous=None
    for name in required_stages(manifest):
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
            try:
                decoded=hdr_metrics(artifact_path(root,histogram.get('rawArtifact')))
                if any(histogram.get(key)!=decoded[key] for key in ('count','p50','p95','p99','p999')):errors.append('RAW_HISTOGRAM_MISMATCH:'+name+':'+kind)
            except Exception:errors.append('INVALID_RAW_HISTOGRAM:'+name+':'+kind)
        resource=stage.get('resourceMeasurements',{})
        if not isinstance(resource,dict) or any(k not in resource or resource[k] is None for k in RESOURCE_METRICS):errors.append('MISSING_RESOURCE_MEASUREMENTS:'+name)
        elif any(not resource_metric_valid(resource[field],1 if field in ('cpu','activeCallPreservation') else None) for field in RESOURCE_METRICS):errors.append('INVALID_RESOURCE_MEASUREMENTS:'+name)
        workers=stage.get('workers',[])
        if not isinstance(workers,list) or not workers:errors.append('MISSING_WORKER_EVIDENCE:'+name)
        else:
            end=0;count=len(workers);ips=set();hosts=set();scheduled_start=None
            for index,worker in enumerate(workers):
                if not isinstance(worker,dict) or worker.get('workerIndex')!=index or worker.get('workerCount')!=count or worker.get('testOnly') is not False or worker.get('summaryArtifact') not in records or worker.get('rawHistogramArtifact') not in records or worker.get('generatorSamplesArtifact') not in records:errors.append('INVALID_WORKER_EVIDENCE:'+name);continue
                errors.extend(error+':'+name+':'+str(index) for error in worker_errors(root,worker,manifest,records))
                try:
                    original=_json(artifact_path(root,worker['summaryArtifact']))
                    original_start,original_finish=utc(original['startedAt']),utc(original['finishedAt'])
                    if original_start<utc(stage['startedAt'])-timedelta(milliseconds=250) or original_finish>utc(stage['finishedAt'])+timedelta(milliseconds=250):errors.append('WORKER_OUTSIDE_STAGE_INTERVAL:'+name+':'+str(index))
                    scheduled=utc(original['observed']['scheduledStartAt'])
                    if scheduled_start is None:scheduled_start=scheduled
                    elif scheduled!=scheduled_start:errors.append('WORKER_SCHEDULED_START_MISMATCH:'+name+':'+str(index))
                except Exception:errors.append('INVALID_WORKER_STAGE_CLOCK:'+name+':'+str(index))
                start,stop=worker.get('socketStart'),worker.get('socketEnd')
                if type(start) is not int or type(stop) is not int or start!=end or stop<start:errors.append('WORKER_RANGE_GAP_OR_OVERLAP:'+name)
                else:end=stop
                if worker.get('sourceIp') in ips or not worker.get('sourceIp') or worker.get('hostId') in hosts or not worker.get('hostId'):errors.append('WORKER_SOURCE_NOT_DISTRIBUTED:'+name)
                ips.add(worker.get('sourceIp'));hosts.add(worker.get('hostId'))
                if worker.get('generatorLimited') is not False or any(mapping(worker.get('headroom')).get(k) is not True for k in ('cpu','nic','fd','eventLoop','pendingOperations','pendingBytes')):errors.append('GENERATOR_HEADROOM_NOT_PROVEN:'+name)
            if end!=stage.get('observedSockets'):errors.append('OBSERVED_SOCKETS_NOT_COVERED:'+name)
        envelope=mapping(manifest.get('declaredEnvelope')) or P2_ENVELOPE
        minimum={'10k':10000,'100k':100000,'200k-per-cell':200000,'multi-cell':400000}.get(name,envelope.get('sockets',10000000))
        if not number(stage.get('observedSockets'),minimum):errors.append('STAGE_SOCKET_TARGET_MISSED:'+name)
        if name in ('p2','p2-n-minus-one','soak-24h'):
            errors.extend(error+':'+name for error in p2_source_window_errors(root,stage,envelope,86395.5 if name=='soak-24h' else 300))
            expected={key:envelope.get(key,P2_ENVELOPE[key]) for key in ('distinctUsers','establishedCalls','callAttemptsPerSecond','inboundSetupFramesPerSecond','registrationsPerSecond')}
            for field,target in expected.items():
                if not number(mapping(stage.get('observed')).get(field),target):errors.append('P2_TARGET_MISSED:'+name+':'+field)
            ratio=envelope.get('crossCellRatio',.98)
            if not number(mapping(stage.get('observed')).get('crossCellRatio'),max(0,ratio-.01),min(1,ratio+.01)):errors.append('P2_CROSS_CELL_TARGET_MISSED:'+name)
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
    envelope_errors,decision=envelope_decision(manifest,records);errors.extend(envelope_errors)
    errors[:]=list(dict.fromkeys(errors))
    if not errors:report['decision']=decision
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
