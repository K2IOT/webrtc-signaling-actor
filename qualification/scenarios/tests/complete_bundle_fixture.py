"""Ephemeral TEST_ONLY verifier input generator, never deployed measurements or release artifacts.

False production flags exercise the parser's positive branch only. The disposable
unit signing key is not enrolled in any real environment; nothing leaves TemporaryDirectory.
"""
import base64
import hashlib
import json
import shutil
from datetime import timedelta
from pathlib import Path
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


def complete_bundle(verifier, root, now):
    root = Path(root)
    context = dict(candidateId=root.name, gitCommit='a'*40,
                   imageDigests={'actor':'sha256:'+'f'*64, 'gateway':'sha256:'+'1'*64, 'control':'sha256:'+'2'*64},
                   **{key:'a'*64 for key in verifier.BINDINGS})
    envelope = dict(name='TEST_ONLY_UNIT_ENVELOPE', sockets=4, distinctUsers=4,
                    establishedCalls=1, callAttemptsPerSecond=1, meanCallSeconds=300,
                    inboundSetupFramesPerSecond=1, registrationsPerSecond=1, crossCellRatio=1)
    start = now-timedelta(days=1, minutes=40)
    fixture = Path(__file__).parent/'fixtures'/'TEST_ONLY_latency.hdr'
    raw = verifier.hdr_metrics(fixture)
    records = {}

    def write(name, value):
        path = root/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        records[name] = dict(sha256=hashlib.sha256(path.read_bytes()).hexdigest(), bytes=path.stat().st_size)

    stages = {}
    headroom = {key:True for key in ('cpu','nic','fd','eventLoop','pendingOperations','pendingBytes')}
    for name in verifier.required_stages({'declaredEnvelope':envelope}):
        seconds = 86400 if name=='soak-24h' else 302
        scheduled = start+timedelta(seconds=1)
        finished = scheduled+timedelta(seconds=seconds)
        prefix = name+'/worker'
        config = dict(context, testOnly=False, seed=42, workerIndex=0, workerCount=1,
                      stageSockets=4, localSocketLimit=4, sourceIps=['192.0.2.1'],
                      workerHostId='TEST_ONLY_HOST', scheduledStartAt=scheduled.isoformat())
        scenario = dict(name='TEST_ONLY_UNIT_SCENARIO', targets=envelope, durationSeconds=seconds)
        write(prefix+'/config.json',json.dumps(config))
        write(prefix+'/scenario.yaml',yaml.safe_dump(scenario))
        raw_path = root/(prefix+'/latency.hdr')
        shutil.copyfile(fixture, raw_path)
        records[prefix+'/latency.hdr'] = dict(sha256=hashlib.sha256(raw_path.read_bytes()).hexdigest(),bytes=raw_path.stat().st_size)
        samples = []
        for elapsed in range(2,seconds+1,2):
            sample = dict(elapsedNanos=elapsed*1000000000, sampleIntervalNanos=2000000000,
                          cpu=.1,nicReceiveBytesPerSecond=1,nicTransmitBytesPerSecond=1,
                          nicCapacityBytesPerSecond=1000,fd=1,fdSoftLimit=100,eventLoopLagNanos=0,
                          pendingOperations=0,maxPendingOperations=100,pendingBytes=0,maxPendingBytes=1000,
                          headroom=headroom, workload=dict(authenticatedSockets=4, establishedCallerCalls=1,
                          callAttempts=elapsed,crossCellAttempts=elapsed,relayFrames=elapsed,
                          registrations=elapsed,reconnects=0))
            samples.append(json.dumps(sample,separators=(',',':')))
        write(prefix+'/generator.jsonl','\n'.join(samples)+'\n')
        summary = dict(context, status='PASSED',testOnly=False,seed=42,workerIndex=0,workerCount=1,
                       workerHostId='TEST_ONLY_HOST',sourceIp='192.0.2.1',socketRange=dict(start=0,end=4),
                       rawHistogram='latency.hdr',generatorSamples='generator.jsonl',failures=[],
                       configHash=records[prefix+'/config.json']['sha256'],scenarioHash=records[prefix+'/scenario.yaml']['sha256'],
                       scenario=scenario['name'],requestedTargets=scenario['targets'],startedAt=start.isoformat(),
                       finishedAt=finished.isoformat(),cleanupFinishedAt=(finished+timedelta(seconds=1)).isoformat(),
                       observed=dict(attempts=raw['count'],successes=raw['count'],failures=0,missedIntendedArrivals=0,
                       lateDispatches=0,peakAuthenticatedSockets=4,peakEstablishedCallerCalls=1,callAttempts=seconds,
                       crossCellAttempts=seconds,relayFrames=seconds,registrations=seconds,reconnects=0,
                       durationSeconds=seconds,workloadDurationNanos=seconds*1000000000,
                       scheduledStartAt=scheduled.isoformat(),latencies={'CONTROL':dict(samples=raw['count'],
                       **{key+'Ms':raw[key] for key in ('p50','p95','p99','p999')})}))
        write(prefix+'/summary.json',json.dumps(summary))
        worker = dict(workerIndex=0,workerCount=1,hostId='TEST_ONLY_HOST',sourceIp='192.0.2.1',socketStart=0,
                      socketEnd=4,testOnly=False,generatorLimited=False,headroom=headroom,
                      summaryArtifact=prefix+'/summary.json',rawHistogramArtifact=prefix+'/latency.hdr',
                      generatorSamplesArtifact=prefix+'/generator.jsonl',phaseArtifacts={'CONTROL':prefix+'/latency.hdr'},
                      configArtifact=prefix+'/config.json',scenarioArtifact=prefix+'/scenario.yaml')
        stages[name] = dict(context,status='PASSED',testOnly=False,generatorLimited=False,startedAt=start.isoformat(),
                            finishedAt=finished.isoformat(),durationSeconds=seconds,observedSockets=4,workers=[worker],
                            observed=envelope,resourceMeasurements={key:1 for key in verifier.RESOURCE_METRICS},
                            histograms={kind:dict(raw,rawArtifact=prefix+'/latency.hdr') for kind in ('control','relay','activation','clientDelivery')})
        start = finished+timedelta(seconds=1)

    for name in verifier.REPORTS:
        write(name,'TEST_ONLY synthetic unit input; not release evidence.\n')
    write('capacity-adr.md','TEST_ONLY unit capacity ADR stand-in.\n')
    receipt_context = dict(context,testOnly=False,receiptVersion=1,cell='c001',drillId='TEST_ONLY_DRILL',
                           sourceIdentity='TEST_ONLY_UNIT_COLLECTOR',observedAt=now.isoformat())
    write('raw-drill.json',json.dumps(dict(receipt_context,receiptType='FAULT_TIMELINE',
        events=[dict(sequence=1,kind='DRILL_STARTED',at=(now-timedelta(minutes=5)).isoformat()),
                dict(sequence=2,kind='FAULT_INJECTED',at=(now-timedelta(minutes=4)).isoformat()),
                dict(sequence=3,kind='OBSERVATION_COMPLETED',at=(now-timedelta(minutes=1)).isoformat())])))
    writer = dict(podUid='11111111-1111-4111-8111-111111111111',bootId='22222222-2222-4222-8222-222222222222',systemIdentifier='123456789',storageEpoch=1)
    write('physical-fence.json',json.dumps(dict(receipt_context,receiptType='PHYSICAL_FENCE',oldWriter=writer,
        writeAccessRevoked=True,fenceMethod='STORAGE_ACCESS_REVOKED',fencedAt=(now-timedelta(minutes=4)).isoformat(),
        promotedAt=(now-timedelta(minutes=3)).isoformat(),trafficOpenedAt=(now-timedelta(minutes=2)).isoformat())))
    write('acknowledged-wal.json',json.dumps(dict(receipt_context,receiptType='ACKNOWLEDGED_WAL',oldWriter=writer,
        recoveryMode='SYNCHRONOUS_FAILOVER',acknowledgedWalLsn='0/FF',recoveredWalLsn='1/0',
        acknowledgedTimeline=1,recoveredTimeline=2,recoveredSystemIdentifier=writer['systemIdentifier'],
        recoveredTimelineHistory=[dict(timeline=1,forkLsn='0/FF')],acknowledgedOperations=1,reconciledOperations=1)))
    write('outside-epoch.json',json.dumps(dict(receipt_context,receiptType='OUTSIDE_BACKUP_EPOCH',
        sourcePlacement='OUTSIDE_BACKUP',backupStorageEpoch=1,outsideBackupHighWater=4,restoredStorageEpoch=5)))
    gates = {}
    for name in verifier.GATES:
        gate = dict(context,status='PASSED',testOnly=False,startedAt=(now-timedelta(days=2)).isoformat(),
                    finishedAt=now.isoformat(),invariantViolations=0,checks={key:True for key in verifier.CHECKS[name]},
                    artifacts=['raw-drill.json'])
        if name in ('n-minus-one','dependency-faults','chaos-timing','dr-restore','deployment'):
            gate['faultTimeline']='raw-drill.json'
        if name=='dr-restore':
            for key in ('physicalFenceReceipt','acknowledgedWalReceipt','outsideBackupEpochHighWaterReceipt'):
                gate[key]={'physicalFenceReceipt':'physical-fence.json','acknowledgedWalReceipt':'acknowledged-wal.json','outsideBackupEpochHighWaterReceipt':'outside-epoch.json'}[key]
        if name=='capacity':gate['metrics']={'stages':stages}
        if name=='soak':gate['metrics']={'durationSeconds':86400,'retainedDatasetAgeSeconds':86400}
        if name=='n-minus-one':gate['metrics']={'eligibleReconnects':1,'p99ReconnectSeconds':1}
        gates[name]=gate
    manifest = dict(context,manifestVersion=1,testOnly=False,builtAt=(now-timedelta(days=2)).isoformat(),
                    evidenceValidUntil=(now+timedelta(days=1)).isoformat(),artifacts=records,gates=gates,
                    declaredEnvelope=envelope,capacityAdrArtifact='capacity-adr.md',
                    environment={key:'TEST_ONLY_UNIT_METADATA' for key in ('instanceTypes','kernel','jvm','podRequestsLimits',
                    'topology','dependencyVersions','datasetCardinalities','generatorModel','seedScenario','originalFaultTimelines')})
    private = Ed25519PrivateKey.generate()
    trust = root.parent/'TEST_ONLY_external_trust.json'
    trust.write_text(json.dumps(dict(environment='PRODUCTION',testOnly=False,keys={'TEST_ONLY_UNIT_KEY':dict(
        validFrom=(now-timedelta(days=3)).isoformat(),validUntil=(now+timedelta(days=1)).isoformat(),
        publicKeyPem=private.public_key().public_bytes(Encoding.PEM,PublicFormat.SubjectPublicKeyInfo).decode())})))
    def seal():
        manifest.pop('signature',None)
        payload=json.dumps(manifest,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        manifest['signature']=dict(algorithm='Ed25519',keyId='TEST_ONLY_UNIT_KEY',value=base64.b64encode(private.sign(payload)).decode())
        (root/'manifest.yaml').write_text(yaml.safe_dump(manifest))
    seal()
    return manifest,trust,seal
