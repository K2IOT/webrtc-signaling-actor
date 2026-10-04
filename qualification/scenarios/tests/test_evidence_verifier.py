"""Synthetic verifier unit inputs; never candidate qualification measurements."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from datetime import datetime,timedelta,timezone

MODULE=Path(__file__).resolve().parents[1]/'evidence_verifier.py'
spec=importlib.util.spec_from_file_location('evidence_verifier',MODULE)
verifier=importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

class EvidenceVerifierContractTest(unittest.TestCase):
    def setUp(self):
        self.workspace=tempfile.TemporaryDirectory(prefix='TEST_ONLY_evidence_verifier_')
        self.root=Path(self.workspace.name)/'20261004-0000-aaaaaaa-ffffffffffff'
        self.root.mkdir()
        self.now=datetime(2026,10,4,12,tzinfo=timezone.utc)
    def tearDown(self):self.workspace.cleanup()
    def test_missing_manifest_and_trust_never_qualify(self):
        report=verifier.verify(self.root,None,self.now)
        self.assertEqual(report['decision'],'NOT_QUALIFIED')
        self.assertIn('MISSING_MANIFEST',report['errors'])
    def test_gate_list_covers_every_section49_9_row(self):
        self.assertEqual(set(verifier.GATES),{'functional','safety','capacity','burst-skew','soak','n-minus-one','dependency-faults','chaos-timing','security','dr-restore','deployment','operations'})
    def test_relative_artifact_escape_and_symlink_escape_are_rejected(self):
        with self.assertRaises(ValueError):verifier.artifact_path(self.root,'../outside.json')
        outside=Path(self.workspace.name)/'outside.json';outside.write_text('{}')
        (self.root/'escape.json').symlink_to(outside)
        with self.assertRaises(ValueError):verifier.artifact_path(self.root,'escape.json')
    def test_candidate_id_matches_full_git_and_actor_manifest_digest(self):
        manifest={'candidateId':self.root.name,'gitCommit':'a'*40,'imageDigests':{'actor':'sha256:'+'f'*64,'gateway':'sha256:'+'1'*64,'control':'sha256:'+'2'*64}}
        self.assertEqual(verifier.candidate_errors(self.root,manifest),[])
        manifest['imageDigests']['actor']='sha256:'+'e'*64
        self.assertIn('CANDIDATE_IMAGE_MISMATCH',verifier.candidate_errors(self.root,manifest))
    def test_test_only_safety_violation_and_stale_artifact_are_independent_blockers(self):
        context={key:'a'*64 for key in verifier.BINDINGS};context['candidateId']=self.root.name
        gate={**context,'status':'PASSED','testOnly':True,'startedAt':(self.now-timedelta(days=8)).isoformat(),'finishedAt':(self.now-timedelta(days=8)+timedelta(seconds=10)).isoformat(),'invariantViolations':1,'checks':{name:True for name in verifier.CHECKS['safety']},'artifacts':['safety.json']}
        errors=verifier.gate_errors('safety',gate,context,self.now)
        self.assertIn('TEST_ONLY_EVIDENCE:safety',errors)
        self.assertIn('STALE_EVIDENCE:safety',errors)
        self.assertIn('SAFETY_INVARIANT_VIOLATION:safety',errors)
    def test_same_candidate_configuration_mismatch_is_rejected(self):
        context={key:'a'*64 for key in verifier.BINDINGS};context['candidateId']=self.root.name
        gate={**context,'status':'PASSED','testOnly':False,'startedAt':(self.now-timedelta(minutes=2)).isoformat(),'finishedAt':self.now.isoformat(),'invariantViolations':0,'checks':{name:True for name in verifier.CHECKS['functional']},'artifacts':['functional.json']}
        gate['configurationFingerprint']='b'*64
        self.assertIn('BINDING_MISMATCH:functional:configurationFingerprint',verifier.gate_errors('functional',gate,context,self.now))
    def test_retained_data_soak_cannot_be_less_than_twenty_four_hours(self):
        context={key:'a'*64 for key in verifier.BINDINGS};context['candidateId']=self.root.name
        gate={**context,'status':'PASSED','testOnly':False,'startedAt':(self.now-timedelta(hours=1)).isoformat(),'finishedAt':self.now.isoformat(),'invariantViolations':0,'checks':{name:True for name in verifier.CHECKS['soak']},'artifacts':['soak.json'],'metrics':{'retainedDatasetAgeSeconds':86400,'durationSeconds':3600}}
        self.assertIn('SOAK_DURATION_INSUFFICIENT',verifier.gate_errors('soak',gate,context,self.now))

    def gate(self,name):
        context={key:'a'*64 for key in verifier.BINDINGS};context.update(candidateId=self.root.name,gitCommit='a'*40,imageDigests={'actor':'sha256:'+'f'*64,'gateway':'sha256:'+'1'*64,'control':'sha256:'+'2'*64})
        return context,{**context,'status':'PASSED','testOnly':False,'startedAt':(self.now-timedelta(days=1)).isoformat(),'finishedAt':self.now.isoformat(),'invariantViolations':0,'checks':{key:True for key in verifier.CHECKS[name]},'artifacts':['raw.json']}
    def test_non_finite_boolean_and_string_soak_metrics_are_rejected(self):
        for value in (float('nan'),float('inf'),True,'86400'):
            with self.subTest(value=value):
                context,gate=self.gate('soak');gate['metrics']={'durationSeconds':value,'retainedDatasetAgeSeconds':value}
                errors=verifier.gate_errors('soak',gate,context,self.now)
                self.assertIn('SOAK_DURATION_INSUFFICIENT',errors)
                self.assertIn('EMPTY_OR_UNRETAINED_DATASET:soak',errors)
    def test_soak_declared_duration_cannot_exceed_actual_timestamp_interval(self):
        context,gate=self.gate('soak');gate['startedAt']=(self.now-timedelta(hours=1)).isoformat();gate['metrics']={'durationSeconds':86400,'retainedDatasetAgeSeconds':86400}
        self.assertIn('SOAK_DURATION_INSUFFICIENT',verifier.gate_errors('soak',gate,context,self.now))
    def test_every_gate_binds_full_git_and_all_image_digests(self):
        context,gate=self.gate('functional');gate['imageDigests']={**context['imageDigests'],'gateway':'sha256:'+'3'*64}
        self.assertIn('BINDING_MISMATCH:functional:imageDigests',verifier.gate_errors('functional',gate,context,self.now))
        gate['gitCommit']='b'*40
        self.assertIn('BINDING_MISMATCH:functional:gitCommit',verifier.gate_errors('functional',gate,context,self.now))
    def test_malformed_nested_evidence_fails_closed_without_crash(self):
        import yaml
        context,gate=self.gate('capacity');gate['artifacts']={'unhashable':[]};gate['metrics']={'stages':{name:{'histograms':[]} for name in verifier.STAGES}}
        manifest={**context,'manifestVersion':1,'testOnly':False,'gates':{'capacity':gate}}
        (self.root/'manifest.yaml').write_text(yaml.safe_dump(manifest))
        report=verifier.verify(self.root,None,self.now)
        self.assertEqual(report['decision'],'NOT_QUALIFIED')
        self.assertTrue(report['errors'])

    def test_collector_signature_binds_payload_and_requires_external_trust(self):
        import base64,json
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
        private=Ed25519PrivateKey.generate();public=private.public_key().public_bytes(Encoding.PEM,PublicFormat.SubjectPublicKeyInfo).decode()
        trust={'environment':'PRODUCTION','testOnly':False,'keys':{'TEST_ONLY_unit_key':{'validFrom':(self.now-timedelta(days=1)).isoformat(),'validUntil':(self.now+timedelta(days=1)).isoformat(),'publicKeyPem':public}}}
        external=Path(self.workspace.name)/'TEST_ONLY_trust.json';external.write_text(json.dumps(trust))
        manifest={'candidateId':self.root.name,'measurement':'TEST_ONLY_unit_signature_payload'}
        payload=json.dumps(manifest,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        manifest['signature']={'algorithm':'Ed25519','keyId':'TEST_ONLY_unit_key','value':base64.b64encode(private.sign(payload)).decode()}
        self.assertEqual(verifier._signature(self.root,manifest,external,self.now),[])
        manifest['measurement']='tampered'
        self.assertIn('INVALID_COLLECTOR_SIGNATURE_OR_ROOT',verifier._signature(self.root,manifest,external,self.now))
        internal=self.root/'self-trust.json';internal.write_text(json.dumps(trust))
        self.assertEqual(verifier._signature(self.root,manifest,internal,self.now),['SELF_AUTHORIZED_TRUST_ROOT'])
        trust['testOnly']=True;external.write_text(json.dumps(trust))
        self.assertEqual(verifier._signature(self.root,manifest,external,self.now),['TEST_ONLY_TRUST_ROOT'])
    def test_capacity_stage_cannot_run_in_future_or_outside_gate_interval(self):
        context,gate=self.gate('capacity');stages={};last=self.now-timedelta(hours=12)
        for name in verifier.STAGES:
            stages[name]={**context,'status':'PASSED','testOnly':False,'generatorLimited':False,'startedAt':last.isoformat(),'finishedAt':(last+timedelta(minutes=1)).isoformat()}
            last+=timedelta(minutes=2)
        stages['p2']['finishedAt']=(self.now+timedelta(hours=1)).isoformat();gate['metrics']={'stages':stages}
        errors=verifier._capacity(self.root,gate,context,{})
        self.assertIn('STAGE_OUTSIDE_GATE_INTERVAL:p2',errors)
    def test_histogram_non_monotonic_or_non_finite_cannot_prove_latency(self):
        context,gate=self.gate('capacity');stages={};last=self.now-timedelta(hours=12)
        for name in verifier.STAGES:
            histogram={'p50':100,'p95':50,'p99':100,'p999':200,'count':1,'rawArtifact':'raw.hdr'}
            stages[name]={**context,'status':'PASSED','testOnly':False,'generatorLimited':False,'startedAt':last.isoformat(),'finishedAt':(last+timedelta(minutes=1)).isoformat(),'histograms':{'control':histogram,'relay':{**histogram,'p95':float('nan')}}}
            last+=timedelta(minutes=2)
        gate['metrics']={'stages':stages}
        errors=verifier._capacity(self.root,gate,context,{'raw.hdr':{}})
        self.assertIn('INVALID_PERCENTILE_ORDER:p2:control',errors)
        self.assertIn('MISSING_RAW_PERCENTILES:p2:relay',errors)

    def envelope(self,sockets=10000000):
        return {'name':'TEST_ONLY_UNIT_ENVELOPE','sockets':sockets,'distinctUsers':int(sockets*.8),'establishedCalls':int(sockets*.3),'callAttemptsPerSecond':int(sockets/1000),'meanCallSeconds':300,'inboundSetupFramesPerSecond':int(sockets/20),'registrationsPerSecond':int(sockets/500),'crossCellRatio':.98}
    def test_lower_measured_envelope_requires_indexed_capacity_adr_and_never_claims_10m(self):
        manifest={'declaredEnvelope':self.envelope(1000000)}
        errors,decision=verifier.envelope_decision(manifest,{})
        self.assertIn('LOWER_ENVELOPE_REQUIRES_CAPACITY_ADR',errors)
        manifest['capacityAdrArtifact']='approved-capacity-adr.md'
        errors,decision=verifier.envelope_decision(manifest,{'approved-capacity-adr.md':{}})
        self.assertEqual(errors,[])
        self.assertEqual(decision,'PRODUCTION_QUALIFIED:TEST_ONLY_UNIT_ENVELOPE')
        self.assertEqual(verifier.required_stages(manifest),('10k','100k','200k-per-cell','multi-cell','p0-envelope','p2','p2-n-minus-one','soak-24h'))
    def test_ten_million_label_requires_exact_spec_p2_envelope(self):
        manifest={'declaredEnvelope':self.envelope()}
        self.assertEqual(verifier.envelope_decision(manifest,{})[0],[])
        manifest['declaredEnvelope']['establishedCalls']=1
        self.assertIn('TEN_MILLION_ENVELOPE_MISMATCH:establishedCalls',verifier.envelope_decision(manifest,{})[0])
    def test_invalid_declared_envelope_numbers_never_qualify(self):
        for value in (float('nan'),True,-1,'10000'):
            manifest={'declaredEnvelope':self.envelope()};manifest['declaredEnvelope']['callAttemptsPerSecond']=value
            self.assertTrue(verifier.envelope_decision(manifest,{})[0])

    def test_java_hdr_fixture_is_decoded_in_microseconds_with_all_actual_samples(self):
        path=MODULE.parent/'tests'/'fixtures'/'TEST_ONLY_latency.hdr'
        decoded=verifier.hdr_metrics(path)
        self.assertEqual(decoded['count'],4)
        self.assertEqual(decoded['p50'],2.0)
        self.assertEqual(decoded['p95'],20.015)
        self.assertEqual(decoded['p99'],20.015)
        self.assertEqual(decoded['p999'],20.015)
    def test_malformed_raw_hdr_never_becomes_a_percentile_measurement(self):
        malformed=self.root/'not-a-histogram.hdr';malformed.write_bytes(b'not a histogram')
        with self.assertRaises(ValueError):verifier.hdr_metrics(malformed)

    def worker_fixture(self):
        import json,shutil
        context,_=self.gate('capacity');directory=self.root/'worker';directory.mkdir()
        histogram=MODULE.parent/'tests'/'fixtures'/'TEST_ONLY_latency.hdr';shutil.copyfile(histogram,directory/'latency.hdr');shutil.copyfile(histogram,directory/'control.hdr')
        summary={**context,'status':'PASSED','testOnly':False,'workerIndex':0,'workerCount':1,'workerHostId':'TEST_ONLY_HOST','sourceIp':'192.0.2.1','socketRange':{'start':0,'end':4},'rawHistogram':'latency.hdr','generatorSamples':'generator.jsonl','failures':[],'observed':{'attempts':4,'successes':4,'failures':0,'missedIntendedArrivals':0,'lateDispatches':0,'peakAuthenticatedSockets':4,'durationSeconds':1,'latencies':{'CONTROL':{'samples':4,'p50Ms':2.0,'p95Ms':20.015,'p99Ms':20.015,'p999Ms':20.015}}}}
        sample={'elapsedNanos':1000000000,'sampleIntervalNanos':1000000000,'cpu':.1,'nicReceiveBytesPerSecond':1,'nicTransmitBytesPerSecond':1,'nicCapacityBytesPerSecond':1000,'fd':1,'fdSoftLimit':100,'eventLoopLagNanos':0,'pendingOperations':0,'maxPendingOperations':100,'pendingBytes':0,'maxPendingBytes':1000,'headroom':{key:True for key in ('cpu','nic','fd','eventLoop','pendingOperations','pendingBytes')}}
        (directory/'summary.json').write_text(json.dumps(summary));(directory/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        descriptor={'workerIndex':0,'workerCount':1,'hostId':'TEST_ONLY_HOST','sourceIp':'192.0.2.1','socketStart':0,'socketEnd':4,'summaryArtifact':'worker/summary.json','rawHistogramArtifact':'worker/latency.hdr','generatorSamplesArtifact':'worker/generator.jsonl','phaseArtifacts':{'CONTROL':'worker/control.hdr'}}
        records={name:{} for name in ('worker/summary.json','worker/latency.hdr','worker/generator.jsonl','worker/control.hdr')}
        return context,descriptor,records,summary,sample
    def test_worker_original_samples_override_a_declared_headroom_boolean(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        self.assertEqual(verifier.worker_errors(self.root,worker,context,records),[])
        sample['cpu']=.99
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        self.assertIn('WORKER_RESOURCE_HEADROOM_NOT_PROVEN',verifier.worker_errors(self.root,worker,context,records))
    def test_worker_summary_cannot_change_source_binding_or_raw_sample_count(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture();summary['sourceIp']='192.0.2.2';summary['observed']['attempts']=3;summary['observed']['successes']=3
        (self.root/'worker'/'summary.json').write_text(json.dumps(summary))
        errors=verifier.worker_errors(self.root,worker,context,records)
        self.assertIn('WORKER_SOURCE_BINDING_MISMATCH',errors)
        self.assertIn('WORKER_RAW_SAMPLE_COUNT_MISMATCH',errors)

if __name__=='__main__':unittest.main()
