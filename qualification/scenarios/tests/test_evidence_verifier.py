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

    def test_resource_strings_booleans_and_negative_counts_are_not_measurements(self):
        context,gate=self.gate('capacity')
        for bad in ('healthy',True,-1,float('inf')):
            stages={name:{'resourceMeasurements':{field:bad for field in verifier.RESOURCE_METRICS}} for name in verifier.STAGES}
            gate['metrics']={'stages':stages}
            self.assertIn('INVALID_RESOURCE_MEASUREMENTS:p2',verifier._capacity(self.root,gate,context,{}))

    def test_resource_nested_labels_need_bounded_finite_numeric_leaves(self):
        context,gate=self.gate('capacity')
        stages={name:{'resourceMeasurements':{field:{'c001':{'actor':1}} for field in verifier.RESOURCE_METRICS}} for name in verifier.STAGES}
        stages['p2']['resourceMeasurements']['dbPools']={'c001':{'safety':'usable'}}
        gate['metrics']={'stages':stages}
        self.assertIn('INVALID_RESOURCE_MEASUREMENTS:p2',verifier._capacity(self.root,gate,context,{}))
        self.assertNotIn('INVALID_RESOURCE_MEASUREMENTS:10k',verifier._capacity(self.root,gate,context,{}))

    def test_cpu_and_preservation_ratios_cannot_exceed_one(self):
        context,gate=self.gate('capacity')
        stages={name:{'resourceMeasurements':{field:0 for field in verifier.RESOURCE_METRICS}} for name in verifier.STAGES}
        stages['p2']['resourceMeasurements']['cpu']=1.01
        stages['p2']['resourceMeasurements']['activeCallPreservation']={'c001':2}
        gate['metrics']={'stages':stages}
        self.assertIn('INVALID_RESOURCE_MEASUREMENTS:p2',verifier._capacity(self.root,gate,context,{}))

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
        import json,shutil,hashlib
        context,_=self.gate('capacity');directory=self.root/'worker';directory.mkdir()
        histogram=MODULE.parent/'tests'/'fixtures'/'TEST_ONLY_latency.hdr';shutil.copyfile(histogram,directory/'latency.hdr');shutil.copyfile(histogram,directory/'control.hdr')
        summary={**context,'status':'PASSED','testOnly':False,'workerIndex':0,'workerCount':1,'workerHostId':'TEST_ONLY_HOST','sourceIp':'192.0.2.1','socketRange':{'start':0,'end':4},'rawHistogram':'latency.hdr','generatorSamples':'generator.jsonl','failures':[],'observed':{'attempts':4,'successes':4,'failures':0,'missedIntendedArrivals':0,'lateDispatches':0,'peakAuthenticatedSockets':4,'peakEstablishedCallerCalls':0,'callAttempts':0,'crossCellAttempts':0,'relayFrames':0,'registrations':4,'reconnects':0,'durationSeconds':1,'latencies':{'CONTROL':{'samples':4,'p50Ms':2.0,'p95Ms':20.015,'p99Ms':20.015,'p999Ms':20.015}}}}
        sample={'elapsedNanos':1000000000,'sampleIntervalNanos':1000000000,'cpu':.1,'nicReceiveBytesPerSecond':1,'nicTransmitBytesPerSecond':1,'nicCapacityBytesPerSecond':1000,'fd':1,'fdSoftLimit':100,'eventLoopLagNanos':0,'pendingOperations':0,'maxPendingOperations':100,'pendingBytes':0,'maxPendingBytes':1000,'headroom':{key:True for key in ('cpu','nic','fd','eventLoop','pendingOperations','pendingBytes')}}
        sample['workload']={'authenticatedSockets':4,'establishedCallerCalls':0,'callAttempts':0,'crossCellAttempts':0,'relayFrames':0,'registrations':4,'reconnects':0}
        scheduled=(self.now-timedelta(minutes=1)).isoformat()
        config={**context,'testOnly':False,'seed':42,'workerIndex':0,'workerCount':1,'stageSockets':4,'localSocketLimit':4,'sourceIps':['192.0.2.1'],'workerHostId':'TEST_ONLY_HOST','scheduledStartAt':scheduled}
        scenario={'name':'TEST_ONLY_UNIT_SCENARIO','targets':{'sockets':4},'durationSeconds':1}
        (directory/'config.json').write_text(json.dumps(config));(directory/'scenario.yaml').write_text(json.dumps(scenario))
        summary.update({'seed':42,'scenario':scenario['name'],'requestedTargets':scenario['targets'],'configHash':hashlib.sha256((directory/'config.json').read_bytes()).hexdigest(),'scenarioHash':hashlib.sha256((directory/'scenario.yaml').read_bytes()).hexdigest(),'startedAt':(self.now-timedelta(minutes=2)).isoformat(),'finishedAt':(self.now-timedelta(seconds=59)).isoformat()})
        summary['observed']['scheduledStartAt']=scheduled
        summary['observed']['workloadDurationNanos']=1000000000
        summary['cleanupFinishedAt']=(self.now-timedelta(seconds=58)).isoformat()
        (directory/'summary.json').write_text(json.dumps(summary));(directory/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        descriptor={'workerIndex':0,'workerCount':1,'hostId':'TEST_ONLY_HOST','sourceIp':'192.0.2.1','socketStart':0,'socketEnd':4,'summaryArtifact':'worker/summary.json','rawHistogramArtifact':'worker/latency.hdr','generatorSamplesArtifact':'worker/generator.jsonl','phaseArtifacts':{'CONTROL':'worker/control.hdr'},'configArtifact':'worker/config.json','scenarioArtifact':'worker/scenario.yaml'}
        records={name:{} for name in ('worker/summary.json','worker/latency.hdr','worker/generator.jsonl','worker/control.hdr','worker/config.json','worker/scenario.yaml')}
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

    def test_worker_samples_reject_duplicate_json_keys(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        line=json.dumps(sample).replace('"cpu": 0.1','"cpu": 0.99, "cpu": 0.1')
        (self.root/'worker'/'generator.jsonl').write_text(line+'\n')
        self.assertIn('INVALID_OR_MISSING_WORKER_ARTIFACT',verifier.worker_errors(self.root,worker,context,records))
    def test_worker_samples_cannot_hide_an_unobserved_interval(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        summary['observed']['durationSeconds']=100
        (self.root/'worker'/'summary.json').write_text(json.dumps(summary))
        last={**sample,'elapsedNanos':100000000000}
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n'+json.dumps(last)+'\n')
        self.assertIn('WORKER_RESOURCE_COVERAGE_INCOMPLETE',verifier.worker_errors(self.root,worker,context,records))
    def test_worker_samples_cannot_start_at_the_end_of_the_run(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        summary['observed']['durationSeconds']=100;sample['elapsedNanos']=100000000000
        (self.root/'worker'/'summary.json').write_text(json.dumps(summary))
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        self.assertIn('WORKER_RESOURCE_COVERAGE_INCOMPLETE',verifier.worker_errors(self.root,worker,context,records))
    def test_worker_elapsed_gap_must_match_the_measured_sample_interval(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        summary['observed']['durationSeconds']=3
        (self.root/'worker'/'summary.json').write_text(json.dumps(summary))
        last={**sample,'elapsedNanos':2800000000,'sampleIntervalNanos':100000000}
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n'+json.dumps(last)+'\n')
        self.assertIn('WORKER_RESOURCE_COVERAGE_INCOMPLETE',verifier.worker_errors(self.root,worker,context,records))

    def test_worker_samples_must_measure_live_workload_as_well_as_headroom(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture();sample.pop('workload')
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        self.assertIn('WORKER_WORKLOAD_MEASUREMENT_INVALID',verifier.worker_errors(self.root,worker,context,records))
    def test_worker_cumulative_workload_counters_cannot_exceed_original_summary(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture();sample['workload']['registrations']=5
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        self.assertIn('WORKER_WORKLOAD_MEASUREMENT_INVALID',verifier.worker_errors(self.root,worker,context,records))
    def test_worker_cumulative_counters_cannot_regress_or_use_boolean_counts(self):
        import json,copy
        context,worker,records,summary,sample=self.worker_fixture();summary['observed']['durationSeconds']=2
        (self.root/'worker'/'summary.json').write_text(json.dumps(summary))
        last=copy.deepcopy(sample);last['elapsedNanos']=2000000000;last['workload']['registrations']=3
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n'+json.dumps(last)+'\n')
        self.assertIn('WORKER_WORKLOAD_MEASUREMENT_INVALID',verifier.worker_errors(self.root,worker,context,records))
        sample['workload']['establishedCallerCalls']=False
        (self.root/'worker'/'generator.jsonl').write_text(json.dumps(sample)+'\n')
        self.assertIn('WORKER_WORKLOAD_MEASUREMENT_INVALID',verifier.worker_errors(self.root,worker,context,records))


    def test_original_source_config_and_scenario_must_be_retained(self):
        context,worker,records,summary,sample=self.worker_fixture();records.pop('worker/config.json')
        self.assertIn('WORKER_SOURCE_INPUTS_NOT_RETAINED',verifier.worker_errors(self.root,worker,context,records))

    def test_original_source_hash_cannot_be_replaced_by_a_declared_config_fingerprint(self):
        context,worker,records,summary,sample=self.worker_fixture()
        (self.root/'worker/config.json').write_text('{}')
        self.assertIn('WORKER_SOURCE_INPUT_HASH_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def test_source_config_test_only_and_native_partition_override_summary_claims(self):
        import json,hashlib
        context,worker,records,summary,sample=self.worker_fixture()
        path=self.root/'worker/config.json';config=json.loads(path.read_text());config['testOnly']=True;config['stageSockets']=5;path.write_text(json.dumps(config))
        summary['configHash']=hashlib.sha256(path.read_bytes()).hexdigest();(self.root/'worker/summary.json').write_text(json.dumps(summary))
        self.assertIn('WORKER_SOURCE_CONFIGURATION_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def test_common_source_start_cannot_be_changed_after_original_run(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture();summary['observed']['scheduledStartAt']=(self.now-timedelta(seconds=30)).isoformat()
        (self.root/'worker/summary.json').write_text(json.dumps(summary))
        self.assertIn('WORKER_SOURCE_CONFIGURATION_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def p2_source_fixture(self,disjoint=False,rate_scale=1):
        import json
        workers=[]
        for index in range(2):
            path='TEST_ONLY_source_'+str(index)+'.jsonl'
            workers.append({'generatorSamplesArtifact':path})
            with (self.root/path).open('w') as stream:
                for second in range(1,602 if disjoint else 302):
                    active=not disjoint or (second<=301 if index==0 else second>301)
                    calls=int(second*rate_scale)//2
                    sample={'elapsedNanos':second*1000000000,'workload':{'authenticatedSockets':300,'establishedCallerCalls':150 if active else 0,'callAttempts':calls,'crossCellAttempts':calls-calls//50,'relayFrames':int(250*second*rate_scale),'registrations':int(10*second*rate_scale)}}
                    stream.write(json.dumps(sample)+'\n')
        envelope={'sockets':600,'establishedCalls':300,'callAttemptsPerSecond':1,'inboundSetupFramesPerSecond':500,'registrationsPerSecond':20,'crossCellRatio':.98}
        return {'workers':workers},envelope

    def test_original_sources_prove_a_common_three_hundred_second_p2_window(self):
        stage,envelope=self.p2_source_fixture()
        self.assertEqual(verifier.p2_source_window_errors(self.root,stage,envelope),[])

    def test_source_peaks_at_different_times_cannot_prove_simultaneous_p2(self):
        stage,envelope=self.p2_source_fixture(disjoint=True)
        self.assertIn('NO_SIMULTANEOUS_P2_SOURCE_WINDOW',verifier.p2_source_window_errors(self.root,stage,envelope))

    def test_steady_live_calls_cannot_replace_actual_original_arrival_rates(self):
        stage,envelope=self.p2_source_fixture(rate_scale=.5)
        self.assertIn('NO_SIMULTANEOUS_P2_SOURCE_WINDOW',verifier.p2_source_window_errors(self.root,stage,envelope))

    def test_same_timestamp_worker_drop_cannot_be_hidden_by_merge_order(self):
        import json
        stage,envelope=self.p2_source_fixture()
        envelope={**envelope,'inboundSetupFramesPerSecond':400,'registrationsPerSecond':15}
        path=self.root/stage['workers'][1]['generatorSamplesArtifact']
        samples=[json.loads(line) for line in path.read_text().splitlines()]
        samples[-1]['workload']['establishedCallerCalls']=0
        path.write_text(''.join(json.dumps(sample)+'\n' for sample in samples))
        self.assertIn('NO_SIMULTANEOUS_P2_SOURCE_WINDOW',verifier.p2_source_window_errors(self.root,stage,envelope))

    def test_capacity_cannot_reuse_worker_from_outside_original_stage_window(self):
        context,worker,records,summary,sample=self.worker_fixture()
        _,gate=self.gate('capacity')
        stage={**context,'startedAt':(self.now-timedelta(seconds=30)).isoformat(),'finishedAt':self.now.isoformat(),'workers':[{**worker,'testOnly':False}]}
        gate['metrics']={'stages':{name:stage for name in verifier.STAGES}}
        self.assertIn('WORKER_OUTSIDE_STAGE_INTERVAL:p2:0',verifier._capacity(self.root,gate,context,records))

    def test_capacity_workers_cannot_have_different_original_scheduled_starts(self):
        import json,shutil,hashlib
        context,worker,records,summary,sample=self.worker_fixture()
        second=self.root/'second';shutil.copytree(self.root/'worker',second)
        summary['observed']['scheduledStartAt']=(self.now-timedelta(seconds=61)).isoformat()
        (second/'summary.json').write_text(json.dumps(summary))
        descriptor={**worker,'workerIndex':1,'workerCount':2,'testOnly':False}
        for field in ('summaryArtifact','rawHistogramArtifact','generatorSamplesArtifact','configArtifact','scenarioArtifact'):
            descriptor[field]=descriptor[field].replace('worker/','second/')
            records[descriptor[field]]={}
        _,gate=self.gate('capacity')
        stage={**context,'startedAt':(self.now-timedelta(minutes=3)).isoformat(),'finishedAt':self.now.isoformat(),'workers':[{**worker,'workerCount':2,'testOnly':False},descriptor]}
        gate['metrics']={'stages':{name:stage for name in verifier.STAGES}}
        self.assertIn('WORKER_SCHEDULED_START_MISMATCH:p2:1',verifier._capacity(self.root,gate,context,records))

    def test_worker_cannot_stretch_monotonic_samples_past_its_actual_finish(self):
        import json,copy
        context,worker,records,summary,sample=self.worker_fixture();summary['observed']['durationSeconds']=3
        (self.root/'worker/summary.json').write_text(json.dumps(summary))
        last=copy.deepcopy(sample);last['elapsedNanos']=3000000000
        (self.root/'worker/generator.jsonl').write_text(json.dumps(sample)+'\n'+json.dumps({**sample,'elapsedNanos':2000000000})+'\n'+json.dumps(last)+'\n')
        self.assertIn('WORKER_CLOCK_WINDOW_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def test_worker_monotonic_run_duration_cannot_replace_original_wall_window(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture();summary['finishedAt']=(self.now+timedelta(seconds=10)).isoformat()
        (self.root/'worker/summary.json').write_text(json.dumps(summary))
        self.assertIn('WORKER_CLOCK_WINDOW_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def test_missing_original_workload_stop_never_qualifies(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        del summary['observed']['workloadDurationNanos']
        (self.root/'worker/summary.json').write_text(json.dumps(summary))
        self.assertIn('WORKER_WORKLOAD_STOP_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def test_cleanup_cannot_finish_before_original_workload_stop(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        summary['cleanupFinishedAt']=(self.now-timedelta(seconds=60)).isoformat()
        (self.root/'worker/summary.json').write_text(json.dumps(summary))
        self.assertIn('WORKER_WORKLOAD_STOP_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

    def test_monotonic_workload_stop_must_match_original_wall_window(self):
        import json
        context,worker,records,summary,sample=self.worker_fixture()
        summary['observed']['workloadDurationNanos']=3000000000
        (self.root/'worker/summary.json').write_text(json.dumps(summary))
        self.assertIn('WORKER_WORKLOAD_STOP_MISMATCH',verifier.worker_errors(self.root,worker,context,records))

if __name__=='__main__':unittest.main()
