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

if __name__=='__main__':unittest.main()
