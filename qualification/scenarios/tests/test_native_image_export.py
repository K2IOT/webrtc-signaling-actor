"""Disposable UNIT ONLY OCI archive inputs, never native builds or release artifacts."""
import hashlib,importlib.util,io,json,tarfile,tempfile,unittest
from pathlib import Path
module=Path(__file__).parents[1]/'export-native-image.py'
spec=importlib.util.spec_from_file_location('native_image_export',module)
exporter=importlib.util.module_from_spec(spec)
if module.exists():spec.loader.exec_module(exporter)

class NativeImageExportTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='TEST_ONLY_native_oci_export_');self.root=Path(self.temp.name)
        self.layer=b'TEST_ONLY_OPAQUE_UNIT_LAYER_BYTES';self.layer_hash=hashlib.sha256(self.layer).hexdigest()
        self.config=dict(architecture='amd64',os='linux',rootfs=dict(type='layers',diff_ids=['sha256:'+self.layer_hash]),config=dict(Labels={'org.opencontainers.image.revision':'a'*40}))
    def tearDown(self):self.temp.cleanup()
    def archive(self,mutation=None):
        config=json.loads(json.dumps(self.config));layer=self.layer
        if mutation=='wrong-revision':config['config']['Labels']['org.opencontainers.image.revision']='b'*40
        if mutation=='wrong-diff':config['rootfs']['diff_ids']=['sha256:'+'c'*64]
        if mutation=='wrong-platform':config['architecture']='arm64'
        raw=json.dumps(config,separators=(',',':')).encode();name=hashlib.sha256(raw).hexdigest()+'.json'
        manifest=[dict(Config=name,RepoTags=['TEST_ONLY_UNIT:fixture'],Layers=['layer/layer.tar'])]
        if mutation=='escape':manifest[0]['Layers']=['../outside.tar']
        if mutation=='multiple':manifest.append(manifest[0])
        path=self.root/'TEST_ONLY_archive.tar'
        with tarfile.open(path,'w') as archive:
            for member,body in (('manifest.json',json.dumps(manifest).encode()),(name,raw),('layer/layer.tar',layer)):
                info=tarfile.TarInfo(member);info.size=len(body);archive.addfile(info,io.BytesIO(body))
        return path
    def test_export_preserves_original_config_and_layer_and_builds_verified_oci_dag(self):
        destination=self.root/'TEST_ONLY_oci';result=exporter.export(self.archive(),destination,'a'*40)
        index=json.loads((destination/'index.json').read_text());descriptor=index['manifests'][0]
        manifest_bytes=(destination/'blobs/sha256'/descriptor['digest'].split(':')[1]).read_bytes()
        self.assertEqual(hashlib.sha256(manifest_bytes).hexdigest(),descriptor['digest'].split(':')[1]);self.assertEqual(result['manifestDigest'],descriptor['digest'])
        manifest=json.loads(manifest_bytes)
        for blob in [manifest['config']]+manifest['layers']:
            contents=(destination/'blobs/sha256'/blob['digest'].split(':')[1]).read_bytes()
            self.assertEqual(blob['size'],len(contents));self.assertEqual(blob['digest'],'sha256:'+hashlib.sha256(contents).hexdigest())
        self.assertEqual(manifest['layers'][0]['digest'],'sha256:'+self.layer_hash)
        self.assertEqual(result['sourceCommit'],'a'*40)
    def test_wrong_source_platform_layers_scope_or_path_is_rejected(self):
        for mutation in ('wrong-revision','wrong-diff','wrong-platform','escape','multiple'):
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):exporter.export(self.archive(mutation),self.root/mutation,'a'*40)
    def test_existing_export_is_never_overwritten(self):
        destination=self.root/'TEST_ONLY_existing';destination.mkdir()
        with self.assertRaises(FileExistsError):exporter.export(self.archive(),destination,'a'*40)
