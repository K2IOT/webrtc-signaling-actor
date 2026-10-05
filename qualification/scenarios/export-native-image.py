#!/usr/bin/env python3
"""Export original docker-save bytes as an OCI layout; this is not qualification."""
import argparse,hashlib,json,re,tarfile
from pathlib import Path,PurePosixPath

def export(archive,destination,source_commit):
    destination=Path(destination)
    if destination.exists():raise FileExistsError('Native OCI destination already exists')
    if not isinstance(source_commit,str) or not re.fullmatch('[a-f0-9]{40}',source_commit):raise ValueError('Full immutable source commit required')
    archive=Path(archive)
    if not 0<archive.stat().st_size<=4294967296:raise ValueError('Native archive exceeds bound')
    destination.mkdir(parents=True)
    blobs=destination/'blobs/sha256';blobs.mkdir(parents=True)
    def write(raw):
        digest=hashlib.sha256(raw).hexdigest();(blobs/digest).write_bytes(raw)
        return dict(digest='sha256:'+digest,size=len(raw))
    with tarfile.open(archive,'r:') as saved:
        members=saved.getmembers()
        if len(members)>4096 or len({member.name for member in members})!=len(members):raise ValueError('Native archive member bound/uniqueness')
        def original(name,maximum):
            if not isinstance(name,str) or not name or str(PurePosixPath(name))!=name or PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts:raise ValueError('Native archive path')
            try:member=saved.getmember(name)
            except KeyError:raise ValueError('Missing original native blob') from None
            if not member.isfile() or not 0<member.size<=maximum:raise ValueError('Native blob type/size')
            return saved.extractfile(member)
        def strict_json(raw):
            def pairs(items):
                result={}
                for key,value in items:
                    if key in result:raise ValueError('Duplicate native JSON key')
                    result[key]=value
                return result
            return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite native JSON')))
        manifests=strict_json(original('manifest.json',1048576).read())
        if not isinstance(manifests,list) or len(manifests)!=1 or not isinstance(manifests[0],dict):raise ValueError('Exactly one original native image required')
        manifest=manifests[0];raw_config=original(manifest.get('Config'),1048576).read();body=strict_json(raw_config)
        if not isinstance(body,dict) or body.get('os')!='linux' or body.get('architecture')!='amd64':raise ValueError('Native image platform')
        if not isinstance(body.get('config'),dict) or not isinstance(body['config'].get('Labels'),dict) or body['config']['Labels'].get('org.opencontainers.image.revision')!=source_commit:raise ValueError('Native source commit mismatch')
        paths=manifest.get('Layers');root=body.get('rootfs')
        if not isinstance(paths,list) or not 1<=len(paths)<=64 or any(not isinstance(name,str) for name in paths) or not isinstance(root,dict) or root.get('type')!='layers' or not isinstance(root.get('diff_ids'),list) or len(root['diff_ids'])!=len(paths):raise ValueError('Native layer scope')
        config=write(raw_config);config['mediaType']='application/vnd.oci.image.config.v1+json' 
        layers=[]
        for index,name in enumerate(paths):
            stream=original(name,1073741824);temporary=blobs/'pending-layer';digest=hashlib.sha256();size=0
            with temporary.open('wb') as output:
                while chunk:=stream.read(1048576):digest.update(chunk);size+=len(chunk);output.write(chunk)
            if root['diff_ids'][index]!='sha256:'+digest.hexdigest():raise ValueError('Original native layer digest mismatch')
            final=blobs/digest.hexdigest();temporary.replace(final)
            layers.append(dict(mediaType='application/vnd.oci.image.layer.v1.tar',digest='sha256:'+digest.hexdigest(),size=size))
        raw=json.dumps(dict(schemaVersion=2,mediaType='application/vnd.oci.image.manifest.v1+json',config=config,layers=layers),sort_keys=True,separators=(',',':')).encode()
        descriptor=write(raw);descriptor.update(mediaType='application/vnd.oci.image.manifest.v1+json',platform=dict(os='linux',architecture='amd64'),annotations={'org.opencontainers.image.ref.name':source_commit})
        (destination/'index.json').write_text(json.dumps(dict(schemaVersion=2,mediaType='application/vnd.oci.image.index.v1+json',manifests=[descriptor]),sort_keys=True,separators=(',',':'))+'\n')
        (destination/'oci-layout').write_text('{"imageLayoutVersion":"1.0.0"}\n')
        return dict(sourceCommit=source_commit,manifestDigest=descriptor['digest'],configDigest=config['digest'],platform='linux/amd64',status='NOT_QUALIFIED')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('archive');parser.add_argument('destination');parser.add_argument('--source-commit',required=True)
    arguments=parser.parse_args();print(json.dumps(export(arguments.archive,arguments.destination,arguments.source_commit),sort_keys=True))
