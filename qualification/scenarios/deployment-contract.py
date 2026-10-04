#!/usr/bin/env python3
"""Validate rendered objects, never infer safety from template substrings."""
import pathlib, re, sys, yaml
objects = list(yaml.safe_load_all(pathlib.Path(sys.argv[1]).read_text()))
objects = [o for o in objects if o]
root = pathlib.Path(sys.argv[2])
def one(kind, suffix):
    found = [o for o in objects if o['kind'] == kind and o['metadata']['name'].endswith(suffix)]
    assert len(found) == 1, (kind, suffix, len(found))
    return found[0]
for component, replicas, grace in [('actor', 6, 90), ('gateway', 6, 300), ('control', 3, 90)]:
    d = one('Deployment', '-'+component)
    s, p = d['spec'], d['spec']['template']['spec']
    assert s['replicas'] == replicas
    assert s['strategy']['rollingUpdate'] == {'maxSurge': 1, 'maxUnavailable': 0}
    assert p['terminationGracePeriodSeconds'] == grace
    spread = p['topologySpreadConstraints'][0]
    assert spread['topologyKey'] == 'topology.kubernetes.io/zone'
    assert spread['minDomains'] == 3 and spread['maxSkew'] == 1 and spread['whenUnsatisfiable'] == 'DoNotSchedule'
    anti = p['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]
    assert anti['topologyKey'] == 'kubernetes.io/hostname'
    assert p['securityContext']['runAsNonRoot'] and p['securityContext']['seccompProfile']['type'] == 'RuntimeDefault'
    c = p['containers'][0]
    assert re.fullmatch(r'.+@sha256:[0-9a-f]{64}', c['image'])
    assert c['securityContext']['readOnlyRootFilesystem'] and not c['securityContext']['allowPrivilegeEscalation']
    assert c['securityContext']['capabilities']['drop'] == ['ALL']
    assert c['resources']['requests'] == c['resources']['limits']
    assert c['resources']['limits']['memory'] == '4Gi'
    env = {v['name']: v.get('value') for v in c['env']}
    assert '-Xmx1536m' in env['JAVA_TOOL_OPTIONS'] and '-XX:MaxDirectMemorySize=1024m' in env['JAVA_TOOL_OPTIONS']
    assert env['SIGNALING_NATIVE_FENCE_REQUIRED'] == 'true'
    assert env['SIGNALING_MAX_INGRESS_PRODUCERS'] == '7'
    assert c['readinessProbe']['httpGet'] == {'path': '/ready', 'port': 'health'}
    assert c['livenessProbe']['httpGet'] == {'path': '/live', 'port': 'health'}
    assert any(v['name'] == 'native-contract' and v['secret']['secretName'] == 'test-only-runtime-contract' for v in p['volumes'])
    assert one('PodDisruptionBudget', '-'+component)['spec']['minAvailable'] == (4 if component in ('actor','gateway') else 2)
services = [o for o in objects if o['kind'] == 'Service']
assert all(o['spec'].get('type', 'ClusterIP') == 'ClusterIP' for o in services)
discovery = one('Service', '-c001')['spec']
assert discovery['selector']['app'] == 'webrtc-signaling' and discovery['selector']['plane'] == 'actor' and discovery['selector']['cell'] == 'c001'
assert discovery['clusterIP'] == 'None' and discovery['publishNotReadyAddresses'] is True
assert {p['port'] for p in discovery['ports']} == {25520,8558}
business = one('Service', '-actor')['spec']
assert not business.get('publishNotReadyAddresses', False)
assert {p['port'] for p in business['ports']} == {8443}
assert one('Service', '-gateway')['spec']['ports'][0]['port'] == 8443
role = one('Role', '-c001')
assert all(r['verbs'] == ['get','list','watch'] for r in role['rules'])
assert not any('secrets' in r['resources'] or '*' in r['resources'] for r in role['rules'])
assert len([o for o in objects if o['kind'] == 'RoleBinding']) == 1
np = one('NetworkPolicy', '-default-deny')['spec']
assert np['podSelector'] == {} and set(np['policyTypes']) == {'Ingress','Egress'} and not np.get('ingress') and not np.get('egress')
actor = one('NetworkPolicy', '-actor')['spec']
for rule in actor['ingress']:
    ports = {p['port'] for p in rule['ports']}
    if ports.intersection({25520,8558}):
        assert all('ipBlock' not in source and 'namespaceSelector' not in source for source in rule['from'])
for o in objects:
    if o['kind'] != 'NetworkPolicy': continue
    for direction in ('ingress','egress'):
        for r in o['spec'].get(direction, []):
            for source in r.get('from', r.get('to', [])):
                if 'ipBlock' in source:
                    assert source['ipBlock']['cidr'] not in ('0.0.0.0/0', '::/0')
for relative, name in [('deploy/postgres/cloudnativepg-cluster.yaml','signaling-cell-authority'), ('deploy/directory/cloudnativepg-directory.yaml','signaling-directory-authority')]:
    cluster = yaml.safe_load((root/relative).read_text())
    assert cluster['apiVersion'] == 'postgresql.cnpg.io/v1' and cluster['metadata']['name'] == name
    s = cluster['spec']
    assert s['instances'] == 3 and s['enableSuperuserAccess'] is False
    assert re.fullmatch(r'ghcr.io/cloudnative-pg/postgresql:17\.6@sha256:[0-9a-f]{64}', s['imageName'])
    assert s['postgresql']['parameters']['synchronous_commit'] == 'on'
    assert s['postgresql']['synchronous'] == {'method':'any','number':1,'dataDurability':'required'}
    assert s['postgresql']['syncReplicaElectionConstraint'] == {'enabled':True,'nodeLabelsAntiAffinity':['topology.kubernetes.io/zone']}
    assert s['affinity']['enablePodAntiAffinity'] and s['affinity']['podAntiAffinityType'] == 'required'
    assert s['affinity']['topologyKey'] == 'topology.kubernetes.io/zone'
    assert s['primaryUpdateStrategy'] == 'supervised' and s['primaryUpdateMethod'] == 'switchover'
    assert s['certificates']['serverTLSSecret'] and s['certificates']['clientCASecret']
    assert s['storage']['size'] and s['resources']['limits']['memory']
manifest = yaml.safe_load((root/'config/compatibility-manifest.yaml').read_text())
assert '@sha256:' in manifest['containers']['postgres'] and '@sha256:' in manifest['containers']['cloudNativePg']
pg_policy = one('NetworkPolicy', '-cell-authority')['spec']
assert pg_policy['podSelector']['matchLabels'] == {'cnpg.io/cluster':'signaling-cell-authority'}
assert set(pg_policy['policyTypes']) == {'Ingress','Egress'}
assert any(r['ports'] == [{'protocol':'TCP','port':8000}] and r['from'][0]['namespaceSelector']['matchLabels']['kubernetes.io/metadata.name'] == 'cnpg-system' for r in pg_policy['ingress'])
assert not any('ipBlock' in source for r in pg_policy['ingress'] for source in r['from'])

print('PASS: topology, quorum, bounded resources, private discovery/business, mTLS mounts, least-privilege RBAC, deny-default networking and synchronous cross-AZ durability. TEST_ONLY render; no deployment or HA qualification.')
