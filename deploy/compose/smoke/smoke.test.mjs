// Behaviour tests of smoke.mjs against in-process fakes (fake-stack.mjs). LOCAL_TEST_ONLY /
// NOT_QUALIFIED. Run: node --test smoke.test.mjs. The PKI is generated per run by openssl and is
// TEST_ONLY; the fakes mirror the Java response shapes, they prove nothing about the live stack.
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { startFakeStack } from './fake-stack.mjs';
import { readTopology, resolveConfig, parseResolve } from './config.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const SMOKE = join(here, 'smoke.mjs');
const TOPOLOGY = readTopology(join(here, '..', 'topology.env'));
const opensslMissing = spawnSync('openssl', ['version']).status !== 0;
if (opensslMissing && process.env.CI) throw new Error('openssl is required under CI');
const USERS = {
  admin: { password: 'admin123', userId: TOPOLOGY.ACCOUNT_ADMIN_USER_ID },
  mod: { password: 'mod123', userId: TOPOLOGY.ACCOUNT_MOD_USER_ID },
};
const ORIGIN = TOPOLOGY.ORIGIN;
const RESOLVE = 'bootstrap.signaling.local=127.0.0.1,c001.signaling.local=127.0.0.1,c002.signaling.local=127.0.0.1';

let dir;
const pki = { text: {}, files: {} };

before(() => {
  if (opensslMissing) return;
  dir = mkdtempSync(join(tmpdir(), 'smoke-test-'));
  const run = (...args) => execFileSync('openssl', args, { cwd: dir, stdio: 'pipe' });
  const ec = ['-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes'];
  const ca = (name) => run('req', '-x509', ...ec, '-keyout', `${name}.key`, '-out', `${name}.crt`,
    '-subj', `/CN=TEST_ONLY ${name}`, '-days', '2', '-addext', 'basicConstraints=critical,CA:TRUE',
    '-addext', 'keyUsage=critical,keyCertSign');
  const leaf = (name, issuer, san, usage) => {
    run('req', '-new', ...ec, '-keyout', `${name}.key`, '-out', `${name}.csr`, '-subj', `/CN=TEST_ONLY ${name}`);
    writeFileSync(join(dir, `${name}.ext`), `subjectAltName=${san}\nextendedKeyUsage=${usage}\n`);
    run('x509', '-req', '-in', `${name}.csr`, '-CA', `${issuer}.crt`, '-CAkey', `${issuer}.key`, '-CAcreateserial',
      '-out', `${name}.crt`, '-days', '2', '-extfile', `${name}.ext`);
  };
  ca('workload-ca');
  ca('admin-ca');
  ca('other-ca');
  leaf('bootstrap', 'workload-ca', 'DNS:bootstrap.signaling.local', 'serverAuth');
  leaf('c001', 'workload-ca', 'DNS:c001.signaling.local', 'serverAuth');
  leaf('c002', 'workload-ca', 'DNS:c002.signaling.local', 'serverAuth');
  leaf('platform', 'workload-ca', 'IP:127.0.0.1', 'serverAuth');
  leaf('admin-client', 'admin-ca', 'URI:spiffe://signaling.local/admin/smoke', 'clientAuth');
  for (const name of ['workload-ca', 'admin-ca', 'other-ca', 'bootstrap', 'c001', 'c002', 'platform', 'admin-client']) {
    pki.files[`${name}.crt`] = join(dir, `${name}.crt`);
    pki.files[`${name}.key`] = join(dir, `${name}.key`);
    pki.text[`${name}.crt`] = readFileSync(join(dir, `${name}.crt`));
    pki.text[`${name}.key`] = readFileSync(join(dir, `${name}.key`));
  }
});
after(() => dir && rmSync(dir, { recursive: true, force: true }));

function smokeEnv(stack, extra = {}) {
  return {
    PATH: process.env.PATH,
    IDENTITY_URL: stack.identityUrl,
    BOOTSTRAP_URL: stack.bootstrapUrl,
    ADMIN_URL: stack.adminUrl,
    CA_FILE: pki.files['workload-ca.crt'],
    ADMIN_CERT_FILE: pki.files['admin-client.crt'],
    ADMIN_KEY_FILE: pki.files['admin-client.key'],
    ADMIN_CA_FILE: pki.files['admin-ca.crt'],
    ORIGIN,
    SMOKE_RESOLVE: RESOLVE,
    SMOKE_TIMEOUT_MS: '3000',
    ...extra,
  };
}

function runSmoke(env, limitMs = 60000) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [SMOKE], { env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (c) => { stdout += c; });
    child.stderr.on('data', (c) => { stderr += c; });
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error(`smoke hung\n${stdout}\n${stderr}`)); }, limitMs);
    child.on('close', (code) => { clearTimeout(timer); resolve({ code, stdout, stderr, output: stdout + stderr }); });
  });
}

async function withStack(faults, fn) {
  const stack = await startFakeStack({ pki, users: USERS, origin: ORIGIN, faults });
  try { return await fn(stack); } finally { await stack.close(); }
}

function assertFailedAt(result, step, pattern) {
  assert.equal(result.code, 1, result.output);
  const lines = result.stdout.trim().split('\n');
  const last = lines.at(-1);
  assert.match(last, new RegExp(`^SMOKE_FAILED step ${step}: `), result.output);
  if (pattern) assert.match(last, pattern, result.output);
  assert.doesNotMatch(result.output, /SMOKE_PASSED/);
  for (let n = 1; n < step; n++) assert.ok(lines.some((l) => l.startsWith(`STEP ${n} ok `)), `step ${n} passed first\n${result.output}`);
}

function assertNoSecrets(result, stack) {
  for (const token of stack.issued) {
    assert.ok(!result.output.includes(token), 'token printed');
    for (const part of token.split('.')) assert.ok(part.length < 20 || !result.output.includes(part), 'token part printed');
  }
  for (const { password } of Object.values(USERS)) assert.ok(!result.output.includes(password), 'password printed');
}

describe('smoke.mjs against the fake stack', { skip: opensslMissing && 'openssl not installed' }, () => {
  test('smoke_passes_every_step_against_the_fakes', async () => {
    await withStack({}, async (stack) => {
      const result = await runSmoke(smokeEnv(stack));
      assert.equal(result.code, 0, result.output);
      const lines = result.stdout.trim().split('\n');
      for (const [n, name] of [[1, 'login'], [2, 'bootstrap'], [3, 'wss-auth'], [4, 'cross-cell-call'], [5, 'revoke']])
        assert.ok(lines.some((l) => l.startsWith(`STEP ${n} ok ${name}`)), `STEP ${n} ok ${name}\n${result.output}`);
      assert.match(lines.at(-1), /^SMOKE_PASSED .*LOCAL_TEST_ONLY.*NOT_QUALIFIED/);
      assert.equal(lines.length, 6, result.output);
      assertNoSecrets(result, stack);
      // The revoke is token scoped (the callee's jti) so a later run can log in again.
      assert.equal(stack.revocations.length, 1);
      assert.equal(stack.revocations[0].userId, USERS.mod.userId);
      assert.match(stack.revocations[0].jti, /^[0-9a-f-]{36}$/);
      // Every frame the smoke sent was a v:1 frame with decimal-string counters.
      const sentTypes = new Set(stack.clientFrames.map((f) => f.type));
      for (const type of ['AUTH', 'INVITE', 'ACCEPT', 'NEGOTIATE_REQUEST', 'OFFER', 'ANSWER', 'ICE_CANDIDATES', 'HANGUP', 'EVENT_RECEIVED'])
        assert.ok(sentTypes.has(type), `sent ${type}`);
      for (const f of stack.clientFrames) {
        assert.equal(f.v, 1);
        for (const k of ['negotiationId', 'iceGeneration']) if (k in f) assert.match(f[k], /^[1-9][0-9]*$/);
      }
      const invite = stack.clientFrames.find((f) => f.type === 'INVITE');
      assert.equal(invite.payload.targetUserId, USERS.mod.userId);
    });
  });

  test('smoke_exits_nonzero_with_reason_on_bad_login', async () => {
    // A closed port: nothing listens on port 1 of the loopback.
    const result = await runSmoke({ PATH: process.env.PATH, IDENTITY_URL: 'http://127.0.0.1:1', SMOKE_TIMEOUT_MS: '2000' });
    assertFailedAt(result, 1, /login/);
  });

  test('smoke_fails_step_1_on_rejected_credentials', async () => {
    await withStack({}, async (stack) => {
      const result = await runSmoke(smokeEnv(stack, { SMOKE_CALLEE_PASSWORD: 'TEST_ONLY_wrong' }));
      assertFailedAt(result, 1, /HTTP 401/);
      assert.ok(!result.output.includes('TEST_ONLY_wrong'));
    });
  });

  test('smoke_fails_step_2_when_both_users_land_in_the_same_cell', async () => {
    await withStack({ sameCell: true }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack)), 2, /same cell/);
    });
  });

  test('smoke_fails_step_2_without_the_workload_ca', async () => {
    await withStack({}, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack, { CA_FILE: '' })), 2, /CA_FILE/);
      assertFailedAt(await runSmoke(smokeEnv(stack, { CA_FILE: pki.files['other-ca.crt'] })), 2, /certificate/i);
    });
  });

  test('smoke_fails_step_3_when_auth_is_rejected', async () => {
    await withStack({ authReject: true }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack)), 3, /1008 AUTHORIZATION_REJECTED/);
    });
  });

  test('smoke_fails_step_3_when_the_origin_is_not_allowed', async () => {
    await withStack({}, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack, { ORIGIN: 'https://evil.example.test' })), 3, /403/);
    });
  });

  test('smoke_fails_step_4_without_ack_committed', async () => {
    await withStack({ noAck: true }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack, { SMOKE_TIMEOUT_MS: '1500' })), 4, /INVITE.*ACK_COMMITTED/);
    });
  });

  test('smoke_fails_step_4_on_a_non_monotonic_call_version', async () => {
    await withStack({ nonMonotonic: true }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack)), 4, /ACCEPT.*callVersion/);
    });
  });

  test('smoke_fails_step_4_without_the_event_at_the_other_side', async () => {
    await withStack({ noTerminatedEvent: true }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack, { SMOKE_TIMEOUT_MS: '1500' })), 4, /CALL_TERMINATED/);
    });
  });

  test('smoke_fails_step_5_on_the_wrong_close_code', async () => {
    await withStack({ revokeClose: [1008, 'AUTH_FRESHNESS_UNKNOWN'] }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack)), 5, /1008 AUTH_FRESHNESS_UNKNOWN/);
    });
    await withStack({ revokeClose: [1000, 'NORMAL'] }, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack)), 5, /1000/);
    });
  });

  test('smoke_fails_step_5_without_admin_client_material', async () => {
    await withStack({}, async (stack) => {
      assertFailedAt(await runSmoke(smokeEnv(stack, { ADMIN_CERT_FILE: '' })), 5, /ADMIN_CERT_FILE/);
      // A client certificate that ADMIN_CA_FILE did not issue is refused before any request.
      assertFailedAt(await runSmoke(smokeEnv(stack, { ADMIN_CA_FILE: pki.files['other-ca.crt'] })), 5, /ADMIN_CA_FILE/);
      // Without the local pre-check, the admin listener itself refuses a client certificate it did not issue.
      const result = await runSmoke(smokeEnv(stack, { ADMIN_CA_FILE: '', ADMIN_CERT_FILE: pki.files['c001.crt'],
        ADMIN_KEY_FILE: pki.files['c001.key'] }));
      assertFailedAt(result, 5, /admin revoke: /);
      assert.equal(stack.revocations.length, 0, 'no revoke was accepted');
      assert.ok(!result.output.includes(String(pki.text['c001.key']).split('\n')[1]), 'key material printed');
    });
  });
});

describe('config', () => {
  test('defaults_follow_topology_env', () => {
    const config = resolveConfig({});
    assert.equal(config.identityUrl, `http://${TOPOLOGY.PUBLISH_HOST}:${TOPOLOGY.IDENTITY_PUBLISH}`);
    assert.equal(config.bootstrapUrl, `https://${TOPOLOGY.HOSTNAME_BOOTSTRAP}:${TOPOLOGY.CONTROL_PUBLISH}`);
    assert.equal(config.adminUrl, `https://${TOPOLOGY.PLATFORM_IP}:${TOPOLOGY.ADMIN_PORT}/v1/admin/revocations`);
    assert.equal(config.origin, TOPOLOGY.ORIGIN);
    assert.equal(config.caller.username, 'admin');
    assert.equal(config.callee.username, 'mod');
    assert.equal(config.caller.expectedUserId, TOPOLOGY.ACCOUNT_ADMIN_USER_ID);
    assert.equal(config.callee.expectedUserId, TOPOLOGY.ACCOUNT_MOD_USER_ID);
    assert.equal(config.revokeScope, 'token');
    // The literal values of topology.env today; a drift there must be a deliberate change here.
    assert.equal(config.identityUrl, 'http://127.0.0.1:8081');
    assert.equal(config.bootstrapUrl, 'https://bootstrap.signaling.local:8445');
    assert.equal(config.adminUrl, 'https://172.30.0.12:9444/v1/admin/revocations');
    assert.equal(config.origin, 'https://app.signaling.local');
  });

  test('env_overrides_and_validation', () => {
    const config = resolveConfig({ IDENTITY_URL: 'http://127.0.0.1:9', REVOKE_SCOPE: 'user', SMOKE_TIMEOUT_MS: '1234' });
    assert.equal(config.identityUrl, 'http://127.0.0.1:9');
    assert.equal(config.revokeScope, 'user');
    assert.equal(config.timeoutMs, 1234);
    assert.throws(() => resolveConfig({ REVOKE_SCOPE: 'everyone' }), /REVOKE_SCOPE/);
    assert.throws(() => resolveConfig({ SMOKE_TIMEOUT_MS: 'soon' }), /SMOKE_TIMEOUT_MS/);
  });

  test('smoke_resolve_maps_names_only', () => {
    assert.deepEqual([...parseResolve('a.test=127.0.0.1, B.test=::1')], [['a.test', '127.0.0.1'], ['b.test', '::1']]);
    assert.deepEqual([...parseResolve('')], []);
    assert.throws(() => parseResolve('a.test=not-an-ip'), /SMOKE_RESOLVE/);
  });
});
