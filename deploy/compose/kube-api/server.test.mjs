// Tests for the fake Kubernetes API. LOCAL_TEST_ONLY / NOT_QUALIFIED. Built-ins only.
// Run: node --test deploy/compose/kube-api/
// The CA, server certificate and token below are generated per run and are TEST_ONLY.
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { createServer as createNetServer, connect } from 'node:net';
import { request } from 'node:https';
import { request as httpRequest } from 'node:http';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { start } from './server.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const SERVER = join(here, 'server.mjs');
const opensslMissing = spawnSync('openssl', ['version']).status !== 0;
if (opensslMissing && process.env.CI) throw new Error('openssl is required under CI');

const TOKEN = 'TEST_ONLY-' + 'a1b2c3d4'.repeat(8);
const IPS = ['172.30.1.11', '172.30.1.12', '172.30.1.13', '172.30.1.14', '172.30.1.15', '172.30.1.16'];
const SELECTOR = 'app=webrtc-signaling,plane=actor,cell=c001';
const PODS = '/api/v1/namespaces/wsa-c001/pods';
const GOOD = `${PODS}?labelSelector=${encodeURIComponent(SELECTOR)}`;

let dir;
let ca;
const files = {};

before(() => {
  if (opensslMissing) return;
  dir = mkdtempSync(join(tmpdir(), 'kube-api-test-'));
  const run = (...args) => execFileSync('openssl', args, { cwd: dir, stdio: 'pipe' });
  const ec = ['-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes'];
  run('req', '-x509', ...ec, '-keyout', 'ca.key', '-out', 'ca.crt', '-subj', '/CN=TEST_ONLY CA',
    '-days', '2', '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign');
  run('req', '-new', ...ec, '-keyout', 'kube-api.key', '-out', 'kube-api.csr', '-subj', '/CN=kube-api');
  writeFileSync(join(dir, 'ext'), 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n');
  run('x509', '-req', '-in', 'kube-api.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key', '-CAcreateserial',
    '-out', 'kube-api.crt', '-days', '2', '-extfile', 'ext');
  writeFileSync(join(dir, 'kube-token'), TOKEN, { mode: 0o600 });
  ca = readFileSync(join(dir, 'ca.crt'));
  Object.assign(files, {
    KUBE_TLS_CERT: join(dir, 'kube-api.crt'),
    KUBE_TLS_KEY: join(dir, 'kube-api.key'),
    KUBE_TOKEN_FILE: join(dir, 'kube-token'),
  });
});
after(() => dir && rmSync(dir, { recursive: true, force: true }));

const baseEnv = (extra = {}) => ({
  KUBE_CELL: 'c001',
  KUBE_NAMESPACE: 'wsa-c001',
  KUBE_POD_IPS: IPS.join(','),
  KUBE_LISTEN: '127.0.0.1',
  KUBE_PORT: '0',
  KUBE_HEALTH_PORT: '0',
  ...files,
  ...extra,
});

const freePort = () => new Promise((resolve, reject) => {
  const s = createNetServer();
  s.once('error', reject);
  s.listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)); });
});

function call(port, { method = 'GET', path = GOOD, token = TOKEN, headers = {}, body, tls = {} } = {}) {
  return new Promise((resolve, reject) => {
    const h = { ...headers };
    if (token !== null) h.authorization = `Bearer ${token}`;
    if (body !== undefined) h['content-length'] = Buffer.byteLength(body);
    const r = request({ host: '127.0.0.1', port, method, path, headers: h, ca, agent: false, ...tls }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
    });
    r.on('error', reject);
    r.end(body);
  });
}

const health = (port, path = '/healthz') => new Promise((resolve, reject) => {
  httpRequest({ host: '127.0.0.1', port, path, agent: false }, (res) => {
    res.resume();
    res.on('end', () => resolve(res.statusCode));
  }).on('error', reject).end();
});

async function withServer(extraEnv, fn) {
  const s = await start(baseEnv(extraEnv));
  try { return await fn(s, s.api.address().port, s.health.address().port); } finally { await s.close(); }
}

// Runs the real server.mjs as a child process and collects everything it writes.
async function child(env, { until = null } = {}) {
  const p = spawn(process.execPath, [SERVER], { env: { PATH: process.env.PATH, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '';
  let err = '';
  p.stdout.on('data', (c) => (out += c));
  p.stderr.on('data', (c) => (err += c));
  // a server that should have refused to start must not hang the suite
  const watchdog = setTimeout(() => p.kill('SIGKILL'), 5000);
  const exited = new Promise((resolve) => p.on('exit', (code) => { clearTimeout(watchdog); resolve(code); }));
  const listening = until === 'listening'
    ? new Promise((resolve) => { const t = setInterval(() => { if (out.includes('\n')) { clearInterval(t); resolve(); } }, 20); p.on('exit', () => { clearInterval(t); resolve(); }); })
    : null;
  if (listening) await listening;
  return { p, exited, out: () => out, err: () => err };
}

const skip = opensslMissing ? 'openssl not installed' : false;

describe('fake kube-API', { skip }, () => {
  test('serves_the_exact_pod_list_shape', () => withServer({}, async (_s, port) => {
    const expected = {
      items: IPS.map((ip, i) => ({
        metadata: { name: `actor-c001-${i + 1}`, labels: { app: 'webrtc-signaling', plane: 'actor', cell: 'c001' } },
        status: { podIP: ip, phase: 'Running', containerStatuses: [{ name: 'actor', state: { running: {} }, ready: false }] },
        spec: { containers: [{ name: 'actor', ports: [{ name: 'management', containerPort: 8558 }] }] },
      })),
    };
    const r = await call(port);
    assert.equal(r.status, 200);
    assert.equal(r.headers['content-type'], 'application/json');
    assert.deepEqual(JSON.parse(r.body), expected);
    // the unencoded selector (as the Java fixture's contains() also accepted) is the same selector
    const raw = await call(port, { path: `${PODS}?labelSelector=${SELECTOR}` });
    assert.equal(raw.status, 200);
    assert.equal(raw.body, r.body, 'deterministic body');
  }));

  test('rejects_a_missing_or_wrong_token_with_401', () => withServer({}, async (_s, port) => {
    const wrongSameLength = TOKEN.slice(0, -1) + (TOKEN.endsWith('0') ? '1' : '0');
    for (const opts of [
      { token: null },
      { token: 'wrong' },
      { token: wrongSameLength },
      { token: TOKEN + 'x' },
      { token: '', headers: {} },
      { token: null, headers: { authorization: TOKEN } },
      { token: null, headers: { authorization: `bearer ${TOKEN}` } },
      { token: null, headers: { authorization: `Basic ${TOKEN}` } },
      { token: null, path: `${GOOD}&token=${TOKEN}` },
    ]) {
      const r = await call(port, opts);
      assert.equal(r.status, 401, JSON.stringify(opts));
      assert.equal(r.body, '');
    }
    // an unauthenticated caller learns nothing about which paths exist
    assert.equal((await call(port, { token: null, path: '/nope' })).status, 401);
  }));

  test('rejects_wrong_selector_namespace_or_cell_with_404', () => withServer({}, async (_s, port) => {
    const enc = encodeURIComponent;
    const sel = (s) => `${PODS}?labelSelector=${enc(s)}`;
    for (const path of [
      `/api/v1/namespaces/wsa-c002/pods?labelSelector=${enc(SELECTOR)}`,
      `/api/v1/namespaces/default/pods?labelSelector=${enc(SELECTOR)}`,
      sel('app=webrtc-signaling,plane=actor,cell=c002'),
      sel('app=webrtc-signaling,plane=gateway,cell=c001'),
      PODS,
      `${PODS}/?labelSelector=${enc(SELECTOR)}`,
      `/api/v1/namespaces/wsa-c001/pods/actor-c001-1`,
      `/api/v1/pods?labelSelector=${enc(SELECTOR)}`,
      '/',
    ]) {
      const r = await call(port, { path });
      assert.equal(r.status, 404, path);
      assert.equal(r.body, '');
    }
    assert.equal((await call(port, { path: `${PODS}?labelSelector=%E0%A4%A` })).status, 400);
  }));

  test('exact_selector_only', () => withServer({}, async (_s, port) => {
    const enc = encodeURIComponent;
    for (const s of [
      'plane=actor,app=webrtc-signaling,cell=c001', // reordered
      `${SELECTOR},extra=1`, // superset
      'app=webrtc-signaling,plane=actor', // subset
      'cell=c001',
      `${SELECTOR} `,
      '',
    ]) {
      assert.equal((await call(port, { path: `${PODS}?labelSelector=${enc(s)}` })).status, 404, JSON.stringify(s));
    }
    // other query parameters are not tolerated next to the exact selector
    assert.equal((await call(port, { path: `${GOOD}&limit=1` })).status, 404);
    assert.equal((await call(port, { path: `${GOOD}&labelSelector=cell%3Dc002` })).status, 404);
  }));

  test('does_not_log_the_token', async () => {
    const port = await freePort();
    const hp = await freePort();
    const c = await child(baseEnv({ KUBE_PORT: String(port), KUBE_HEALTH_PORT: String(hp) }), { until: 'listening' });
    try {
      await call(port);
      await call(port, { token: 'a-wrong-token-value' });
      await call(port, { token: null, headers: { cookie: 'SECRET-COOKIE' }, path: '/secret-path' });
      await call(port, { method: 'POST', body: 'SECRET-BODY' });
      assert.equal(await health(hp), 200);
    } finally {
      c.p.kill('SIGTERM');
    }
    assert.equal(await c.exited, 0, 'SIGTERM exits cleanly');
    assert.equal(c.out(), 'KUBE_API_LISTENING cell=c001\n');
    assert.equal(c.err(), '');
    for (const secret of [TOKEN, 'a-wrong-token-value', 'SECRET-COOKIE', 'secret-path', 'SECRET-BODY', dir]) {
      assert.ok(!(c.out() + c.err()).includes(secret), `output leaks ${secret}`);
    }
  });

  test('healthz_after_load_only', async () => {
    let release;
    let hp;
    const gate = new Promise((r) => (release = r));
    const starting = start(baseEnv(), { beforeApiListen: async (info) => { hp = info.healthPort; await gate; } });
    for (let i = 0; i < 500 && !hp; i++) await new Promise((r) => setTimeout(r, 10));
    try {
      // the health socket answers 503 while the API is not listening yet
      assert.ok(hp, 'health socket opens before the API');
      assert.equal(await health(hp), 503);
    } finally {
      release(); // never leave start() pending, even when an assertion failed
    }
    const s = await starting;
    try {
      assert.equal(await health(s.health.address().port), 200);
      assert.equal(await health(s.health.address().port, '/other'), 404);
      assert.equal((await call(s.api.address().port)).status, 200);
    } finally {
      await s.close();
    }
  });

  test('healthz_never_ok_when_loading_fails', async () => {
    const hp = await freePort();
    const c = await child(baseEnv({ KUBE_TOKEN_FILE: join(dir, 'missing'), KUBE_HEALTH_PORT: String(hp) }));
    assert.equal(await c.exited, 1);
    await assert.rejects(health(hp));
  });

  // The Pekko Kubernetes discovery client of the actors offers TLS 1.2 only (pekko-discovery-
  // kubernetes-api reference.conf: tls-version = "TLSv1.2"), as a real API server accepts; a
  // 1.3-only fake refused every discovery with protocol_version (Task 10 acceptance run).
  test('serves_tls_1_2_and_1_3_and_refuses_below_1_2', () => withServer({}, async (_s, port) => {
    await assert.rejects(call(port, { tls: { minVersion: 'TLSv1', maxVersion: 'TLSv1.1' } }));
    const tls12 = await call(port, { tls: { minVersion: 'TLSv1.2', maxVersion: 'TLSv1.2' } });
    assert.equal(tls12.status, 200);
    const tls13 = await call(port, { tls: { minVersion: 'TLSv1.3' } });
    assert.equal(tls13.status, 200);
  }));

  test('refuses_a_client_that_does_not_trust_the_kube_ca', () => withServer({}, async (_s, port) => {
    await assert.rejects(call(port, { tls: { ca: undefined } }));
  }));

  test('post_is_405', () => withServer({}, async (_s, port) => {
    for (const method of ['POST', 'PUT', 'PATCH', 'DELETE']) {
      for (const token of [TOKEN, null]) {
        const r = await call(port, { method, token, body: '{"x":1}' });
        assert.equal(r.status, 405, `${method} token=${token !== null}`);
        assert.equal(r.headers.allow, 'GET');
        assert.equal(r.body, '');
      }
    }
  }));

  test('bounds_slow_and_numerous_clients', () => withServer({}, async (s) => {
    assert.equal(s.api.requestTimeout, 5000);
    assert.equal(s.api.headersTimeout, 5000);
    assert.equal(s.api.keepAliveTimeout, 5000);
    assert.ok(s.api.maxConnections > 0 && s.api.maxConnections <= 128);
    assert.equal(s.health.requestTimeout, 5000);
    // a request announcing a large body is answered and the connection closed, the body is not read
    const port = s.api.address().port;
    const r = await call(port, { method: 'POST', body: 'x'.repeat(1 << 20), headers: { connection: 'keep-alive' } });
    assert.equal(r.status, 405);
    assert.equal(r.headers.connection, 'close');
  }));

  test('listens_only_on_the_given_address', async () => {
    const s = await start(baseEnv({ KUBE_LISTEN: '127.0.0.2' }));
    try {
      assert.equal(s.api.address().address, '127.0.0.2');
      assert.equal(s.health.address().address, '127.0.0.2');
      const refused = (port) => new Promise((resolve) => {
        const sock = connect({ host: '127.0.0.1', port });
        sock.on('connect', () => { sock.destroy(); resolve(false); });
        sock.on('error', () => resolve(true));
      });
      assert.ok(await refused(s.api.address().port));
      assert.ok(await refused(s.health.address().port));
    } finally {
      await s.close();
    }
  });

  test('refuses_wildcard_listen', async () => {
    for (const bad of ['0.0.0.0', '::', '0:0:0:0:0:0:0:0', '::0', '::ffff:0.0.0.0', 'localhost', 'kube-api.local', '', ' 127.0.0.1', '127.0.0.1:443']) {
      for (const key of ['KUBE_LISTEN', 'KUBE_HEALTH_LISTEN']) {
        const c = await child(baseEnv({ [key]: bad, KUBE_PORT: String(await freePort()), KUBE_HEALTH_PORT: String(await freePort()) }));
        assert.equal(await c.exited, 1, `${key}=${JSON.stringify(bad)}`);
        assert.equal(c.err(), 'KUBE_API_REJECTED listen\n');
        assert.equal(c.out(), '');
      }
    }
  });

  test('bad_env_exits_nonzero_with_one_line', async () => {
    const cases = [
      [{ KUBE_TLS_CERT: join(dir, 'absent.crt') }, 'tls'],
      [{ KUBE_TLS_KEY: join(dir, 'absent.key') }, 'tls'],
      [{ KUBE_TLS_CERT: undefined }, 'tls'],
      [{ KUBE_TLS_KEY: files.KUBE_TLS_CERT }, 'tls'], // a certificate is not a private key
      [{ KUBE_TOKEN_FILE: join(dir, 'absent-token') }, 'token'],
      [{ KUBE_TOKEN_FILE: undefined }, 'token'],
      [{ KUBE_CELL: 'c1' }, 'cell'],
      [{ KUBE_CELL: undefined }, 'cell'],
      [{ KUBE_NAMESPACE: 'Not A Namespace' }, 'namespace'],
      [{ KUBE_POD_IPS: '' }, 'pod-ips'],
      [{ KUBE_POD_IPS: '172.30.1.11,not-an-ip' }, 'pod-ips'],
      [{ KUBE_LISTEN: undefined }, 'listen'],
      [{ KUBE_PORT: 'http' }, 'port'],
    ];
    for (const [extra, category] of cases) {
      const env = baseEnv(extra);
      for (const k of Object.keys(env)) if (env[k] === undefined) delete env[k];
      const c = await child(env);
      assert.equal(await c.exited, 1, JSON.stringify(extra));
      assert.equal(c.err(), `KUBE_API_REJECTED ${category}\n`, JSON.stringify(extra));
      assert.equal(c.out(), '');
      assert.ok(!c.err().includes(dir) && !c.err().includes(TOKEN));
    }
  });

  test('an_empty_or_short_token_file_is_rejected', async () => {
    const short = join(dir, 'short-token');
    writeFileSync(short, 'abc');
    const c = await child(baseEnv({ KUBE_TOKEN_FILE: short }));
    assert.equal(await c.exited, 1);
    assert.equal(c.err(), 'KUBE_API_REJECTED token\n');
  });

  test('a_busy_port_is_rejected_without_leaving_the_health_socket_open', async () => {
    const blocker = createNetServer();
    await new Promise((r) => blocker.listen(0, '127.0.0.1', r));
    const hp = await freePort();
    try {
      const c = await child(baseEnv({ KUBE_PORT: String(blocker.address().port), KUBE_HEALTH_PORT: String(hp) }));
      assert.equal(await c.exited, 1);
      assert.equal(c.err(), 'KUBE_API_REJECTED listen\n');
      await assert.rejects(health(hp));
    } finally {
      blocker.close();
    }
  });
});
