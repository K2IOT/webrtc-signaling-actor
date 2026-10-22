// Fake Kubernetes API for Pekko kubernetes-api discovery. LOCAL_TEST_ONLY / NOT_QUALIFIED.
// One process per cell: it answers exactly one request, the pod list of that cell's actors, on the
// cell network only. Built-ins only, no dependencies. Configuration is read once, from the
// environment (see README of the compose environment); nothing is reloaded.
// Slowloris bounds: request/headers/keep-alive timeouts are 5 s, at most 64 connections, header
// block at most 8 KiB, and every response closes the connection so an announced body is never read.
import { createServer as createHttps } from 'node:https';
import { createServer as createHttp } from 'node:http';
import { readFileSync } from 'node:fs';
import { createHash, timingSafeEqual } from 'node:crypto';
import { isIP } from 'node:net';
import { fileURLToPath } from 'node:url';

export class Rejected extends Error {
  constructor(category) {
    super(category);
    this.category = category;
  }
}

const TIMEOUT_MS = 5000;
const MAX_CONNECTIONS = 64;
const UNSPECIFIED = /^[0:.]+$|^::ffff:0\.0\.0\.0$/i;
const sha256 = (s) => createHash('sha256').update(s).digest();

function concreteIp(value, category) {
  if (typeof value !== 'string' || !isIP(value) || UNSPECIFIED.test(value)) throw new Rejected(category);
  return value;
}

function port(value, fallback) {
  const v = value ?? fallback;
  if (!/^[0-9]{1,5}$/.test(v) || Number(v) > 65535) throw new Rejected('port');
  return Number(v);
}

function readFile(path, category) {
  try {
    return readFileSync(path);
  } catch {
    throw new Rejected(category);
  }
}

function config(env) {
  const cell = env.KUBE_CELL ?? '';
  if (!/^c[0-9]{3}$/.test(cell)) throw new Rejected('cell');
  const namespace = env.KUBE_NAMESPACE ?? '';
  if (!/^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/.test(namespace)) throw new Rejected('namespace');
  const podIps = (env.KUBE_POD_IPS ?? '').split(',');
  if (podIps.some((ip) => !isIP(ip))) throw new Rejected('pod-ips');
  const listen = concreteIp(env.KUBE_LISTEN, 'listen');
  const healthListen = concreteIp(env.KUBE_HEALTH_LISTEN ?? listen, 'listen');
  const token = readFile(env.KUBE_TOKEN_FILE ?? '', 'token').toString('utf8').replace(/\r?\n$/, '');
  if (token.length < 16) throw new Rejected('token');
  return {
    cell, namespace, podIps, listen, healthListen, token,
    port: port(env.KUBE_PORT, '443'),
    healthPort: port(env.KUBE_HEALTH_PORT, '8559'),
    cert: readFile(env.KUBE_TLS_CERT ?? '', 'tls'),
    key: readFile(env.KUBE_TLS_KEY ?? '', 'tls'),
  };
}

// Same document as the SP1 integration-test fake (LocalCompositionServices.pods) plus pod
// metadata; Pekko reads items[].status.podIP/phase and spec.containers[].ports.
function podList({ cell, podIps }) {
  const labels = { app: 'webrtc-signaling', plane: 'actor', cell };
  return Buffer.from(JSON.stringify({
    items: podIps.map((ip, i) => ({
      metadata: { name: `actor-${cell}-${i + 1}`, labels },
      status: {
        podIP: ip,
        phase: 'Running',
        containerStatuses: [{ name: 'actor', state: { running: {} }, ready: false }],
      },
      spec: { containers: [{ name: 'actor', ports: [{ name: 'management', containerPort: 8558 }] }] },
    })),
  }));
}

function apiHandler(cfg) {
  const path = `/api/v1/namespaces/${cfg.namespace}/pods`;
  const selector = `app=webrtc-signaling,plane=actor,cell=${cfg.cell}`;
  const expectedAuth = sha256(`Bearer ${cfg.token}`);
  const body = podList(cfg);
  return (req, res) => {
    // No request body, header, path or token is ever logged or echoed.
    const reply = (status, headers = {}, payload) => {
      res.writeHead(status, { Connection: 'close', ...headers });
      res.end(payload);
    };
    if (req.method !== 'GET') return reply(405, { Allow: 'GET' });
    if (!timingSafeEqual(sha256(req.headers.authorization ?? ''), expectedAuth)) {
      return reply(401, { 'WWW-Authenticate': 'Bearer' });
    }
    const q = req.url.indexOf('?');
    const pathname = q < 0 ? req.url : req.url.slice(0, q);
    const query = q < 0 ? '' : req.url.slice(q + 1);
    const m = /^labelSelector=([^&]*)$/.exec(query);
    if (pathname !== path || !m) return reply(404);
    let given;
    try {
      given = decodeURIComponent(m[1]);
    } catch {
      return reply(400);
    }
    if (given !== selector) return reply(404);
    reply(200, { 'Content-Type': 'application/json', 'Content-Length': body.length }, body);
  };
}

function listen(server, port, host) {
  return new Promise((resolve, reject) => {
    server.once('error', () => reject(new Rejected('listen')));
    server.listen(port, host, () => resolve());
  });
}

const closeAll = (servers) => Promise.all(servers.map((s) => new Promise((r) => (s.listening ? s.close(r) : r()))));

function bound(server) {
  server.requestTimeout = TIMEOUT_MS;
  server.headersTimeout = TIMEOUT_MS;
  server.keepAliveTimeout = TIMEOUT_MS;
  server.maxConnections = MAX_CONNECTIONS;
  return server;
}

/**
 * Validates and loads everything first (a rejection opens no socket), then opens the health socket
 * (503), then the TLS API, then flips health to 200. `opts.beforeApiListen` is a test seam only.
 */
export async function start(env, opts = {}) {
  const cfg = config(env);
  let ready = false;
  let api;
  try {
    // TLS 1.2 floor like a real API server: the actors' Pekko discovery client offers TLSv1.2 only
    // (pekko-discovery-kubernetes-api tls-version default), so a 1.3-only server refuses it.
    api = bound(createHttps(
      { cert: cfg.cert, key: cfg.key, minVersion: 'TLSv1.2', maxHeaderSize: 8192 },
      apiHandler(cfg),
    ));
  } catch {
    throw new Rejected('tls');
  }
  const health = bound(createHttp({ maxHeaderSize: 8192 }, (req, res) => {
    const ok = req.method === 'GET' && req.url === '/healthz';
    res.writeHead(!ok ? 404 : ready ? 200 : 503, { Connection: 'close' });
    res.end();
  }));
  const servers = [api, health];
  try {
    await listen(health, cfg.healthPort, cfg.healthListen);
    await opts.beforeApiListen?.({ healthPort: health.address().port });
    await listen(api, cfg.port, cfg.listen);
  } catch (e) {
    await closeAll(servers);
    throw e;
  }
  ready = true;
  return { api, health, close: () => closeAll(servers) };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const fail = (category) => {
    process.stderr.write(`KUBE_API_REJECTED ${category}\n`);
    process.exit(1);
  };
  process.on('uncaughtException', () => fail('internal'));
  start(process.env).then(
    (s) => {
      process.stdout.write(`KUBE_API_LISTENING cell=${process.env.KUBE_CELL}\n`);
      for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => s.close().then(() => process.exit(0)));
    },
    (e) => fail(e instanceof Rejected ? e.category : 'internal'),
  );
}
