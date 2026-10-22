// Smoke configuration: environment over topology.env defaults. LOCAL_TEST_ONLY / NOT_QUALIFIED.
// Defaults are read from deploy/compose/topology.env (or TOPOLOGY_ENV), so they cannot drift from
// the compose model. The two TEST_ONLY identity accounts are the committed signaling-identity ones.
import { readFileSync } from 'node:fs';
import { isIP } from 'node:net';
import { fileURLToPath } from 'node:url';

const DEFAULT_TOPOLOGY = fileURLToPath(new URL('../topology.env', import.meta.url));

/** Shell-sourceable KEY=value lines, optional surrounding quotes; comments and blanks ignored. */
export function readTopology(path = DEFAULT_TOPOLOGY) {
  const values = {};
  for (const line of readFileSync(path, 'utf8').split('\n')) {
    const match = /^([A-Z][A-Z0-9_]*)=(.*)$/.exec(line.trim());
    if (!match) continue;
    let value = match[2];
    if (value.length >= 2 && /^(['"]).*\1$/.test(value)) value = value.slice(1, -1);
    values[match[1]] = value;
  }
  return values;
}

/** SMOKE_RESOLVE="name=ip,name=ip": names the smoke resolves without /etc/hosts. */
export function parseResolve(spec = '') {
  const map = new Map();
  for (const item of spec.split(',').map((s) => s.trim()).filter(Boolean)) {
    const [name, ip] = item.split('=').map((s) => s?.trim());
    if (!name || !ip || !isIP(ip)) throw new Error(`SMOKE_RESOLVE entry must be name=ip, got "${item}"`);
    map.set(name.toLowerCase(), ip);
  }
  return map;
}

function positiveInt(value, name, fallback) {
  if (value === undefined || value === '') return fallback;
  if (!/^[1-9][0-9]{0,8}$/.test(value)) throw new Error(`${name} must be a positive integer of milliseconds`);
  return Number(value);
}

function required(topology, key) {
  const value = topology[key];
  if (!value) throw new Error(`topology.env has no ${key}`);
  return value;
}

/** File paths stay unread here; each step reads what it needs and names the missing variable. */
export function resolveConfig(env = process.env) {
  const topology = readTopology(env.TOPOLOGY_ENV || DEFAULT_TOPOLOGY);
  const pick = (key, fallback) => (env[key] === undefined || env[key] === '' ? fallback : env[key]);
  const revokeScope = pick('REVOKE_SCOPE', 'token');
  if (!['token', 'user'].includes(revokeScope)) throw new Error('REVOKE_SCOPE must be token or user');
  const account = (role, username, password, idKey) => {
    const name = pick(`SMOKE_${role}_USER`, username);
    return {
      username: name,
      password: pick(`SMOKE_${role}_PASSWORD`, password),
      // Only the committed accounts have a pinned id in topology.env.
      expectedUserId: name === username ? topology[idKey] || null : null,
    };
  };
  return {
    identityUrl: pick('IDENTITY_URL', `http://${required(topology, 'PUBLISH_HOST')}:${required(topology, 'IDENTITY_PUBLISH')}`),
    bootstrapUrl: pick('BOOTSTRAP_URL', `https://${required(topology, 'HOSTNAME_BOOTSTRAP')}:${required(topology, 'CONTROL_PUBLISH')}`),
    adminUrl: pick('ADMIN_URL',
      `https://${required(topology, 'PLATFORM_IP')}:${required(topology, 'ADMIN_PORT')}/v1/admin/revocations`),
    origin: pick('ORIGIN', required(topology, 'ORIGIN')),
    caFile: env.CA_FILE || '',
    adminCertFile: env.ADMIN_CERT_FILE || '',
    adminKeyFile: env.ADMIN_KEY_FILE || '',
    adminCaFile: env.ADMIN_CA_FILE || '',
    // TEST_ONLY passwords of the committed signaling-identity accounts (application.yml, compose.yaml).
    caller: account('CALLER', 'admin', 'admin123', 'ACCOUNT_ADMIN_USER_ID'),
    callee: account('CALLEE', 'mod', 'mod123', 'ACCOUNT_MOD_USER_ID'),
    revokeScope,
    resolve: parseResolve(env.SMOKE_RESOLVE || ''),
    timeoutMs: positiveInt(env.SMOKE_TIMEOUT_MS, 'SMOKE_TIMEOUT_MS', 30000),
    httpTimeoutMs: positiveInt(env.SMOKE_HTTP_TIMEOUT_MS, 'SMOKE_HTTP_TIMEOUT_MS', 5000),
    totalTimeoutMs: positiveInt(env.SMOKE_TOTAL_TIMEOUT_MS, 'SMOKE_TOTAL_TIMEOUT_MS', 300000),
  };
}
