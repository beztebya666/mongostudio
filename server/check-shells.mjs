import { spawnSync } from 'node:child_process';

function firstLine(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean) || '';
}

function probe(binName, fallbackName) {
  const bin = String(process.env[binName] || fallbackName || '').trim();
  const result = spawnSync(bin, ['--version'], {
    encoding: 'utf8',
    timeout: 4000,
    windowsHide: true,
    env: {
      ...process.env,
      NO_COLOR: '1',
      TERM: 'dumb',
    },
  });

  const output = firstLine(result.stdout) || firstLine(result.stderr);
  const errorCode = String(result.error?.code || '').trim();
  const errorMessage = String(result.error?.message || '').trim();
  const status = Number.isFinite(Number(result.status)) ? Number(result.status) : null;
  const ok = !result.error && status === 0;

  return {
    env: binName,
    bin,
    ok,
    status,
    output,
    errorCode: errorCode || null,
    errorMessage: errorMessage || null,
  };
}

const checks = [
  probe('MONGO_BIN', 'mongo'),
  probe('MONGOSH_BIN', 'mongosh'),
];

const summary = {
  ok: checks.every((item) => item.ok),
  checkedAt: new Date().toISOString(),
  checks,
};

for (const item of checks) {
  if (item.ok) {
    const msg = item.output ? ` (${item.output})` : '';
    console.log(`[OK] ${item.env}=${item.bin}${msg}`);
    continue;
  }
  const details = item.errorCode
    ? `${item.errorCode}${item.errorMessage ? `: ${item.errorMessage}` : ''}`
    : `exit ${item.status ?? 'unknown'}${item.output ? `: ${item.output}` : ''}`;
  console.error(`[FAIL] ${item.env}=${item.bin} -> ${details}`);
}

if (process.argv.includes('--json')) {
  console.log(JSON.stringify(summary, null, 2));
}

if (!summary.ok) {
  process.exitCode = 1;
}
