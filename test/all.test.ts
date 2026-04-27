import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const root = process.cwd();
const baileysService = readFileSync(
  join(root, 'src/api/integrations/channel/whatsapp/whatsapp.baileys.service.ts'),
  'utf8'
);
const instanceController = readFileSync(
  join(root, 'src/api/controllers/instance.controller.ts'),
  'utf8'
);

function extractBlock(source: string, marker: string, nextMarker: string) {
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `Missing marker: ${marker}`);
  const end = source.indexOf(nextMarker, start + marker.length);
  assert.notEqual(end, -1, `Missing next marker: ${nextMarker}`);
  return source.slice(start, end);
}

const scheduleReconnectBlock = extractBlock(
  baileysService,
  'private scheduleReconnect()',
  'public async logoutInstance()'
);

assert.ok(
  !scheduleReconnectBlock.includes('const shouldForceReauthentication = !this.instance.wuid;'),
  'scheduled reconnect must not force reauthentication solely because wuid is empty'
);
assert.match(
  scheduleReconnectBlock,
  /hasAuth(?:entication)?Artifacts|hasFreshAuthArtifact/,
  'scheduled reconnect must check for an existing auth artifact before forcing reauthentication'
);
assert.match(
  scheduleReconnectBlock,
  /shouldForceReauthentication\s*=\s*!this\.instance\.wuid\s*&&\s*!has/,
  'scheduled reconnect must keep fresh QR/pairing artifacts stable while wuid is still empty'
);

const connectToWhatsappBlock = extractBlock(
  instanceController,
  'public async connectToWhatsapp',
  'public async restartInstance'
);

assert.match(
  connectToWhatsappBlock,
  /if \(state == 'connecting'\) \{\s*if \(this\.hasAuthenticationArtifacts\(instance\)\) \{\s*return this\.connectOutcomeResponse\(instanceName, instance\);\s*\}/s,
  '/instance/connect must return the existing QR/pairing artifact immediately while already connecting'
);
assert.match(
  connectToWhatsappBlock,
  /if \(state == 'close'\) \{\s*if \(this\.hasAuthenticationArtifacts\(instance\)\) \{\s*return this\.connectOutcomeResponse\(instanceName, instance\);\s*\}/s,
  '/instance/connect must not start a fresh socket when a close-state instance still has an auth artifact'
);

console.log('WhatsApp auth lifecycle regression checks passed');
