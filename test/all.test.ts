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
assert.match(
  scheduleReconnectBlock,
  /hasRecentlyScannedAuthenticationArtifact\(\)/,
  'scheduled reconnect must also keep a just-scanned QR stable even after Baileys clears QR fields'
);
assert.match(
  scheduleReconnectBlock,
  /!hasRecentlyScannedAuthArtifact/,
  'scheduled reconnect must not force reauthentication while a scanned QR is still completing'
);

const qrConsumedBlock = extractBlock(
  baileysService,
  'private async emitAuthenticationArtifactScannedUpdate()',
  'private async connectionUpdate'
);
assert.match(
  qrConsumedBlock,
  /Events\.CONNECTION_UPDATE/,
  'QR-consumed updates must be forwarded to webhooks so Chatwoot can enter qr_scanned'
);
assert.match(
  qrConsumedBlock,
  /hasQr:\s*false/,
  'QR-consumed webhook must include hasQr:false'
);
assert.match(
  qrConsumedBlock,
  /state:\s*'connecting'/,
  'QR-consumed webhook must keep the instance in connecting state, not reauth_required'
);

const pairingCodeHelperBlock = extractBlock(
  baileysService,
  'private async requestPairingCodeForCurrentQr()',
  'private async emitAuthenticationArtifactScannedUpdate()'
);
assert.match(
  pairingCodeHelperBlock,
  /try\s*{/,
  'pairing-code generation must be isolated so failures do not prevent QR generation'
);
assert.match(
  pairingCodeHelperBlock,
  /catch \(error\)/,
  'pairing-code generation failures must be caught and logged'
);
assert.match(
  pairingCodeHelperBlock,
  /return null;/,
  'failed pairing-code generation must fall back to a QR-only authorization artifact'
);

const qrUpdateBlock = extractBlock(
  baileysService,
  'if (qr) {',
  'if (!qr && !connection'
);
assert.match(
  qrUpdateBlock,
  /this\.instance\.qrcode\.pairingCode\s*=\s*await this\.requestPairingCodeForCurrentQr\(\);/,
  'QR generation must use the safe pairing-code helper'
);
assert.match(
  qrUpdateBlock,
  /this\.sendDataWebhook\(Events\.QRCODE_UPDATED/,
  'QR generation must still emit qrcode.updated after the safe pairing-code attempt'
);
assert.ok(
  !baileysService.includes('qrcode-terminal') && !baileysService.includes('qrcodeTerminal.generate'),
  'QR authorization artifacts must not be rendered into production logs'
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
