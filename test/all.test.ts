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
const instanceRouter = readFileSync(join(root, 'src/api/routes/instance.router.ts'), 'utf8');

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
  'public async logoutInstance'
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
  /if \(state == 'connecting'\) \{\s*if \(this\.hasAuthenticationArtifacts\(instance\)\) \{\s*return await this\.connectOutcomeResponse\(instanceName, instance\);\s*\}/s,
  '/instance/connect must return the existing QR/pairing artifact immediately while already connecting'
);
assert.match(
  connectToWhatsappBlock,
  /if \(state == 'close'\) \{[\s\S]*if \(this\.hasAuthenticationArtifacts\(instance\)\) \{\s*return await this\.connectOutcomeResponse\(instanceName, instance\);\s*\}/,
  '/instance/connect must not start a fresh socket when a close-state instance still has an auth artifact'
);
assert.match(
  connectToWhatsappBlock,
  /if \(state == 'close'\) \{\s*if \(rawState == 'open'\) \{[\s\S]*await instance\.prepareForFreshConnectAttempt\?\.\(\);[\s\S]*await instance\.connectToWhatsapp\(number\);[\s\S]*return await this\.connectOutcomeResponse\(instanceName, instance\);\s*\}\s*if \(this\.hasAuthenticationArtifacts\(instance\)\)/,
  'stale-open recovery must replace the socket before returning any retained QR artifact'
);

const prepareFreshConnectBlock = extractBlock(
  baileysService,
  'public async prepareForFreshConnectAttempt()',
  'public async forceReauthentication'
);
assert.match(
  prepareFreshConnectBlock,
  /this\.shouldResetEndedUnauthenticatedSession\(\)/,
  'fresh connect attempts must reset ended unauthenticated QR-limit sessions'
);
assert.match(
  prepareFreshConnectBlock,
  /this\.resetEndedUnauthenticatedSessionForFreshConnect\(\)/,
  'fresh connect attempts must clear QR-limit endSession before reconnecting'
);
assert.match(prepareFreshConnectBlock, /persistedAuthRegistered === false/);
assert.match(prepareFreshConnectBlock, /this\.instance\.qrcode = \{ count: 0 \}/);
assert.match(prepareFreshConnectBlock, /this\.scannedAuthenticationArtifactAt = null/);
assert.match(connectToWhatsappBlock, /await instance\.prepareForFreshConnectAttempt\?\.\(\)/);

const connectToWhatsappRuntimeBlock = extractBlock(
  baileysService,
  'public async connectToWhatsapp(number?: string)',
  'public async reloadConnection'
);
assert.match(
  connectToWhatsappRuntimeBlock,
  /this\.shouldResetEndedUnauthenticatedSession\(\)/,
  'connectToWhatsapp must not leave QR-limit endSession stuck behind the generic deleting guard'
);
assert.match(
  connectToWhatsappRuntimeBlock,
  /this\.resetEndedUnauthenticatedSessionForFreshConnect\(\)/,
  'connectToWhatsapp must reset ended unauthenticated sessions before checking the deleting guard'
);
assert.match(
  baileysService,
  /return this\.endSession && !this\.isDeleting && !this\.instance\.wuid;/,
  'only non-deleting unauthenticated ended sessions may be reset for a new QR cycle'
);

const logoutBlock = extractBlock(
  baileysService,
  'public async logoutInstance',
  'public hasLiveConnection'
);
assert.match(logoutBlock, /this\.stateConnection\s*=\s*{\s*state:\s*'close'/s);
assert.match(logoutBlock, /this\.instance\.authState\s*=\s*undefined/);
assert.match(logoutBlock, /this\.isDeleting\s*=\s*permanent/);

const restartBlock = extractBlock(baileysService, 'public async restart()', 'public async getProfileName');
assert.match(restartBlock, /connectionStatus:\s*'reconnecting'/);
assert.match(restartBlock, /this\.stateConnection\s*=\s*{\s*state:\s*'close'/s);
assert.match(restartBlock, /return this\.connectToWhatsapp\(this\.phoneNumber\)/);

assert.match(instanceRouter, /routerPath\('reauthorize'\)/);
assert.match(instanceController, /public async reauthorizeInstance/);
assert.match(instanceController, /this\.reauthorizationInFlight\.get\(instanceName\)/);
assert.match(baileysService, /if \(this\.reauthenticationInFlight\) \{/);
assert.match(instanceController, /this\.runtimeConnectionState\(runtimeInstance\)/);
assert.match(baileysService, /public async hasPersistedAuthenticationCredentials\(\)/);
assert.match(instanceController, /hasPersistedAuthenticationCredentials/);
assert.match(baileysService, /await eventAuthState\.saveCreds\(\)/);
assert.match(baileysService, /if \(eventClient !== this\.client\) \{\s*return;/s);
assert.match(baileysService, /public async hasDurableLiveConnection\(\)/);
assert.match(
  baileysService,
  /public async hasDurableLiveConnection\(\)[\s\S]*await this\.waitForEventProcessingIdle\(\)[\s\S]*this\.hasLiveConnection\(\)[\s\S]*hasPersistedAuthenticationCredentials\(\)/
);
assert.match(
  instanceController,
  /const outcomeStatus = response\.instance\.status;[\s\S]*outcomeStatus !== 'open' && outcomeStatus !== 'qr_ready'/
);
assert.match(instanceController, /private readonly lifecycleOperations = new Map<string, Promise<void>>\(\)/);
assert.match(instanceController, /this\.enqueueLifecycleOperation\(instanceName/);
assert.match(instanceController, /state === 'close' && rawState !== 'open'/);
assert.match(logoutBlock, /await this\.waitForEventProcessingIdle\(\)/);
assert.match(logoutBlock, /const previousClient = this\.client;\s*this\.client = null;/s);
assert.match(restartBlock, /await this\.waitForEventProcessingIdle\(\)/);
assert.match(restartBlock, /previousClient\?\.end\(new Error\('restart'\)\)/);
assert.match(
  baileysService,
  /const settings = await this\.findSettings\(\);\s*if \(eventClient !== this\.client\) \{\s*return;/s
);

console.log('WhatsApp auth lifecycle regression checks passed');
