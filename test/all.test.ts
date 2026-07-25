import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { LifecycleOperationRegistry } from '../src/api/services/lifecycle-operation.registry';

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
const monitorService = readFileSync(join(root, 'src/api/services/monitor.service.ts'), 'utf8');
const mysqlSchema = readFileSync(join(root, 'prisma/mysql-schema.prisma'), 'utf8');
const psqlBouncerSchema = readFileSync(join(root, 'prisma/psql_bouncer-schema.prisma'), 'utf8');
const channelService = readFileSync(join(root, 'src/api/services/channel.service.ts'), 'utf8');

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
  'private async emitAuthenticationArtifactScannedUpdate(',
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
assert.match(
  qrConsumedBlock,
  /lifecycleOperationMetadata\(operationId\)/,
  'QR-consumed updates must preserve the lifecycle operation ID'
);

const pairingCodeHelperBlock = extractBlock(
  baileysService,
  'private async requestPairingCodeForCurrentQr()',
  'private lifecycleOperationMetadata'
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
assert.match(
  baileysService,
  /tryRecoverInitialConnectionWithoutQr[\s\S]*forceReauthenticationFromEvent\(this\.phoneNumber \?\? this\.instance\.number\)/,
  'initial QR timeout must be recoverable through the serialized in-place reauthentication path'
);
assert.match(
  baileysService,
  /const persistedAuthRegistered = await this\.hasPersistedAuthenticationCredentials\(\);\s*if \(persistedAuthRegistered !== false\) \{[\s\S]*return false;\s*\}\s*await this\.forceReauthenticationFromEvent/,
  'automatic QR recovery must never clear registered or unverifiable persisted credentials'
);

const logoutBlock = extractBlock(
  baileysService,
  'public async logoutInstance',
  'public hasLiveConnection'
);
assert.match(logoutBlock, /this\.stateConnection\s*=\s*{\s*state:\s*'close'/s);
assert.match(logoutBlock, /this\.instance\.authState\s*=\s*undefined/);
assert.match(logoutBlock, /this\.isDeleting\s*=\s*permanent/);
assert.match(logoutBlock, /if \(waitForEventProcessing\) \{\s*await this\.waitForEventProcessingIdle\(\);/s);
assert.match(
  baileysService,
  /handleMissingInstanceRecord[\s\S]*logoutInstance\(\{ permanent: true, waitForEventProcessing: false \}\)/,
  'missing-record cleanup inside the event queue must not wait for that same queue'
);

const restartBlock = extractBlock(baileysService, 'public async restart()', 'public async getProfileName');
assert.match(restartBlock, /connectionStatus:\s*'reconnecting'/);
assert.match(restartBlock, /this\.stateConnection\s*=\s*{\s*state:\s*'close'/s);
assert.match(restartBlock, /return this\.connectToWhatsapp\(this\.phoneNumber\)/);

assert.match(instanceRouter, /routerPath\('reauthorize'\)/);
assert.match(monitorService, /loadInstancesFromRedis[\s\S]*ownerJid: instanceData\.ownerJid/);
assert.match(monitorService, /loadInstancesFromProvider[\s\S]*ownerJid: instance\.ownerJid/);
assert.match(mysqlSchema, /model Media \{[\s\S]*fileUrl\s+String\?/);
assert.match(psqlBouncerSchema, /model Chat \{[\s\S]*@@unique\(\[instanceId, remoteJid\]\)/);
assert.match(psqlBouncerSchema, /model Media \{[\s\S]*fileUrl\s+String\?/);
assert.match(instanceController, /public async reauthorizeInstance/);
assert.match(instanceController, /this\.reauthorizationOperations\.start\(/);
assert.match(instanceController, /accepted:\s*true/);
assert.doesNotMatch(
  extractBlock(instanceController, 'public async reauthorizeInstance', 'private async performReauthorizeInstance'),
  /return await operation/,
  'reauthorize must acknowledge the operation without waiting for Baileys'
);
assert.match(baileysService, /if \(this\.reauthenticationInFlight\) \{/);
const forceReauthenticationBlock = extractBlock(
  baileysService,
  'public async forceReauthentication(number?: string, operationId?: string)',
  'private async forceReauthenticationFromEvent'
);
assert.match(
  forceReauthenticationBlock,
  /if \(this\.reauthenticationInFlight\)[\s\S]*this\.markReauthorizationPending[\s\S]*this\.startForceReauthentication\(number, true\)/,
  'single-flight reservation must happen before waiting for the old event queue to drain'
);
assert.match(
  baileysService,
  /private async performForceReauthenticationAfterIdle[\s\S]*waitForConnectionAttemptIdle[\s\S]*waitForEventProcessingIdle/
);
const eventReauthenticationBlock = extractBlock(
  baileysService,
  'private async forceReauthenticationFromEvent',
  'private async startForceReauthentication'
);
assert.match(eventReauthenticationBlock, /reauthenticationInFlight \|\| this\.reauthenticationReservationOperationId/);
assert.match(eventReauthenticationBlock, /return null;/, 'event callbacks must not await a command waiting for their queue');
assert.match(instanceController, /activeReauthorizationOperationId\?\.\(\)/);
assert.match(instanceController, /operationId:\s*activeOperationId \?\? lifecycleOperationId/);
assert.match(instanceController, /if \(!acceptance\.deduplicated\) \{\s*instance\.markReauthorizationPending/s);
assert.match(baileysService, /lifecycleEventSequence: this\.lifecycleEventSequence/);
assert.match(instanceController, /this\.runtimeConnectionState\(runtimeInstance\)/);
assert.match(baileysService, /public async hasPersistedAuthenticationCredentials\(\)/);
assert.match(instanceController, /hasPersistedAuthenticationCredentials/);
assert.match(
  baileysService,
  /Object\.assign\(eventAuthState\.state\.creds, events\['creds\.update'\]\);\s*await eventAuthState\.saveCreds\(\)/,
  'credential updates must be merged into the persisted auth state before saveCreds'
);
assert.match(baileysService, /await eventAuthState\.saveCreds\(\)/);
assert.match(baileysService, /eventClient !== this\.client \|\| !this\.isCurrentLifecycleOperation/);
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
  /const settings = await this\.findSettings\(\);\s*if \(eventClient !== this\.client \|\| !this\.isCurrentLifecycleOperation/s
);
assert.match(channelService, /const now = new Date\(\)\.toISOString\(\);/);
assert.doesNotMatch(channelService, /getTimezoneOffset\(\)/, 'webhook timestamps must use real UTC instants');

async function verifyLifecycleOperationRegistry() {
  const lifecycleOperations = new LifecycleOperationRegistry();
  let releaseFirstOperation: (() => void) | undefined;
  let operationRuns = 0;
  const firstAcceptance = lifecycleOperations.start(
    'instance-a',
    async () => {
      operationRuns += 1;
      await new Promise<void>((resolve) => {
        releaseFirstOperation = resolve;
      });
    },
    { operationId: 'operation-a', retentionMs: 100 }
  );
  const duplicateAcceptance = lifecycleOperations.start('instance-a', async () => {
    operationRuns += 1;
  });

  assert.equal(firstAcceptance.deduplicated, false);
  assert.equal(firstAcceptance.inFlight, true);
  assert.equal(duplicateAcceptance.deduplicated, true);
  assert.equal(duplicateAcceptance.operationId, firstAcceptance.operationId);
  await Promise.resolve();
  assert.equal(operationRuns, 1, 'duplicate requests must share one background operation');
  releaseFirstOperation?.();
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(lifecycleOperations.get('instance-a'), null, 'settled operations must not mask runtime state reads');

  const retainedAcceptance = lifecycleOperations.start(
    'instance-a',
    async () => {
      operationRuns += 1;
    },
    { operationId: 'operation-a' }
  );
  assert.equal(retainedAcceptance.deduplicated, true);
  assert.equal(retainedAcceptance.inFlight, false);
  assert.equal(retainedAcceptance.operationId, firstAcceptance.operationId);
  assert.equal(operationRuns, 1, 'settled operations must remain deduplicated during the causal fence');

  const nextAcceptance = lifecycleOperations.start(
    'instance-a',
    async () => {
      operationRuns += 1;
    },
    { operationId: 'operation-b' }
  );
  assert.notEqual(nextAcceptance.operationId, firstAcceptance.operationId);
  assert.equal(nextAcceptance.inFlight, true);
  await Promise.resolve();
  assert.equal(operationRuns, 2, 'a new causal ID must start a new operation after the previous one settles');

  const longRunningOperations = new LifecycleOperationRegistry();
  let releaseLongRunningOperation: (() => void) | undefined;
  const longRunningAcceptance = longRunningOperations.start(
    'instance-c',
    () =>
      new Promise<void>((resolve) => {
        releaseLongRunningOperation = resolve;
      }),
    { retentionMs: 5 }
  );
  await new Promise<void>((resolve) => setTimeout(resolve, 15));
  assert.equal(
    longRunningOperations.get('instance-c')?.operationId,
    longRunningAcceptance.operationId,
    'retention must begin after completion and never evict an active operation'
  );
  releaseLongRunningOperation?.();

  let reportedFailure: string | undefined;
  lifecycleOperations.start(
    'instance-b',
    async () => {
      throw new Error('expected failure');
    },
    {
      onFailure: (error) => {
        reportedFailure = (error as Error).message;
      },
    }
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(reportedFailure, 'expected failure');
}

verifyLifecycleOperationRegistry()
  .then(() => console.log('WhatsApp auth lifecycle regression checks passed'))
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
