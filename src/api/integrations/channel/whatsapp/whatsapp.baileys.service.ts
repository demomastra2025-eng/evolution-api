import { getCollectionsDto } from '@api/dto/business.dto';
import { OfferCallDto } from '@api/dto/call.dto';
import {
  ArchiveChatDto,
  BlockUserDto,
  DeleteMessage,
  getBase64FromMediaMessageDto,
  LastMessage,
  MarkChatUnreadDto,
  NumberBusiness,
  OnWhatsAppDto,
  PrivacySettingDto,
  ReadMessageDto,
  SendPresenceDto,
  UpdateMessageDto,
  WhatsAppNumberDto,
} from '@api/dto/chat.dto';
import {
  AcceptGroupInvite,
  CreateGroupDto,
  GetParticipant,
  GroupDescriptionDto,
  GroupInvite,
  GroupJid,
  GroupPictureDto,
  GroupSendInvite,
  GroupSubjectDto,
  GroupToggleEphemeralDto,
  GroupUpdateParticipantDto,
  GroupUpdateSettingDto,
} from '@api/dto/group.dto';
import { InstanceDto, SetPresenceDto } from '@api/dto/instance.dto';
import { HandleLabelDto, LabelDto } from '@api/dto/label.dto';
import {
  Button,
  ContactMessage,
  KeyType,
  MediaMessage,
  Options,
  SendAudioDto,
  SendButtonsDto,
  SendContactDto,
  SendListDto,
  SendLocationDto,
  SendMediaDto,
  SendPollDto,
  SendPtvDto,
  SendReactionDto,
  SendStatusDto,
  SendStickerDto,
  SendTextDto,
  StatusMessage,
  TypeButton,
} from '@api/dto/sendMessage.dto';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import * as s3Service from '@api/integrations/storage/s3/libs/minio.server';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository, Query } from '@api/repository/repository.service';
import { chatbotController, waMonitor } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { ChannelStartupService } from '@api/services/channel.service';
import { Events, MessageSubtype, TypeMediaMessage, wa } from '@api/types/wa.types';
import { CacheEngine } from '@cache/cacheengine';
import {
  AudioConverter,
  CacheConf,
  Chatwoot,
  ConfigService,
  configService,
  Database,
  Log,
  Openai,
  ProviderSession,
  QrCode,
  S3,
} from '@config/env.config';
import { BadRequestException, InternalServerErrorException, NotFoundException } from '@exceptions';
import ffmpegPath from '@ffmpeg-installer/ffmpeg';
import { Boom } from '@hapi/boom';
import { createId as cuid } from '@paralleldrive/cuid2';
import { Contact as ContactModel, Instance, Message, Prisma } from '@prisma/client';
import { createJid } from '@utils/createJid';
import { fetchLatestWaWebVersion } from '@utils/fetchLatestWaWebVersion';
import { makeProxyAgent, makeProxyAgentUndici } from '@utils/makeProxyAgent';
import { getOnWhatsappCache, saveOnWhatsappCache } from '@utils/onWhatsappCache';
import { status } from '@utils/renderStatus';
import { sendTelemetry } from '@utils/sendTelemetry';
import useMultiFileAuthStatePrisma from '@utils/use-multi-file-auth-state-prisma';
import { AuthStateProvider } from '@utils/use-multi-file-auth-state-provider-files';
import { useMultiFileAuthStateRedisDb } from '@utils/use-multi-file-auth-state-redis-db';
import audioDecode from 'audio-decode';
import axios from 'axios';
import makeWASocket, {
  AnyMessageContent,
  BufferedEventData,
  BufferJSON,
  CacheStore,
  CatalogCollection,
  Chat,
  ConnectionState,
  Contact,
  decryptPollVote,
  delay,
  DisconnectReason,
  downloadContentFromMessage,
  downloadMediaMessage,
  generateMessageIDV2,
  generateWAMessageFromContent,
  getAggregateVotesInPollMessage,
  GetCatalogOptions,
  getContentType,
  getDevice,
  GroupMetadata,
  isJidBroadcast,
  isJidGroup,
  isJidNewsletter,
  isPnUser,
  jidNormalizedUser,
  makeCacheableSignalKeyStore,
  MessageUpsertType,
  MessageUserReceiptUpdate,
  MiscMessageGenerationOptions,
  ParticipantAction,
  prepareWAMessageMedia,
  Product,
  proto,
  UserFacingSocketConfig,
  WAMediaUpload,
  WAMessage,
  WAMessageKey,
  WAPresence,
  WASocket,
} from 'baileys';
import { Label } from 'baileys/lib/Types/Label';
import { LabelAssociation } from 'baileys/lib/Types/LabelAssociation';
import { spawn } from 'child_process';
import { isArray, isBase64, isURL } from 'class-validator';
import { createHash } from 'crypto';
import EventEmitter2 from 'eventemitter2';
import ffmpeg from 'fluent-ffmpeg';
import FormData from 'form-data';
import { getLinkPreview } from 'link-preview-js';
import Long from 'long';
import mimeTypes from 'mime-types';
import NodeCache from 'node-cache';
import cron from 'node-cron';
import { join } from 'path';
import P from 'pino';
import qrcode, { QRCodeToDataURLOptions } from 'qrcode';
import sharp from 'sharp';
import { PassThrough, Readable } from 'stream';
import { v4 } from 'uuid';

import { BaileysMessageProcessor } from './baileysMessage.processor';
import { useVoiceCallsBaileys } from './voiceCalls/useVoiceCallsBaileys';

export interface ExtendedIMessageKey extends proto.IMessageKey {
  remoteJidAlt?: string;
  participantAlt?: string;
  server_id?: string;
  isViewOnce?: boolean;
}

interface CanonicalJidResolution {
  remoteJid?: string;
  rawRemoteJid?: string;
  remoteJidAlt?: string;
  remoteLid?: string;
  addressingMode?: 'pn' | 'lid';
}

export class MediaUnavailableError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly mediaUrl?: string,
    public readonly reason: string = 'media_unavailable',
  ) {
    super(`Historical media is unavailable (status ${statusCode})`);
    this.name = 'MediaUnavailableError';
  }
}

const CONTACT_UPDATE_PERSISTENCE_CONCURRENCY = 2;
const CONTACT_PROFILE_LOOKUP_CONCURRENCY = 4;

async function eachWithConcurrencyLimit<T>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<void>,
) {
  if (!items.length) {
    return;
  }

  const workerCount = Math.max(1, Math.min(concurrency, items.length));
  let nextIndex = 0;

  const runWorker = async () => {
    let currentIndex = nextIndex++;
    while (currentIndex < items.length) {
      await worker(items[currentIndex], currentIndex);
      currentIndex = nextIndex++;
    }
  };

  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
}

async function mapWithConcurrencyLimit<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>,
) {
  const results = new Array<R>(items.length);
  await eachWithConcurrencyLimit(items, concurrency, async (item, index) => {
    results[index] = await worker(item, index);
  });
  return results;
}

const groupMetadataCache = new CacheService(new CacheEngine(configService, 'groups').getEngine());

// Adicione a função getVideoDuration no início do arquivo
async function getVideoDuration(input: Buffer | string | Readable): Promise<number> {
  const MediaInfoFactory = (await import('mediainfo.js')).default;
  const mediainfo = await MediaInfoFactory({ format: 'JSON' });

  let fileSize: number;
  let readChunk: (size: number, offset: number) => Promise<Buffer>;

  if (Buffer.isBuffer(input)) {
    fileSize = input.length;
    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return input.slice(offset, offset + size);
    };
  } else if (typeof input === 'string') {
    const fs = await import('fs');
    const stat = await fs.promises.stat(input);
    fileSize = stat.size;
    const fd = await fs.promises.open(input, 'r');

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      const buffer = Buffer.alloc(size);
      await fd.read(buffer, 0, size, offset);
      return buffer;
    };

    try {
      const result = await mediainfo.analyzeData(() => fileSize, readChunk);
      const jsonResult = JSON.parse(result);

      const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
      const duration = generalTrack.Duration;

      return Math.round(parseFloat(duration));
    } finally {
      await fd.close();
    }
  } else if (input instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of input) {
      chunks.push(chunk);
    }
    const data = Buffer.concat(chunks);
    fileSize = data.length;

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return data.slice(offset, offset + size);
    };
  } else {
    throw new Error('Tipo de entrada não suportado');
  }

  const result = await mediainfo.analyzeData(() => fileSize, readChunk);
  const jsonResult = JSON.parse(result);

  const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
  const duration = generalTrack.Duration;

  return Math.round(parseFloat(duration));
}

export class BaileysStartupService extends ChannelStartupService {
  private messageProcessor = new BaileysMessageProcessor();

  constructor(
    public readonly configService: ConfigService,
    public readonly eventEmitter: EventEmitter2,
    public readonly prismaRepository: PrismaRepository,
    public readonly cache: CacheService,
    public readonly chatwootCache: CacheService,
    public readonly baileysCache: CacheService,
    private readonly providerFiles: ProviderFiles,
  ) {
    super(configService, eventEmitter, prismaRepository, chatwootCache);
    this.instance.qrcode = { count: 0 };
    this.messageProcessor.mount({
      onMessageReceive: this.messageHandle['messages.upsert'].bind(this), // Bind the method to the current context
    });

    this.authStateProvider = new AuthStateProvider(this.providerFiles);
  }

  private authStateProvider: AuthStateProvider;
  private readonly msgRetryCounterCache: CacheStore = new NodeCache();
  private readonly userDevicesCache: CacheStore = new NodeCache({ stdTTL: 300000, useClones: false });
  private endSession = false;
  private isDeleting = false; // Flag to prevent reconnection during deletion
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private connectInFlight: Promise<WASocket> | null = null;
  private reauthenticationInFlight: Promise<WASocket> | null = null;
  private logBaileys = this.configService.get<Log>('LOG').BAILEYS;
  private eventProcessingQueue: Promise<void> = Promise.resolve();
  private persistedAuthRegistered: boolean | null = null;
  private persistedAuthCheckedAt = 0;
  private readonly persistedAuthCheckTtlMs = 30_000;
  // Cumulative history sync counters (reset on new sync or completion)
  private historySyncMessageCount = 0;
  private historySyncChatCount = 0;
  private historySyncContactCount = 0;
  private historySyncLastProgress = -1;
  private initialConnectionRecoveryAttempted = false;
  private initialConnectionRecoveryInFlight: Promise<boolean> | null = null;
  private scannedAuthenticationArtifactAt: number | null = null;
  private readonly SCANNED_AUTHENTICATION_ARTIFACT_TTL_MS = 2 * 60 * 1000;

  // Cache TTL constants (in seconds)
  private readonly MESSAGE_CACHE_TTL_SECONDS = 5 * 60; // 5 minutes - avoid duplicate message processing
  private readonly UPDATE_CACHE_TTL_SECONDS = 30 * 60; // 30 minutes - avoid duplicate status updates

  public stateConnection: wa.StateConnection = { state: 'close' };

  public phoneNumber: string;

  public get connectionStatus() {
    return this.stateConnection;
  }

  private trimToUndefined(value?: string | null): string | undefined {
    const trimmed = value?.trim();
    return trimmed ? trimmed : undefined;
  }

  private normalizeJid(jid?: string | null): string | undefined {
    const value = jid?.trim();
    if (!value) {
      return undefined;
    }

    const atIndex = value.indexOf('@');
    if (atIndex === -1) {
      return value.replace(/:\d+$/, '') || undefined;
    }

    const user = value.slice(0, atIndex).replace(/:\d+$/, '');
    const server = value.slice(atIndex + 1);
    const normalized = `${user}@${server}`;

    return normalized || undefined;
  }

  private isLidJid(jid?: string | null): boolean {
    return !!this.normalizeJid(jid)?.includes('@lid');
  }

  private pickPreferredName(...values: Array<string | null | undefined>) {
    for (const value of values) {
      const preferred = this.trimToUndefined(value);
      if (preferred) {
        return preferred;
      }
    }

    return undefined;
  }

  private uniqueNormalizedJids(...jids: Array<string | null | undefined>) {
    return [...new Set(jids.map((jid) => this.normalizeJid(jid)).filter((jid): jid is string => !!jid))];
  }

  private isPnJid(jid?: string | null) {
    const normalizedJid = this.normalizeJid(jid);
    return !!normalizedJid && normalizedJid.includes('@s.whatsapp.net');
  }

  private extractLidPnMapping(left?: string | null, right?: string | null) {
    const normalizedLeft = this.normalizeJid(left);
    const normalizedRight = this.normalizeJid(right);

    if (!normalizedLeft || !normalizedRight || normalizedLeft === normalizedRight) {
      return null;
    }

    if (this.isLidJid(normalizedLeft) && this.isPnJid(normalizedRight)) {
      return { lid: normalizedLeft, pn: normalizedRight };
    }

    if (this.isPnJid(normalizedLeft) && this.isLidJid(normalizedRight)) {
      return { lid: normalizedRight, pn: normalizedLeft };
    }

    return null;
  }

  private dedupeLidPnMappings(
    mappings: Array<{ lid?: string | null; pn?: string | null } | null | undefined>,
  ): Array<{ lid: string; pn: string }> {
    const dedupedMappings = new Map<string, { lid: string; pn: string }>();

    for (const mapping of mappings) {
      const lid = this.normalizeJid(mapping?.lid);
      const pn = this.normalizeJid(mapping?.pn);

      if (!lid || !pn || !this.isLidJid(lid) || !this.isPnJid(pn)) {
        continue;
      }

      dedupedMappings.set(`${lid}|${pn}`, { lid, pn });
    }

    return [...dedupedMappings.values()];
  }

  private extractIdentityMappingsFromKey(key: Partial<ExtendedIMessageKey> | undefined) {
    if (!key) {
      return [];
    }

    return this.dedupeLidPnMappings([
      this.extractLidPnMapping(key.remoteJid, key.remoteJidAlt),
      this.extractLidPnMapping(key.participant, key.participantAlt),
    ]);
  }

  private async ingestIdentityMappings(
    mappings: Array<{ lid?: string | null; pn?: string | null }>,
    options: { reconcileDatabase?: boolean; syncMessages?: boolean } = {},
  ) {
    const normalizedMappings = this.dedupeLidPnMappings(mappings);

    if (!normalizedMappings.length) {
      return [];
    }

    const nativeStoreMappings = (this.client?.signalRepository?.lidMapping as any)?.storeLIDPNMappings;

    if (nativeStoreMappings) {
      try {
        await nativeStoreMappings.call(this.client.signalRepository.lidMapping, normalizedMappings);
      } catch (error) {
        this.logger.debug({
          message: 'Failed to persist LID-PN mappings using native Baileys mapping store',
          count: normalizedMappings.length,
          error: error?.toString?.() ?? String(error),
        });
      }
    }

    await saveOnWhatsappCache(
      normalizedMappings.map(({ lid, pn }) => ({
        remoteJid: pn,
        remoteJidAlt: lid,
        lid: 'lid' as const,
      })),
    );

    if (options.reconcileDatabase === false) {
      return normalizedMappings;
    }

    await eachWithConcurrencyLimit(normalizedMappings, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async ({ lid, pn }) => {
      await this.reconcileIdentityAliases(
        {
          remoteJid: pn,
          remoteJidAlt: lid,
          remoteLid: lid,
        },
        {
          syncMessages: options.syncMessages ?? true,
        },
      );
    });

    return normalizedMappings;
  }

  private async emitContactUpdatesForIdentityMappings(mappings: Array<{ lid: string; pn: string }>) {
    if (!mappings.length) {
      return;
    }

    const contactsRaw = (
      await mapWithConcurrencyLimit(mappings, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async ({ lid, pn }) => {
        const normalizedContact = await this.normalizeContactPayload({
          id: lid,
          remoteJidAlt: pn,
          remoteLid: lid,
        } as Partial<Contact> & { id: string; remoteJidAlt: string; remoteLid: string });

        if (!normalizedContact) {
          return null;
        }

        return {
          ...normalizedContact,
          profilePicUrl: await this.resolveProfilePictureUrlForIdentity(normalizedContact),
        };
      })
    ).filter(Boolean);

    if (!contactsRaw.length) {
      return;
    }

    this.sendDataWebhook(Events.CONTACTS_UPDATE, contactsRaw);

    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      await eachWithConcurrencyLimit(contactsRaw, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (contact) => {
        await this.chatwootService.reconcileContactIdentity(
          { instanceName: this.instance.name, instanceId: this.instance.id },
          {
            remoteJid: contact.remoteJid,
            remoteJidAlt: contact.remoteJidAlt,
            remoteLid: contact.remoteLid,
            pushName: contact.pushName,
            profilePicUrl: contact.profilePicUrl,
          },
        );
      });
    }
  }

  private buildMessageIdentityLookupKey(key: Partial<ExtendedIMessageKey> | undefined) {
    const id = key?.id;

    if (!id) {
      return undefined;
    }

    const remoteJid = this.normalizeJid(key.remoteJid) ?? this.normalizeJid(key.remoteJidAlt) ?? '';
    const participant = this.normalizeJid(key.participant) ?? this.normalizeJid(key.participantAlt) ?? '';
    const fromMe = key.fromMe === true ? '1' : '0';

    return [id, fromMe, remoteJid, participant].join('|');
  }

  private storedMessageMatchesKey(
    message: Partial<Message> | null | undefined,
    key: Partial<ExtendedIMessageKey> | undefined,
    searchId?: string,
  ) {
    if (!message?.key || !key) {
      return false;
    }

    const storedKey = message.key as ExtendedIMessageKey;
    const expectedId = searchId ?? key.id;

    if (expectedId && storedKey.id !== expectedId) {
      return false;
    }

    if (typeof key.fromMe === 'boolean' && storedKey.fromMe !== key.fromMe) {
      return false;
    }

    const remoteJidCandidates = this.uniqueNormalizedJids(key.remoteJid, key.remoteJidAlt);
    const storedRemoteJids = this.uniqueNormalizedJids(storedKey.remoteJid, storedKey.remoteJidAlt);
    if (remoteJidCandidates.length && !storedRemoteJids.some((candidate) => remoteJidCandidates.includes(candidate))) {
      return false;
    }

    const participantCandidates = this.uniqueNormalizedJids(key.participant, key.participantAlt);
    const storedParticipants = this.uniqueNormalizedJids(storedKey.participant, storedKey.participantAlt);
    if (
      participantCandidates.length &&
      !storedParticipants.some((candidate) => participantCandidates.includes(candidate))
    ) {
      return false;
    }

    return true;
  }

  private async findStoredMessageByKey(
    key: Partial<ExtendedIMessageKey> | undefined,
    options: { searchId?: string } = {},
  ) {
    const searchId = options.searchId ?? key?.id;

    if (!searchId) {
      return null;
    }

    const provider = this.configService.get<Database>('DATABASE').PROVIDER;
    let messages: any[];

    if (provider === 'mysql') {
      messages = (await this.prismaRepository.$queryRaw`
        SELECT * FROM Message
        WHERE instanceId = ${this.instanceId}
        AND JSON_UNQUOTE(JSON_EXTRACT(\`key\`, '$.id')) = ${searchId}
        LIMIT 25
      `) as any[];
    } else {
      messages = (await this.prismaRepository.$queryRaw`
        SELECT * FROM "Message"
        WHERE "instanceId" = ${this.instanceId}
        AND "key"->>'id' = ${searchId}
        LIMIT 25
      `) as any[];
    }

    if (!messages.length) {
      return null;
    }

    return messages.find((message) => this.storedMessageMatchesKey(message, key, searchId)) ?? messages[0] ?? null;
  }

  private buildIdentityCandidates(
    resolution: Partial<CanonicalJidResolution> = {},
    ...extraJids: Array<string | null | undefined>
  ) {
    return this.uniqueNormalizedJids(
      resolution.remoteJid,
      resolution.remoteJidAlt,
      resolution.remoteLid,
      resolution.rawRemoteJid,
      ...extraJids,
    );
  }

  private async resolveLidToPnJid(jid?: string | null): Promise<string | undefined> {
    const normalizedJid = this.normalizeJid(jid);

    if (!normalizedJid || !this.isLidJid(normalizedJid) || !this.client?.signalRepository?.lidMapping?.getPNForLID) {
      return undefined;
    }

    try {
      const mappedJid = await this.client.signalRepository.lidMapping.getPNForLID(normalizedJid);
      const normalizedMappedJid = this.normalizeJid(mappedJid);

      if (normalizedMappedJid && !this.isLidJid(normalizedMappedJid)) {
        return normalizedMappedJid;
      }
    } catch (error) {
      this.logger.debug({
        message: 'Failed to resolve LID to PN using native Baileys mapping',
        inputJid: normalizedJid,
        error: error?.toString?.() ?? String(error),
      });
    }

    return undefined;
  }

  private async resolveCanonicalJidWithNative(
    remoteJid?: string | null,
    remoteJidAlt?: string | null,
    options: { phoneNumber?: string | null; remoteLid?: string | null } = {},
  ): Promise<CanonicalJidResolution> {
    const resolution = this.resolveCanonicalJid(remoteJid, remoteJidAlt, options);

    if (resolution.remoteJid && !this.isLidJid(resolution.remoteJid)) {
      return resolution;
    }

    const lidCandidates = this.uniqueNormalizedJids(options.remoteLid, remoteJid, remoteJidAlt).filter((jid) =>
      this.isLidJid(jid),
    );

    for (const lidJid of lidCandidates) {
      const pnJid = await this.resolveLidToPnJid(lidJid);

      if (pnJid) {
        return this.resolveCanonicalJid(pnJid, lidJid, {
          phoneNumber: options.phoneNumber,
          remoteLid: lidJid,
        });
      }
    }

    return resolution;
  }

  private async resolveMessageContactIdentity(message: WAMessage) {
    const keyAny = message.key as ExtendedIMessageKey & { remoteLid?: string };
    const remoteJid = this.normalizeJid(keyAny.remoteJid);

    if (
      remoteJid &&
      (isJidGroup(remoteJid) || isJidBroadcast(remoteJid)) &&
      (keyAny.participant || keyAny.participantAlt)
    ) {
      return this.resolveCanonicalJidWithNative(keyAny.participant, keyAny.participantAlt, {
        remoteLid: this.isLidJid(keyAny.participant) ? keyAny.participant : keyAny.participantAlt,
      });
    }

    return this.resolveCanonicalJidWithNative(keyAny.remoteJid, keyAny.remoteJidAlt, {
      remoteLid: keyAny.remoteLid,
    });
  }

  private async findBestContactByJids(jids: Array<string | null | undefined>) {
    const candidates = this.uniqueNormalizedJids(...jids);

    if (!candidates.length) {
      return null;
    }

    const contacts = await this.prismaRepository.contact.findMany({
      where: { instanceId: this.instanceId, remoteJid: { in: candidates } },
    });

    const contactByJid = new Map<string, (typeof contacts)[number]>(
      contacts.map((contact) => [contact.remoteJid, contact] as const),
    );

    for (const candidate of candidates) {
      const contact = contactByJid.get(candidate);
      if (contact) {
        return contact;
      }
    }

    return contacts[0] ?? null;
  }

  private async resolveProfilePictureUrlForIdentity(
    resolution: Partial<CanonicalJidResolution> = {},
    ...extraJids: Array<string | null | undefined>
  ) {
    const candidates = this.buildIdentityCandidates(resolution, ...extraJids);

    for (const candidate of candidates) {
      const picture = await this.profilePicture(candidate);
      if (picture.profilePictureUrl) {
        return picture.profilePictureUrl;
      }
    }

    return null;
  }

  private resolveCanonicalJid(
    remoteJid?: string | null,
    remoteJidAlt?: string | null,
    options: { phoneNumber?: string | null; remoteLid?: string | null } = {},
  ): CanonicalJidResolution {
    const normalizedRemoteJid = this.normalizeJid(remoteJid);
    const normalizedRemoteJidAlt = this.normalizeJid(remoteJidAlt);
    const normalizedPhoneJid = options.phoneNumber ? createJid(options.phoneNumber) : undefined;
    const normalizedRemoteLid = this.normalizeJid(options.remoteLid);

    const remoteJidCanonical =
      [normalizedRemoteJidAlt, normalizedPhoneJid, normalizedRemoteJid].find((jid) => jid && !this.isLidJid(jid)) ||
      normalizedRemoteJid ||
      normalizedRemoteJidAlt ||
      normalizedPhoneJid;

    const rawRemoteLid =
      [normalizedRemoteLid, normalizedRemoteJid, normalizedRemoteJidAlt].find((jid) => this.isLidJid(jid)) || undefined;

    const alternateJid =
      [normalizedRemoteJid, normalizedRemoteJidAlt, normalizedPhoneJid].find(
        (jid) => jid && jid !== remoteJidCanonical,
      ) || undefined;

    return {
      remoteJid: remoteJidCanonical,
      rawRemoteJid: normalizedRemoteJid,
      remoteJidAlt: alternateJid,
      remoteLid: rawRemoteLid,
      addressingMode: rawRemoteLid
        ? remoteJidCanonical && remoteJidCanonical !== rawRemoteLid
          ? 'pn'
          : 'lid'
        : undefined,
    };
  }

  private applyCanonicalKeyIdentity(
    key: Partial<ExtendedIMessageKey> | undefined,
    options: { phoneNumber?: string | null; remoteLid?: string | null } = {},
  ) {
    if (!key) {
      return key;
    }

    const resolution = this.resolveCanonicalJid(key.remoteJid, key.remoteJidAlt, {
      phoneNumber: options.phoneNumber,
      remoteLid: options.remoteLid ?? (key as any).remoteLid,
    });

    if (resolution.remoteJid) {
      key.remoteJid = resolution.remoteJid;
    }
    if (resolution.remoteJidAlt) {
      key.remoteJidAlt = resolution.remoteJidAlt;
    } else {
      delete key.remoteJidAlt;
    }
    if (resolution.remoteLid) {
      (key as any).remoteLid = resolution.remoteLid;
    }
    if (resolution.addressingMode) {
      (key as any).addressingMode = resolution.addressingMode;
    }
    if (key.participant || key.participantAlt) {
      const participantResolution = this.resolveCanonicalJid(key.participant, key.participantAlt, {
        remoteLid: this.isLidJid(key.participant) ? key.participant : key.participantAlt,
      });

      if (participantResolution.remoteJid) {
        key.participant = participantResolution.remoteJid;
      } else if (key.participant) {
        key.participant = this.normalizeJid(key.participant);
      }

      if (participantResolution.remoteJidAlt) {
        key.participantAlt = participantResolution.remoteJidAlt;
      } else {
        delete key.participantAlt;
      }
    }

    return key;
  }

  private async applyCanonicalKeyIdentityWithNative(
    key: Partial<ExtendedIMessageKey> | undefined,
    options: { phoneNumber?: string | null; remoteLid?: string | null } = {},
  ) {
    if (!key) {
      return key;
    }

    const extractedMappings = this.extractIdentityMappingsFromKey(key);
    if (extractedMappings.length) {
      await this.ingestIdentityMappings(extractedMappings, { reconcileDatabase: false });
    }

    const resolution = await this.resolveCanonicalJidWithNative(key.remoteJid, key.remoteJidAlt, {
      phoneNumber: options.phoneNumber,
      remoteLid: options.remoteLid ?? (key as any).remoteLid,
    });

    if (resolution.remoteJid) {
      key.remoteJid = resolution.remoteJid;
    }
    if (resolution.remoteJidAlt) {
      key.remoteJidAlt = resolution.remoteJidAlt;
    } else {
      delete key.remoteJidAlt;
    }
    if (resolution.remoteLid) {
      (key as any).remoteLid = resolution.remoteLid;
    }
    if (resolution.addressingMode) {
      (key as any).addressingMode = resolution.addressingMode;
    }
    if (key.participant || key.participantAlt) {
      const participantResolution = await this.resolveCanonicalJidWithNative(key.participant, key.participantAlt, {
        remoteLid: this.isLidJid(key.participant) ? key.participant : key.participantAlt,
      });

      if (participantResolution.remoteJid) {
        key.participant = participantResolution.remoteJid;
      } else if (key.participant) {
        key.participant = this.normalizeJid(key.participant);
      }

      if (participantResolution.remoteJidAlt) {
        key.participantAlt = participantResolution.remoteJidAlt;
      } else {
        delete key.participantAlt;
      }
    }

    return key;
  }

  private async normalizeContactPayload(contact: Partial<Contact> & { id?: string; phoneNumber?: string | null }) {
    const resolution = await this.resolveCanonicalJidWithNative(contact.id, (contact as any)?.remoteJidAlt, {
      phoneNumber: contact.phoneNumber,
      remoteLid: (contact as any)?.remoteLid ?? (contact as any)?.lid,
    });
    const remoteJid = resolution.remoteJid;

    if (!remoteJid) {
      return null;
    }

    return {
      remoteJid,
      canonicalJid: remoteJid,
      rawRemoteJid:
        resolution.rawRemoteJid && resolution.rawRemoteJid !== remoteJid ? resolution.rawRemoteJid : undefined,
      remoteJidAlt: resolution.remoteJidAlt,
      remoteLid: resolution.remoteLid,
      pushName: this.pickPreferredName(
        (contact as any)?.verifiedName,
        (contact as any)?.notify,
        contact?.name,
        remoteJid.split('@')[0],
      ),
      profilePicUrl: null,
      instanceId: this.instanceId,
    };
  }

  private async normalizeChatPayload(
    chat: Partial<Chat> & { id?: string; phoneNumber?: string | null; unreadCount?: number; name?: string },
  ) {
    const resolution = await this.resolveCanonicalJidWithNative(chat.id, (chat as any)?.remoteJidAlt, {
      phoneNumber: chat.phoneNumber,
      remoteLid: (chat as any)?.remoteLid ?? (chat as any)?.accountLid,
    });
    const remoteJid = resolution.remoteJid;

    if (!remoteJid) {
      return null;
    }

    return {
      remoteJid,
      rawRemoteJid:
        resolution.rawRemoteJid && resolution.rawRemoteJid !== remoteJid ? resolution.rawRemoteJid : undefined,
      remoteJidAlt: resolution.remoteJidAlt,
      remoteLid: resolution.remoteLid,
      instanceId: this.instanceId,
      name: chat.name,
      unreadMessages: chat.unreadCount !== undefined ? chat.unreadCount : 0,
    };
  }

  private chatPersistencePayload(chat: {
    remoteJid: string;
    instanceId: string;
    name?: string | null;
    unreadMessages: number;
    labels?: Prisma.JsonValue | Prisma.InputJsonValue | null;
  }) {
    const name = this.pickPreferredName(chat.name);

    return {
      remoteJid: chat.remoteJid,
      instanceId: chat.instanceId,
      unreadMessages: chat.unreadMessages,
      ...(name ? { name } : {}),
      ...(chat.labels !== undefined ? { labels: chat.labels as Prisma.InputJsonValue | null } : {}),
    };
  }

  private contactPersistencePayload(contact: {
    remoteJid: string;
    pushName?: string;
    profilePicUrl?: string | null;
    instanceId: string;
  }) {
    const pushName = this.pickPreferredName(contact.pushName);
    const profilePicUrl = this.trimToUndefined(contact.profilePicUrl);

    return {
      remoteJid: contact.remoteJid,
      instanceId: contact.instanceId,
      ...(pushName ? { pushName } : {}),
      ...(profilePicUrl ? { profilePicUrl } : {}),
    };
  }

  private async reconcileMessageIdentityAliases(resolution: Partial<CanonicalJidResolution> = {}) {
    const remoteJid = this.normalizeJid(resolution.remoteJid);

    if (!remoteJid) {
      return 0;
    }

    const aliasJids = this.buildIdentityCandidates(resolution).filter((candidate) => candidate !== remoteJid);
    if (!aliasJids.length) {
      return 0;
    }

    const provider = this.configService.get<Database>('DATABASE').PROVIDER;
    let updatedMessages = 0;

    if (provider === 'mysql') {
      updatedMessages = await this.prismaRepository.$executeRaw(
        Prisma.sql`
          UPDATE Message
          SET \`key\` = JSON_SET(\`key\`, '$.remoteJid', ${remoteJid})
          WHERE instanceId = ${this.instanceId}
          AND JSON_UNQUOTE(JSON_EXTRACT(\`key\`, '$.remoteJid')) IN (${Prisma.join(aliasJids)})
        `,
      );
    } else {
      updatedMessages = await this.prismaRepository.$executeRaw(
        Prisma.sql`
          UPDATE "Message"
          SET "key" = jsonb_set("key", '{remoteJid}', to_jsonb(CAST(${remoteJid} AS text)), true)
          WHERE "instanceId" = ${this.instanceId}
          AND "key"->>'remoteJid' IN (${Prisma.join(aliasJids)})
        `,
      );
    }

    await this.prismaRepository.messageUpdate.updateMany({
      where: { instanceId: this.instanceId, remoteJid: { in: aliasJids } },
      data: { remoteJid },
    });

    return Number(updatedMessages) || 0;
  }

  private async persistCanonicalContactEntity(
    resolution: Partial<CanonicalJidResolution> & { pushName?: string | null; profilePicUrl?: string | null } = {},
  ) {
    const remoteJid = this.normalizeJid(resolution.remoteJid);

    if (!remoteJid) {
      return null;
    }

    const candidates = this.buildIdentityCandidates(resolution);
    const contacts = candidates.length
      ? await this.prismaRepository.contact.findMany({
          where: { instanceId: this.instanceId, remoteJid: { in: candidates } },
        })
      : [];

    const pushName = this.pickPreferredName(resolution.pushName, ...contacts.map((contact) => contact.pushName));
    const profilePicUrl = this.pickPreferredName(
      resolution.profilePicUrl,
      ...contacts.map((contact) => contact.profilePicUrl),
    );
    const hasIncomingData = !!this.pickPreferredName(resolution.pushName, resolution.profilePicUrl);

    if (!contacts.length && !hasIncomingData) {
      return null;
    }

    const aliasContactIds = contacts.filter((contact) => contact.remoteJid !== remoteJid).map((contact) => contact.id);
    const payload = this.contactPersistencePayload({
      remoteJid,
      pushName,
      profilePicUrl,
      instanceId: this.instanceId,
    });

    await this.prismaRepository.$transaction([
      this.prismaRepository.contact.upsert({
        where: { remoteJid_instanceId: { remoteJid, instanceId: this.instanceId } },
        create: payload,
        update: payload,
      }),
      ...(aliasContactIds.length
        ? [
            this.prismaRepository.contact.deleteMany({
              where: { id: { in: aliasContactIds } },
            }),
          ]
        : []),
    ]);

    return {
      remoteJid,
      pushName,
      profilePicUrl,
      instanceId: this.instanceId,
    };
  }

  private async persistCanonicalChatEntity(
    resolution: Partial<CanonicalJidResolution> & {
      name?: string | null;
      unreadMessages?: number;
      labels?: Prisma.JsonValue | Prisma.InputJsonValue | null;
    } = {},
  ) {
    const remoteJid = this.normalizeJid(resolution.remoteJid);

    if (!remoteJid) {
      return null;
    }

    const candidates = this.buildIdentityCandidates(resolution);
    const chats = candidates.length
      ? await this.prismaRepository.chat.findMany({
          where: { instanceId: this.instanceId, remoteJid: { in: candidates } },
        })
      : [];

    const canonicalChat = chats.find((chat) => chat.remoteJid === remoteJid);
    const incomingName = this.pickPreferredName(resolution.name);
    const mergedName = this.pickPreferredName(incomingName, ...chats.map((chat) => chat.name));
    const mergedLabels =
      resolution.labels !== undefined
        ? resolution.labels
        : (canonicalChat?.labels ?? chats.find((chat) => chat.labels != null)?.labels);
    const mergedUnreadMessages =
      resolution.unreadMessages !== undefined
        ? resolution.unreadMessages
        : (canonicalChat?.unreadMessages ?? Math.max(...chats.map((chat) => chat.unreadMessages), 0));
    const hasIncomingData =
      incomingName !== undefined || resolution.unreadMessages !== undefined || resolution.labels !== undefined;

    if (!chats.length && !hasIncomingData) {
      return null;
    }

    const aliasChatIds = chats.filter((chat) => chat.remoteJid !== remoteJid).map((chat) => chat.id);
    const payload = this.chatPersistencePayload({
      remoteJid,
      instanceId: this.instanceId,
      name: mergedName,
      unreadMessages: mergedUnreadMessages,
      labels: mergedLabels,
    });

    await this.prismaRepository.$transaction([
      this.prismaRepository.chat.upsert({
        where: { instanceId_remoteJid: { instanceId: this.instanceId, remoteJid } },
        create: payload,
        update: payload,
      }),
      ...(aliasChatIds.length
        ? [
            this.prismaRepository.chat.deleteMany({
              where: { id: { in: aliasChatIds } },
            }),
          ]
        : []),
    ]);

    return payload;
  }

  private async reconcileIdentityAliases(
    resolution: Partial<CanonicalJidResolution> = {},
    options: {
      contact?: { pushName?: string | null; profilePicUrl?: string | null };
      chat?: {
        name?: string | null;
        unreadMessages?: number;
        labels?: Prisma.JsonValue | Prisma.InputJsonValue | null;
      };
      syncMessages?: boolean;
    } = {},
  ) {
    const remoteJid = this.normalizeJid(resolution.remoteJid);

    if (!remoteJid) {
      return;
    }

    const updatedMessages = options.syncMessages === false ? 0 : await this.reconcileMessageIdentityAliases(resolution);

    await Promise.all([
      this.persistCanonicalContactEntity({ ...resolution, ...(options.contact ?? {}) }),
      this.persistCanonicalChatEntity({ ...resolution, ...(options.chat ?? {}) }),
    ]);

    const cachePayload = this.cachePayloadForJid({
      remoteJid,
      remoteJidAlt: this.normalizeJid(resolution.remoteJidAlt),
      remoteLid: this.normalizeJid(resolution.remoteLid),
    } as CanonicalJidResolution);

    if (cachePayload && (cachePayload.remoteJid.includes('@s.whatsapp') || cachePayload.remoteJid.includes('@lid'))) {
      await saveOnWhatsappCache([cachePayload]);
    }

    if (updatedMessages > 0 && options.chat?.unreadMessages === undefined) {
      await this.updateChatUnreadMessages(remoteJid);
    }
  }

  private cachePayloadForJid(resolution: CanonicalJidResolution) {
    if (!resolution.remoteJid) {
      return null;
    }

    return {
      remoteJid: resolution.remoteJid,
      remoteJidAlt: resolution.remoteJidAlt,
      lid: resolution.remoteLid ? ('lid' as const) : undefined,
    };
  }

  private clearScheduledReconnect() {
    if (!this.reconnectTimeout) {
      return;
    }

    clearTimeout(this.reconnectTimeout);
    this.reconnectTimeout = null;
  }

  private scheduleReconnect() {
    if (this.reconnectTimeout) {
      this.logger.info({
        message: 'Reconnect already scheduled, skipping duplicate schedule',
        instanceName: this.instance.name,
      });
      return;
    }

    this.logger.info('Reconnecting in 3 seconds...');
    this.reconnectTimeout = setTimeout(() => {
      this.reconnectTimeout = null;

      const hasAuthArtifacts = this.hasAuthenticationArtifacts();
      const hasRecentlyScannedAuthArtifact = this.hasRecentlyScannedAuthenticationArtifact();
      const shouldForceReauthentication = !this.instance.wuid && !hasAuthArtifacts && !hasRecentlyScannedAuthArtifact;

      if (shouldForceReauthentication) {
        this.logger.warn({
          message: 'Reconnect requires fresh auth cycle for unauthenticated instance',
          instanceName: this.instance.name,
        });
      } else if (!this.instance.wuid && hasAuthArtifacts) {
        this.logger.info({
          message: 'Keeping existing auth artifact during unauthenticated reconnect',
          instanceName: this.instance.name,
        });
      } else if (!this.instance.wuid && hasRecentlyScannedAuthArtifact) {
        this.logger.info({
          message: 'Keeping recently scanned auth artifact during unauthenticated reconnect',
          instanceName: this.instance.name,
        });
      }

      const reconnectAttempt = shouldForceReauthentication
        ? this.forceReauthentication(this.phoneNumber)
        : this.connectToWhatsapp(this.phoneNumber);

      void reconnectAttempt.catch((error) => {
        this.logger.error({
          message: 'Scheduled reconnect failed',
          instanceName: this.instance.name,
          error: error?.toString?.() ?? String(error),
        });
      });
    }, 3000);
  }

  public async logoutInstance({
    permanent = false,
    waitForEventProcessing = true,
  }: { permanent?: boolean; waitForEventProcessing?: boolean } = {}) {
    // Mark instance as deleting to prevent reconnection attempts
    this.isDeleting = true;
    this.endSession = true;
    this.clearScheduledReconnect();
    await this.waitForConnectionAttemptIdle();
    if (waitForEventProcessing) {
      await this.waitForEventProcessingIdle();
    }

    const previousClient = this.client;
    this.client = null;

    this.messageProcessor.onDestroy();

    if (previousClient) {
      try {
        await previousClient.logout('Log out instance: ' + this.instanceName);
      } catch (error) {
        this.logger.error({ message: 'Error during logout', error });
      }

      // Improved socket cleanup
      try {
        previousClient.ws?.close();
        previousClient.end(new Error('Instance logout'));
      } catch (error) {
        this.logger.error({ message: 'Error during socket cleanup', error });
      }
    }

    await this.clearPersistedAuthState();

    this.instance.authState = undefined;
    this.instance.qrcode = { count: 0 };
    this.instance.wuid = null;
    this.instance.profilePictureUrl = null;
    this.scannedAuthenticationArtifactAt = null;
    this.stateConnection = {
      state: 'close',
      statusReason: DisconnectReason.loggedOut,
    };
    this.isDeleting = permanent;
  }

  public hasLiveConnection(): boolean {
    return Boolean(
      this.stateConnection.state === 'open' &&
        this.client?.ws?.isOpen &&
        this.client?.user &&
        this.instance.authState?.state?.creds?.registered,
    );
  }

  private async waitForEventProcessingIdle() {
    let pending = this.eventProcessingQueue;
    await pending;

    while (pending !== this.eventProcessingQueue) {
      pending = this.eventProcessingQueue;
      await pending;
    }
  }

  private async waitForConnectionAttemptIdle() {
    const pending = this.connectInFlight;
    if (!pending) {
      return;
    }

    try {
      await pending;
    } catch {
      // The lifecycle operation still has to clean up a failed connection attempt.
    }
  }

  public async hasDurableLiveConnection(): Promise<boolean> {
    await this.waitForEventProcessingIdle();
    if (!this.hasLiveConnection()) {
      return false;
    }

    return (await this.hasPersistedAuthenticationCredentials()) === true;
  }

  public async hasPersistedAuthenticationCredentials(): Promise<boolean | null> {
    if (Date.now() - this.persistedAuthCheckedAt <= this.persistedAuthCheckTtlMs) {
      return this.persistedAuthRegistered;
    }

    try {
      const authState = await this.defineAuthState();
      if (!authState) {
        return null;
      }

      this.persistedAuthRegistered = Boolean(authState.state.creds.registered);
      this.persistedAuthCheckedAt = Date.now();
      return this.persistedAuthRegistered;
    } catch (error) {
      this.logger.warn({
        message: 'Failed to verify persisted WhatsApp authentication credentials',
        instanceName: this.instance.name,
        error: error?.toString?.() ?? String(error),
      });
      return null;
    }
  }

  public hasAuthenticationArtifacts(): boolean {
    return Boolean(this.instance.qrcode?.base64 || this.instance.qrcode?.code || this.instance.qrcode?.pairingCode);
  }

  private hasRecentlyScannedAuthenticationArtifact(): boolean {
    if (!this.scannedAuthenticationArtifactAt) {
      return false;
    }

    return Date.now() - this.scannedAuthenticationArtifactAt <= this.SCANNED_AUTHENTICATION_ARTIFACT_TTL_MS;
  }

  private markAuthenticationArtifactScanned() {
    this.scannedAuthenticationArtifactAt = Date.now();
  }

  private shouldResetEndedUnauthenticatedSession(): boolean {
    return this.endSession && !this.isDeleting && !this.instance.wuid;
  }

  private resetEndedUnauthenticatedSessionForFreshConnect() {
    this.logger.info({
      message: 'Resetting ended unauthenticated session for a fresh WhatsApp auth cycle',
      instanceName: this.instance.name,
    });

    this.endSession = false;
    this.clearScheduledReconnect();
    this.instance.qrcode = { count: 0 };
    this.scannedAuthenticationArtifactAt = null;
    this.stateConnection = {
      state: 'close',
      statusReason: this.stateConnection.statusReason,
    };
  }

  public async prepareForFreshConnectAttempt() {
    this.initialConnectionRecoveryAttempted = false;
    const persistedAuthRegistered =
      this.stateConnection.state === 'open' ? await this.hasPersistedAuthenticationCredentials() : null;

    if (this.stateConnection.state === 'open' && (!this.hasLiveConnection() || persistedAuthRegistered === false)) {
      this.clearScheduledReconnect();
      try {
        this.client?.ws?.close();
        this.client?.end(new Error('Reset stale open session'));
      } catch (error) {
        this.logger.warn({
          message: 'Failed to cleanup stale open session before reconnect',
          instanceName: this.instance.name,
          error: error?.toString?.() ?? String(error),
        });
      }

      this.client = null;
      this.instance.authState = undefined;
      this.instance.qrcode = { count: 0 };
      this.scannedAuthenticationArtifactAt = null;
      this.stateConnection = { state: 'close' };
      this.endSession = false;
      this.isDeleting = false;
    }

    if (this.shouldResetEndedUnauthenticatedSession()) {
      this.resetEndedUnauthenticatedSessionForFreshConnect();
    }
  }

  public async forceReauthentication(number?: string): Promise<WASocket> {
    if (this.reauthenticationInFlight) {
      return this.reauthenticationInFlight;
    }

    await this.waitForConnectionAttemptIdle();
    await this.waitForEventProcessingIdle();

    return this.startForceReauthentication(number);
  }

  private async forceReauthenticationFromEvent(number?: string): Promise<WASocket> {
    return this.startForceReauthentication(number);
  }

  private async startForceReauthentication(number?: string): Promise<WASocket> {
    if (this.reauthenticationInFlight) {
      return this.reauthenticationInFlight;
    }

    const operation = this.performForceReauthentication(number);
    this.reauthenticationInFlight = operation;

    try {
      return await operation;
    } finally {
      if (this.reauthenticationInFlight === operation) {
        this.reauthenticationInFlight = null;
      }
    }
  }

  private async performForceReauthentication(number?: string): Promise<WASocket> {
    this.logger.warn({
      message: 'Forcing WhatsApp auth recovery after initial close without QR',
      instanceName: this.instance.name,
    });

    this.clearScheduledReconnect();
    this.endSession = true;
    this.isDeleting = true;

    const previousClient = this.client;
    this.client = null;

    if (previousClient) {
      try {
        await previousClient.logout('Force reauthentication');
      } catch (error) {
        this.logger.warn({
          message: 'Force reauthentication logout failed',
          error: error?.toString?.() ?? String(error),
        });
      }

      try {
        previousClient.ws?.close();
        previousClient.end(new Error('Force reauthentication'));
      } catch (error) {
        this.logger.warn({
          message: 'Force reauthentication socket cleanup failed',
          error: error?.toString?.() ?? String(error),
        });
      }
    }

    await this.clearPersistedAuthState();

    this.instance.authState = undefined;
    this.instance.qrcode = { count: 0 };
    this.scannedAuthenticationArtifactAt = null;
    this.instance.wuid = null;
    this.instance.profilePictureUrl = null;
    this.stateConnection = { state: 'close' };
    this.phoneNumber = number ?? this.phoneNumber ?? this.instance.number;

    const persisted = await this.updateInstanceRecord(
      {
        connectionStatus: 'close',
        ownerJid: null,
        profileName: null,
        profilePicUrl: null,
        disconnectionAt: new Date(),
        disconnectionReasonCode: DisconnectReason.loggedOut,
      },
      'reauthorize.close',
    );
    if (!persisted) {
      throw new InternalServerErrorException(`The "${this.instance.name}" instance could not enter reauthorization`);
    }

    this.endSession = false;
    this.isDeleting = false;
    return this.connectToWhatsapp(this.phoneNumber);
  }

  public async restart(): Promise<WASocket> {
    this.clearScheduledReconnect();
    this.endSession = true;
    this.isDeleting = true;
    await this.waitForConnectionAttemptIdle();
    await this.waitForEventProcessingIdle();

    const previousClient = this.client;
    this.client = null;

    try {
      previousClient?.ws?.close();
      previousClient?.end(new Error('restart'));
    } catch (error) {
      this.logger.warn({
        message: 'Failed to cleanup socket before restart',
        instanceName: this.instance.name,
        error: error?.toString?.() ?? String(error),
      });
    }

    this.stateConnection = { state: 'reconnecting' };
    const persisted = await this.updateInstanceRecord(
      {
        connectionStatus: 'reconnecting',
        disconnectionAt: new Date(),
      },
      'restart.reconnecting',
    );
    if (!persisted) {
      throw new InternalServerErrorException(`The "${this.instance.name}" instance could not restart`);
    }

    this.stateConnection = { state: 'close' };
    this.endSession = false;
    this.isDeleting = false;
    return this.connectToWhatsapp(this.phoneNumber);
  }

  public async getProfileName() {
    let profileName = this.client.user?.name ?? this.client.user?.verifiedName;
    if (!profileName) {
      const data = await this.prismaRepository.session.findUnique({ where: { sessionId: this.instanceId } });

      if (data) {
        const creds = JSON.parse(JSON.stringify(data.creds), BufferJSON.reviver);
        profileName = creds.me?.name || creds.me?.verifiedName;
      }
    }

    return profileName;
  }

  public async getProfileStatus() {
    const status = await this.client.fetchStatus(this.instance.wuid);

    return status[0]?.status;
  }

  public get profilePictureUrl() {
    return this.instance.profilePictureUrl;
  }

  public get qrCode(): wa.QrCode {
    return {
      pairingCode: this.instance.qrcode?.pairingCode,
      code: this.instance.qrcode?.code,
      base64: this.instance.qrcode?.base64,
      count: this.instance.qrcode?.count,
    };
  }

  private errorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    if (typeof error === 'string') {
      return error;
    }

    try {
      const serialized = JSON.stringify(error);
      return serialized && serialized !== '{}' ? serialized : String(error);
    } catch {
      return String(error);
    }
  }

  private async requestPairingCodeForCurrentQr(): Promise<string | null> {
    if (!this.phoneNumber) {
      return null;
    }

    try {
      await delay(1000);
      return await this.client.requestPairingCode(this.phoneNumber);
    } catch (error) {
      this.logger.warn({
        message: 'Pairing code request failed; continuing with QR authorization artifact',
        instanceName: this.instance.name,
        error: this.errorMessage(error),
      });
      return null;
    }
  }

  private async emitAuthenticationArtifactScannedUpdate() {
    this.markAuthenticationArtifactScanned();

    this.stateConnection = {
      state: 'connecting',
      statusReason: 200,
    };

    this.logger.info({
      message: 'Authentication artifact consumed; waiting for WhatsApp open event',
      instanceName: this.instance.name,
    });

    this.sendDataWebhook(Events.CONNECTION_UPDATE, {
      instance: this.instance.name,
      state: 'connecting',
      status: 'connecting',
      hasQr: false,
      wuid: this.instance.wuid,
    });

    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      this.chatwootService.eventWhatsapp(
        Events.CONNECTION_UPDATE,
        { instanceName: this.instance.name, instanceId: this.instanceId },
        {
          instance: this.instance.name,
          state: 'connecting',
          status: 'connecting',
          hasQr: false,
        },
      );
    }
  }

  private async connectionUpdate({ qr, connection, lastDisconnect }: Partial<ConnectionState>) {
    // Enhanced logging for connection updates
    const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
    this.logger.info({
      message: 'Connection update received',
      connection,
      hasQr: !!qr,
      statusCode,
      instanceName: this.instance.name,
      isDeleting: this.isDeleting,
      endSession: this.endSession,
    });

    if (qr) {
      this.clearScheduledReconnect();
      this.initialConnectionRecoveryAttempted = false;
      this.scannedAuthenticationArtifactAt = null;

      if (this.instance.qrcode.count === this.configService.get<QrCode>('QRCODE').LIMIT) {
        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          message: 'QR code limit reached, please login again',
          statusCode: DisconnectReason.badSession,
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.QRCODE_UPDATED,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { message: 'QR code limit reached, please login again', statusCode: DisconnectReason.badSession },
          );
        }

        this.sendDataWebhook(Events.CONNECTION_UPDATE, {
          instance: this.instance.name,
          state: 'refused',
          statusReason: DisconnectReason.connectionClosed,
          wuid: this.instance.wuid,
          profileName: await this.getProfileName(),
          profilePictureUrl: this.instance.profilePictureUrl,
        });

        this.endSession = true;

        return this.eventEmitter.emit('no.connection', this.instance.name);
      }

      this.instance.qrcode.count++;

      const color = this.configService.get<QrCode>('QRCODE').COLOR;

      const optsQrcode: QRCodeToDataURLOptions = {
        margin: 3,
        scale: 4,
        errorCorrectionLevel: 'H',
        color: { light: '#ffffff', dark: color },
      };

      this.instance.qrcode.pairingCode = await this.requestPairingCodeForCurrentQr();

      qrcode.toDataURL(qr, optsQrcode, (error, base64) => {
        if (error) {
          this.logger.error('Qrcode generate failed:' + error.toString());
          return;
        }

        this.instance.qrcode.base64 = base64;
        this.instance.qrcode.code = qr;

        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.QRCODE_UPDATED,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            {
              qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
            },
          );
        }
      });

      this.logger.log(
        `WhatsApp Web QR authorization artifact generated for instance ${this.instance.name} (qrcodeCount: ${this.instance.qrcode.count})`,
      );

      const persisted = await this.updateInstanceRecord({ connectionStatus: 'connecting' }, 'qrcode.connecting');
      if (!persisted) {
        return;
      }
    }

    if (!qr && !connection && !this.instance.wuid && (this.instance.qrcode?.count ?? 0) > 0) {
      await this.emitAuthenticationArtifactScannedUpdate();
    }

    if (connection) {
      this.stateConnection = {
        state: connection,
        statusReason: (lastDisconnect?.error as Boom)?.output?.statusCode ?? 200,
      };
    }

    if (connection === 'close') {
      // Check if instance is being deleted or session is ending
      if (this.isDeleting || this.endSession) {
        this.logger.info('Instance is being deleted/ended, skipping reconnection attempt');
        return;
      }

      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
      const codesToNotReconnect = [DisconnectReason.loggedOut, DisconnectReason.forbidden, 402, 406];

      // FIX: Do not reconnect if it's the initial connection (waiting for QR code)
      // This prevents infinite loop that blocks QR code generation
      const isInitialConnection = !this.instance.wuid && (this.instance.qrcode?.count ?? 0) === 0;

      if (isInitialConnection) {
        const recovered = await this.tryRecoverInitialConnectionWithoutQr(statusCode);
        if (recovered) {
          return;
        }

        await this.emitInitialConnectionFailure(statusCode, lastDisconnect);
        return;
      }

      const shouldReconnect = !codesToNotReconnect.includes(statusCode);

      this.logger.info({
        message: 'Connection closed, evaluating reconnection',
        statusCode,
        shouldReconnect,
        instanceName: this.instance.name,
      });

      if (shouldReconnect) {
        this.stateConnection = {
          state: 'reconnecting',
          statusReason: statusCode ?? 200,
        };

        const persisted = await this.updateInstanceRecord(
          {
            connectionStatus: 'reconnecting',
            disconnectionAt: new Date(),
            disconnectionReasonCode: statusCode,
            disconnectionObject: JSON.stringify(lastDisconnect),
          },
          'connection.reconnecting',
        );
        if (!persisted) {
          return;
        }

        this.sendDataWebhook(Events.CONNECTION_UPDATE, {
          instance: this.instance.name,
          ...this.stateConnection,
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.CONNECTION_UPDATE,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { instance: this.instance.name, status: 'reconnecting' },
          );
        }

        this.scheduleReconnect();
        return;
      } else {
        this.logger.info(`Skipping reconnection for status code ${statusCode} (code is in codesToNotReconnect list)`);
        this.sendDataWebhook(Events.STATUS_INSTANCE, {
          instance: this.instance.name,
          status: 'closed',
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });

        const persisted = await this.updateInstanceRecord(
          {
            connectionStatus: 'close',
            disconnectionAt: new Date(),
            disconnectionReasonCode: statusCode,
            disconnectionObject: JSON.stringify(lastDisconnect),
          },
          'connection.close',
        );
        if (!persisted) {
          return;
        }

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.STATUS_INSTANCE,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { instance: this.instance.name, status: 'closed' },
          );
        }

        this.eventEmitter.emit('logout.instance', this.instance.name, 'inner');
        this.client?.ws?.close();
        this.client.end(new Error('Close connection'));

        this.sendDataWebhook(Events.CONNECTION_UPDATE, { instance: this.instance.name, ...this.stateConnection });
      }
    }

    if (connection === 'open') {
      this.clearScheduledReconnect();
      this.initialConnectionRecoveryAttempted = false;
      this.scannedAuthenticationArtifactAt = null;
      this.instance.wuid = this.client.user.id.replace(/:\d+/, '');
      try {
        const profilePic = await this.profilePicture(this.instance.wuid);
        this.instance.profilePictureUrl = profilePic.profilePictureUrl;
      } catch {
        this.instance.profilePictureUrl = null;
      }
      const formattedWuid = this.instance.wuid.split('@')[0].padEnd(30, ' ');
      const formattedName = this.instance.name;
      this.logger.info(
        `
        ┌──────────────────────────────┐
        │    CONNECTED TO WHATSAPP     │
        └──────────────────────────────┘`.replace(/^ +/gm, '  '),
      );
      this.logger.info(
        `
        wuid: ${formattedWuid}
        name: ${formattedName}
      `,
      );

      const persisted = await this.updateInstanceRecord(
        {
          ownerJid: this.instance.wuid,
          profileName: (await this.getProfileName()) as string,
          profilePicUrl: this.instance.profilePictureUrl,
          connectionStatus: 'open',
        },
        'connection.open',
      );
      if (!persisted) {
        return;
      }

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
        this.chatwootService.eventWhatsapp(
          Events.CONNECTION_UPDATE,
          { instanceName: this.instance.name, instanceId: this.instanceId },
          { instance: this.instance.name, status: 'open' },
        );
        this.syncChatwootLostMessages();
      }

      this.sendDataWebhook(Events.CONNECTION_UPDATE, {
        instance: this.instance.name,
        wuid: this.instance.wuid,
        profileName: await this.getProfileName(),
        profilePictureUrl: this.instance.profilePictureUrl,
        ...this.stateConnection,
      });
    }

    if (connection === 'connecting') {
      const persisted = await this.updateInstanceRecord({ connectionStatus: 'connecting' }, 'connection.connecting');
      if (!persisted) {
        return;
      }

      this.sendDataWebhook(Events.CONNECTION_UPDATE, { instance: this.instance.name, ...this.stateConnection });
    }
  }

  private async updateInstanceRecord(data: Prisma.InstanceUpdateInput, context: string): Promise<boolean> {
    try {
      await this.prismaRepository.instance.update({
        where: { id: this.instanceId },
        data,
      });

      return true;
    } catch (error) {
      if (!(error instanceof Prisma.PrismaClientKnownRequestError) || error.code !== 'P2025') {
        throw error;
      }

      await this.handleMissingInstanceRecord(context);
      return false;
    }
  }

  private async clearPersistedAuthState() {
    const db = this.configService.get<Database>('DATABASE');
    const cache = this.configService.get<CacheConf>('CACHE');
    const provider = this.configService.get<ProviderSession>('PROVIDER');

    if (provider?.ENABLED) {
      const authState = await this.authStateProvider.authStateProvider(this.instance.id);

      await authState.removeCreds();
    }

    if (cache?.REDIS.ENABLED && cache?.REDIS.SAVE_INSTANCES) {
      const authState = await useMultiFileAuthStateRedisDb(this.instance.id, this.cache);

      await authState.removeCreds();
    }

    if (db.SAVE_DATA.INSTANCE) {
      const authState = await useMultiFileAuthStatePrisma(this.instance.id, this.cache);

      await authState.removeCreds();
    }

    const sessionExists = await this.prismaRepository.session.findFirst({ where: { sessionId: this.instanceId } });
    if (sessionExists) {
      await this.prismaRepository.session.delete({ where: { sessionId: this.instanceId } });
    }

    this.persistedAuthRegistered = false;
    this.persistedAuthCheckedAt = Date.now();
  }

  private async tryRecoverInitialConnectionWithoutQr(statusCode?: number): Promise<boolean> {
    if (this.initialConnectionRecoveryInFlight) {
      return this.initialConnectionRecoveryInFlight;
    }

    if (this.hasAuthenticationArtifacts()) {
      this.logger.info({
        message: 'Initial connection recovery skipped because an auth artifact is already available',
        instanceName: this.instance.name,
        statusCode,
      });
      return true;
    }

    if (this.initialConnectionRecoveryAttempted) {
      this.logger.warn({
        message: 'Initial connection recovery already attempted and QR is still missing',
        instanceName: this.instance.name,
        statusCode,
      });
      return false;
    }

    this.initialConnectionRecoveryAttempted = true;

    this.initialConnectionRecoveryInFlight = (async () => {
      try {
        if (this.hasAuthenticationArtifacts()) {
          return true;
        }

        const persistedAuthRegistered = await this.hasPersistedAuthenticationCredentials();
        if (persistedAuthRegistered !== false) {
          this.logger.warn({
            message: 'Automatic auth recovery preserved persisted WhatsApp credentials',
            instanceName: this.instance.name,
            statusCode,
            persistedAuthRegistered,
          });
          return false;
        }

        await this.forceReauthenticationFromEvent(this.phoneNumber ?? this.instance.number);
        return true;
      } catch (error) {
        this.logger.error({
          message: 'Forced auth recovery failed',
          instanceName: this.instance.name,
          statusCode,
          error: error?.toString?.() ?? String(error),
        });
        return false;
      } finally {
        this.initialConnectionRecoveryInFlight = null;
      }
    })();

    return this.initialConnectionRecoveryInFlight;
  }

  private async emitInitialConnectionFailure(
    statusCode: number | undefined,
    lastDisconnect: Partial<ConnectionState>['lastDisconnect'],
  ) {
    this.logger.warn({
      message: 'QR code was not generated after initial connection recovery',
      instanceName: this.instance.name,
      statusCode,
    });

    this.stateConnection = {
      state: 'close',
      statusReason: statusCode ?? 428,
    };

    const disconnectionAt = new Date();
    const disconnectionObject = JSON.stringify(lastDisconnect);
    const payload = {
      instance: this.instance.name,
      status: 'reauth_required',
      message: 'Authentication artifacts were not generated after reconnect',
      disconnectionAt,
      disconnectionReasonCode: statusCode,
      disconnectionObject,
    };

    this.sendDataWebhook(Events.STATUS_INSTANCE, payload);

    const persisted = await this.updateInstanceRecord(
      {
        connectionStatus: 'close',
        disconnectionAt,
        disconnectionReasonCode: statusCode,
        disconnectionObject,
      },
      'connection.initial-close-without-qr',
    );
    if (!persisted) {
      return;
    }

    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      this.chatwootService.eventWhatsapp(
        Events.STATUS_INSTANCE,
        { instanceName: this.instance.name, instanceId: this.instanceId },
        payload,
      );
    }
  }

  private async handleMissingInstanceRecord(context: string) {
    this.logger.warn({
      message: 'Instance record missing during lifecycle update, removing stale runtime instance',
      instanceName: this.instance.name,
      instanceId: this.instanceId,
      context,
    });

    this.isDeleting = true;
    this.endSession = true;
    this.clearScheduledReconnect();

    try {
      await this.logoutInstance({ permanent: true, waitForEventProcessing: false });
    } catch (error) {
      this.logger.warn({
        message: 'Failed to logout stale runtime instance after missing record',
        instanceName: this.instance.name,
        instanceId: this.instanceId,
        error: error?.toString(),
      });
    }

    try {
      await this.cache.delete(this.instance.name);
      await this.cache.delete(this.instanceId);
    } catch (error) {
      this.logger.warn({
        message: 'Failed to clear stale runtime cache after missing record',
        instanceName: this.instance.name,
        instanceId: this.instanceId,
        error: error?.toString(),
      });
    }

    delete waMonitor.waInstances[this.instance.name];
  }

  private async getMessage(key: proto.IMessageKey, full = false) {
    try {
      const storedMessage = (await this.findStoredMessageByKey(
        key as ExtendedIMessageKey,
      )) as proto.IWebMessageInfo | null;

      if (!storedMessage) {
        return { conversation: '' };
      }

      if (full) {
        return storedMessage;
      }
      if (storedMessage.message?.pollCreationMessage) {
        const messageSecretBase64 = storedMessage.message?.messageContextInfo?.messageSecret;

        if (typeof messageSecretBase64 === 'string') {
          const messageSecret = Buffer.from(messageSecretBase64, 'base64');

          const msg = {
            messageContextInfo: { messageSecret },
            pollCreationMessage: storedMessage.message?.pollCreationMessage,
          };

          return msg;
        }
      }

      return storedMessage.message;
    } catch {
      return { conversation: '' };
    }
  }

  private async defineAuthState() {
    const db = this.configService.get<Database>('DATABASE');
    const cache = this.configService.get<CacheConf>('CACHE');

    const provider = this.configService.get<ProviderSession>('PROVIDER');

    if (provider?.ENABLED) {
      return await this.authStateProvider.authStateProvider(this.instance.id);
    }

    if (cache?.REDIS.ENABLED && cache?.REDIS.SAVE_INSTANCES) {
      this.logger.info('Redis enabled');
      return await useMultiFileAuthStateRedisDb(this.instance.id, this.cache);
    }

    if (db.SAVE_DATA.INSTANCE) {
      return await useMultiFileAuthStatePrisma(this.instance.id, this.cache);
    }
  }

  private async createClient(number?: string): Promise<WASocket> {
    this.instance.authState = await this.defineAuthState();

    if (number) {
      this.phoneNumber = number;
      this.logger.info(`Phone number: ${number}`);
    }

    // Fetch latest WhatsApp Web version automatically
    const baileysVersion = await fetchLatestWaWebVersion({}, this.cache);
    const version = baileysVersion.version;

    const log = `Baileys version: ${version.join('.')}`;
    this.logger.info(log);

    const error = baileysVersion?.error ?? null;
    if (error) {
      this.logger.error({ local: 'fetchLatestWaWebVersion', error });
    }

    this.logger.info(`Group Ignore: ${this.localSettings.groupsIgnore}`);

    let options;

    if (this.localProxy?.enabled) {
      this.logger.verbose('Proxy enabled');

      if (this.localProxy?.host?.includes('proxyscrape')) {
        try {
          const response = await axios.get(this.localProxy?.host);
          const text = response.data;
          const proxyUrls = text.split('\r\n');
          const rand = Math.floor(Math.random() * Math.floor(proxyUrls.length));
          const proxyUrl = 'http://' + proxyUrls[rand];
          this.logger.info('Proxy url: ' + proxyUrl);
          options = { agent: makeProxyAgent(proxyUrl), fetchAgent: makeProxyAgentUndici(proxyUrl) };
        } catch (error) {
          this.logger.error(error);
        }
      } else {
        options = {
          agent: makeProxyAgent({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
          fetchAgent: makeProxyAgentUndici({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
        };
      }
    }

    const socketConfig: UserFacingSocketConfig = {
      ...options,
      version,
      logger: P({ level: this.logBaileys }),
      printQRInTerminal: false,
      auth: {
        creds: this.instance.authState.state.creds,
        keys: makeCacheableSignalKeyStore(this.instance.authState.state.keys, P({ level: 'error' }) as any),
      },
      msgRetryCounterCache: this.msgRetryCounterCache,
      generateHighQualityLinkPreview: true,
      getMessage: async (key) => (await this.getMessage(key)) as Promise<proto.IMessage>,
      // Removido browserOptions para usar Multi-Device nativo (não WebClient)
      markOnlineOnConnect: this.localSettings.alwaysOnline,
      retryRequestDelayMs: 350,
      maxMsgRetryCount: 4,
      // These startup queries are optional and currently rejected by WA
      // for part of our multi-device sessions, which only adds noisy
      // "unexpected error in 'init queries'" logs without improving runtime state.
      fireInitQueries: false,
      connectTimeoutMs: 30_000,
      keepAliveIntervalMs: 30_000,
      qrTimeout: 45_000,
      emitOwnEvents: false,
      shouldIgnoreJid: (jid) => {
        if (this.localSettings.syncFullHistory && isJidGroup(jid)) {
          return false;
        }

        const isGroupJid = this.localSettings.groupsIgnore && isJidGroup(jid);
        const isBroadcast = !this.localSettings.readStatus && isJidBroadcast(jid);
        const isNewsletter = isJidNewsletter(jid);

        return isGroupJid || isBroadcast || isNewsletter;
      },
      syncFullHistory: this.localSettings.syncFullHistory,
      shouldSyncHistoryMessage: (msg: proto.Message.IHistorySyncNotification) => {
        return this.historySyncNotification(msg);
      },
      cachedGroupMetadata: this.getGroupMetadataCache,
      userDevicesCache: this.userDevicesCache,
      transactionOpts: { maxCommitRetries: 10, delayBetweenTriesMs: 3000 },
      patchMessageBeforeSending(message) {
        if (
          message.deviceSentMessage?.message?.listMessage?.listType === proto.Message.ListMessage.ListType.PRODUCT_LIST
        ) {
          message = JSON.parse(JSON.stringify(message));

          message.deviceSentMessage.message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        if (message.listMessage?.listType == proto.Message.ListMessage.ListType.PRODUCT_LIST) {
          message = JSON.parse(JSON.stringify(message));

          message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        return message;
      },
    };

    this.endSession = false;

    this.client = makeWASocket(socketConfig);

    if (this.localSettings.wavoipToken && this.localSettings.wavoipToken.length > 0) {
      useVoiceCallsBaileys(this.localSettings.wavoipToken, this.client, this.connectionStatus.state as any, true);
    }

    this.eventHandler();

    this.client.ws.on('CB:call', (packet) => {
      this.logger.debug({ local: 'socket.call', packet });
      const payload = { event: 'CB:call', packet: packet };
      this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
    });

    this.client.ws.on('CB:ack,class:call', (packet) => {
      this.logger.debug({ local: 'socket.callAck', packet });
      const payload = { event: 'CB:ack,class:call', packet: packet };
      this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
    });

    this.phoneNumber = number;

    return this.client;
  }

  public async connectToWhatsapp(number?: string): Promise<WASocket> {
    if (this.shouldResetEndedUnauthenticatedSession()) {
      this.resetEndedUnauthenticatedSessionForFreshConnect();
    }

    if (this.isDeleting || this.endSession) {
      throw new BadRequestException(`The "${this.instance.name}" instance is being deleted`);
    }

    this.clearScheduledReconnect();

    if (this.connectInFlight) {
      this.logger.info({
        message: 'Connection attempt already in progress, reusing existing promise',
        instanceName: this.instance.name,
      });
      return this.connectInFlight;
    }

    if (this.client && ['open', 'connecting'].includes(this.stateConnection.state)) {
      this.logger.info({
        message: 'Instance already connected or connecting, skipping duplicate connect',
        instanceName: this.instance.name,
        state: this.stateConnection.state,
      });
      return this.client;
    }

    if (this.client && this.stateConnection.state === 'reconnecting') {
      this.logger.info({
        message: 'Stale reconnect state detected, resetting socket before reconnect',
        instanceName: this.instance.name,
      });

      try {
        this.client.ws?.close();
        this.client.end(new Error('Reconnect retry'));
      } catch (error) {
        this.logger.warn({
          message: 'Failed to cleanup stale reconnect socket before reconnect',
          instanceName: this.instance.name,
          error: error?.toString?.() ?? String(error),
        });
      }

      this.stateConnection = {
        state: 'close',
        statusReason: this.stateConnection.statusReason,
      };
    }

    const connectPromise = (async () => {
      try {
        await Promise.all([this.loadChatwoot(), this.loadSettings(), this.loadWebhook(), this.loadProxy()]);

        // Remontar o messageProcessor para garantir que está funcionando após reconexão
        this.messageProcessor.mount({
          onMessageReceive: this.messageHandle['messages.upsert'].bind(this),
        });

        return await this.createClient(number);
      } catch (error) {
        this.logger.error(error);
        throw new InternalServerErrorException(error?.toString());
      }
    })();

    this.connectInFlight = connectPromise;

    try {
      return await connectPromise;
    } finally {
      if (this.connectInFlight === connectPromise) {
        this.connectInFlight = null;
      }
    }
  }

  public async reloadConnection(): Promise<WASocket> {
    try {
      return await this.createClient(this.phoneNumber);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  private readonly chatHandle = {
    'chats.upsert': async (chats: Chat[]) => {
      const existingChatIds = await this.prismaRepository.chat.findMany({
        where: { instanceId: this.instanceId },
        select: { remoteJid: true },
      });

      const existingChatIdSet = new Set(existingChatIds.map((chat) => chat.remoteJid));
      const normalizedChats = (
        await mapWithConcurrencyLimit(chats, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (chat) =>
          this.normalizeChatPayload(chat),
        )
      ).filter(Boolean);

      const chatsToInsert = normalizedChats.filter((chat) => !existingChatIdSet?.has(chat.remoteJid));

      this.sendDataWebhook(Events.CHATS_UPSERT, chatsToInsert);

      if (normalizedChats.length > 0 && this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
        await eachWithConcurrencyLimit(normalizedChats, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (chat) => {
          await this.reconcileIdentityAliases(chat, {
            chat: { name: chat.name, unreadMessages: chat.unreadMessages },
            syncMessages: false,
          });
        });
      }
    },

    'chats.update': async (
      chats: Partial<
        proto.IConversation & { lastMessageRecvTimestamp?: number } & {
          conditional: (bufferedData: BufferedEventData) => boolean;
        }
      >[],
    ) => {
      const chatsRaw = (
        await mapWithConcurrencyLimit(chats, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (chat) =>
          this.normalizeChatPayload(chat as any),
        )
      ).filter(Boolean);

      this.sendDataWebhook(Events.CHATS_UPDATE, chatsRaw);

      if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
        await eachWithConcurrencyLimit(chatsRaw, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (chat) => {
          await this.reconcileIdentityAliases(chat, {
            chat: { name: chat.name, unreadMessages: chat.unreadMessages },
            syncMessages: false,
          });
        });
      }
    },

    'chats.delete': async (chats: string[]) => {
      const resolutions = await mapWithConcurrencyLimit(chats, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (chat) =>
        this.resolveCanonicalJidWithNative(chat),
      );

      await eachWithConcurrencyLimit(resolutions, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (resolution) => {
        const remoteJidCandidates = [
          resolution.remoteJid,
          resolution.rawRemoteJid,
          resolution.remoteJidAlt,
          resolution.remoteLid,
        ].filter((jid, index, array) => !!jid && array.indexOf(jid) === index);

        await this.prismaRepository.chat.deleteMany({
          where: { instanceId: this.instanceId, remoteJid: { in: remoteJidCandidates } },
        });
      });

      this.sendDataWebhook(
        Events.CHATS_DELETE,
        resolutions.map((resolution, index) => resolution.remoteJid || chats[index]),
      );
    },
  };

  private readonly contactHandle = {
    'contacts.upsert': async (contacts: Contact[]) => {
      try {
        const contactsRaw = (
          await mapWithConcurrencyLimit(contacts, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (contact) =>
            this.normalizeContactPayload(contact),
          )
        ).filter(Boolean);

        if (contactsRaw.length > 0) {
          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactsRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.createMany({
              data: contactsRaw.map((contact) => this.contactPersistencePayload(contact)),
              skipDuplicates: true,
            });

          const usersContacts = contactsRaw.filter(
            (c) => c.remoteJid.includes('@s.whatsapp') || c.remoteJid.includes('@lid'),
          );
          if (usersContacts.length > 0) {
            await saveOnWhatsappCache(
              usersContacts
                .map((contact) =>
                  this.cachePayloadForJid({
                    remoteJid: contact.remoteJid,
                    remoteJidAlt: contact.remoteJidAlt,
                    remoteLid: contact.remoteLid,
                  }),
                )
                .filter(Boolean) as any,
            );
          }
        }

        if (
          this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
          this.localChatwoot?.enabled &&
          this.localChatwoot.importContacts &&
          contactsRaw.length
        ) {
          this.chatwootService.addHistoryContacts(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            contactsRaw as unknown as ContactModel[],
          );
          chatwootImport.importHistoryContacts(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            this.localChatwoot,
          );
        }

        const updatedContacts = (
          await mapWithConcurrencyLimit(contacts, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (contact) => {
            const normalizedContact = await this.normalizeContactPayload(contact);
            if (!normalizedContact) {
              return null;
            }

            return {
              ...normalizedContact,
              profilePicUrl: await this.resolveProfilePictureUrlForIdentity(normalizedContact),
            };
          })
        ).filter(Boolean);

        if (updatedContacts.length > 0) {
          const usersContacts = updatedContacts.filter(
            (c) => c.remoteJid.includes('@s.whatsapp') || c.remoteJid.includes('@lid'),
          );
          if (usersContacts.length > 0) {
            await saveOnWhatsappCache(
              usersContacts
                .map((contact) =>
                  this.cachePayloadForJid({
                    remoteJid: contact.remoteJid,
                    remoteJidAlt: contact.remoteJidAlt,
                    remoteLid: contact.remoteLid,
                  }),
                )
                .filter(Boolean) as any,
            );
          }

          this.sendDataWebhook(Events.CONTACTS_UPDATE, updatedContacts);
          await eachWithConcurrencyLimit(updatedContacts, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (contact) => {
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS) {
              await this.reconcileIdentityAliases(contact, {
                contact: { pushName: contact.pushName, profilePicUrl: contact.profilePicUrl },
              });
            }

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              const instance = { instanceName: this.instance.name, instanceId: this.instance.id };
              await this.chatwootService.reconcileContactIdentity(instance, {
                remoteJid: contact.remoteJid,
                remoteJidAlt: contact.remoteJidAlt,
                remoteLid: contact.remoteLid,
                pushName: contact.pushName,
                profilePicUrl: contact.profilePicUrl,
              });
            }
          });
        }
      } catch (error) {
        this.logger.error({ local: 'contacts.upsert', error });
      }
    },

    'contacts.update': async (contacts: Partial<Contact>[]) => {
      const contactsRaw: any[] = [];
      for await (const contact of contacts) {
        this.logger.debug({ local: 'contacts.update', contact });
        const normalizedContact = await this.normalizeContactPayload(contact as Contact);
        if (!normalizedContact) {
          continue;
        }

        contactsRaw.push({
          ...normalizedContact,
          profilePicUrl: await this.resolveProfilePictureUrlForIdentity(normalizedContact),
        });
      }

      const usersContacts = contactsRaw.filter(
        (c) => c.remoteJid.includes('@s.whatsapp') || c.remoteJid.includes('@lid'),
      );
      if (usersContacts.length > 0) {
        await saveOnWhatsappCache(
          usersContacts
            .map((contact) =>
              this.cachePayloadForJid({
                remoteJid: contact.remoteJid,
                remoteJidAlt: contact.remoteJidAlt,
                remoteLid: contact.remoteLid,
              }),
            )
            .filter(Boolean) as any,
        );
      }

      this.sendDataWebhook(Events.CONTACTS_UPDATE, contactsRaw);

      await eachWithConcurrencyLimit(contactsRaw, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (contact) => {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS) {
          await this.reconcileIdentityAliases(contact, {
            contact: { pushName: contact.pushName, profilePicUrl: contact.profilePicUrl },
          });
        }

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          await this.chatwootService.reconcileContactIdentity(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            {
              remoteJid: contact.remoteJid,
              remoteJidAlt: contact.remoteJidAlt,
              remoteLid: contact.remoteLid,
              pushName: contact.pushName,
              profilePicUrl: contact.profilePicUrl,
            },
          );
        }
      });

      //const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
    },
  };

  private readonly messageHandle = {
    'messaging-history.set': async ({
      messages,
      chats,
      contacts,
      lidPnMappings,
      isLatest,
      progress,
      syncType,
    }: {
      chats: Chat[];
      contacts: Contact[];
      messages: WAMessage[];
      lidPnMappings?: Array<{ lid?: string | null; pn?: string | null }>;
      isLatest?: boolean;
      progress?: number;
      syncType?: proto.HistorySync.HistorySyncType;
    }) => {
      try {
        // Reset counters when a new sync starts (progress resets or decreases)
        if (progress <= this.historySyncLastProgress) {
          this.historySyncMessageCount = 0;
          this.historySyncChatCount = 0;
          this.historySyncContactCount = 0;
        }
        this.historySyncLastProgress = progress ?? -1;

        if (syncType === proto.HistorySync.HistorySyncType.ON_DEMAND) {
          this.logger.debug({ local: 'messaging-history.set.on-demand', messages });
        }
        this.logger.info(
          `recv ${chats.length} chats, ${contacts.length} contacts, ${messages.length} msgs (is latest: ${isLatest}, progress: ${progress}%), type: ${syncType}`,
        );

        const instance: InstanceDto = { instanceName: this.instance.name };

        // Chatwoot history import currently runs without a day-window cutoff.
        // We keep the config field for future rollback, but do not enforce it here.
        const timestampLimitToImport = null;

        const contactsMap = new Map();
        const contactsMapLidJid = new Map();
        const historyMappings = this.dedupeLidPnMappings([
          ...((lidPnMappings ?? []) as Array<{ lid?: string | null; pn?: string | null }>),
          ...contacts.map((contact) => ({
            lid: this.isLidJid(contact.id) ? contact.id : ((contact as any)?.lid ?? undefined),
            pn: contact.phoneNumber,
          })),
          ...chats.map((chat) => this.extractLidPnMapping(chat.id, (chat as any)?.accountLid)),
        ]);

        if (historyMappings.length) {
          await this.ingestIdentityMappings(historyMappings, {
            reconcileDatabase: true,
            syncMessages: false,
          });
        }

        const historyMappingByLid = new Map(historyMappings.map(({ lid, pn }) => [lid, pn] as const));

        for (const contact of contacts) {
          let jid = null;

          if (contact?.id?.search('@lid') !== -1) {
            jid = contact.phoneNumber ?? historyMappingByLid.get(contact.id) ?? null;
          }

          if (!jid) {
            jid = contact?.id;
          }

          if (contact.id && (contact.notify || contact.name)) {
            contactsMap.set(contact.id, { name: contact.notify ?? contact.name, jid });
            if (jid && jid !== contact.id) {
              contactsMap.set(jid, { name: contact.notify ?? contact.name, jid });
            }
          }

          contactsMapLidJid.set(contact.id, { jid });
          if (jid && jid !== contact.id) {
            contactsMapLidJid.set(jid, { jid });
          }
        }

        const chatsRepository = new Set(
          (await this.prismaRepository.chat.findMany({ where: { instanceId: this.instanceId } })).map(
            (chat) => chat.remoteJid,
          ),
        );
        const normalizedHistoryChats = (
          await mapWithConcurrencyLimit(chats, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (chat) =>
            this.normalizeChatPayload({
              ...chat,
              id: chat.id,
              name: chat.name,
              unreadCount: (chat as any).unreadCount,
              phoneNumber: contactsMapLidJid.get(chat.id)?.jid,
              remoteLid: chat.accountLid,
            } as any),
          )
        ).filter(Boolean);
        const chatsRaw = normalizedHistoryChats.filter((chat) => !chatsRepository.has(chat.remoteJid));

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await eachWithConcurrencyLimit(chatsRaw, CONTACT_UPDATE_PERSISTENCE_CONCURRENCY, async (chat) => {
            await this.reconcileIdentityAliases(chat, {
              chat: { name: chat.name, unreadMessages: chat.unreadMessages },
              syncMessages: false,
            });
          });
        }

        this.historySyncChatCount += chatsRaw.length;

        this.sendDataWebhook(Events.CHATS_SET, chatsRaw);

        const messagesRaw: any[] = [];

        const messagesRepository: Set<string> = new Set(
          chatwootImport.getRepositoryMessagesCache(instance) ??
            (
              await this.prismaRepository.message.findMany({
                select: { key: true },
                where: { instanceId: this.instanceId },
              })
            )
              .map((message) => {
                return this.buildMessageIdentityLookupKey(message.key as ExtendedIMessageKey);
              })
              .filter((messageKey): messageKey is string => !!messageKey),
        );

        if (chatwootImport.getRepositoryMessagesCache(instance) === null) {
          chatwootImport.setRepositoryMessagesCache(instance, messagesRepository);
        }

        for (const m of messages) {
          if (!m.message || !m.key || !m.messageTimestamp) {
            continue;
          }

          if (Long.isLong(m?.messageTimestamp)) {
            m.messageTimestamp = m.messageTimestamp?.toNumber();
          }

          if (timestampLimitToImport !== null && m.messageTimestamp <= timestampLimitToImport) {
            continue;
          }

          if (!m.pushName && !m.key.fromMe) {
            const participantJid = m.participant || m.key.participant || m.key.remoteJid;
            if (participantJid && contactsMap.has(participantJid)) {
              m.pushName = contactsMap.get(participantJid).name;
            } else if (participantJid) {
              m.pushName = participantJid.split('@')[0];
            }
          }

          const keyAny = m.key as any;
          const historicalIdentity = this.resolveCanonicalJid(keyAny.remoteJid, keyAny.remoteJidAlt, {
            phoneNumber: contactsMapLidJid.get(keyAny.remoteJid)?.jid,
            remoteLid: keyAny.remoteLid,
          });

          if (historicalIdentity.remoteJid) {
            keyAny.remoteJid = historicalIdentity.remoteJid;
          }
          if (historicalIdentity.remoteJidAlt) {
            keyAny.remoteJidAlt = historicalIdentity.remoteJidAlt;
          }
          if (historicalIdentity.remoteLid) {
            keyAny.remoteLid = historicalIdentity.remoteLid;
          }
          if (historicalIdentity.addressingMode) {
            keyAny.addressingMode = historicalIdentity.addressingMode;
          }

          const historyMessageKey = this.buildMessageIdentityLookupKey(keyAny);
          if (historyMessageKey && messagesRepository?.has(historyMessageKey)) {
            continue;
          }

          const preparedMessage = await this.prepareMessageWithNative(m, {
            phoneNumber: contactsMapLidJid.get(keyAny.remoteJid)?.jid,
            remoteLid: keyAny.remoteLid,
          });

          messagesRaw.push(preparedMessage);

          const preparedMessageKey = this.buildMessageIdentityLookupKey(preparedMessage.key as ExtendedIMessageKey);
          if (preparedMessageKey) {
            messagesRepository.add(preparedMessageKey);
          }
        }

        this.historySyncMessageCount += messagesRaw.length;

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await this.prismaRepository.message.createMany({ data: messagesRaw, skipDuplicates: true });
        }

        this.sendDataWebhook(Events.MESSAGES_SET, [...messagesRaw], true, undefined, {
          isLatest,
          progress,
        });

        if (
          this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
          this.localChatwoot?.enabled &&
          this.localChatwoot.importMessages &&
          messagesRaw.length > 0
        ) {
          this.chatwootService.addHistoryMessages(
            instance,
            messagesRaw.filter((msg) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid)),
          );
        }

        this.historySyncContactCount += contacts.length;

        await this.contactHandle['contacts.upsert'](contacts as Contact[]);

        if (progress === 100) {
          this.sendDataWebhook(Events.MESSAGING_HISTORY_SET, {
            messageCount: this.historySyncMessageCount,
            chatCount: this.historySyncChatCount,
            contactCount: this.historySyncContactCount,
          });

          this.historySyncMessageCount = 0;
          this.historySyncChatCount = 0;
          this.historySyncContactCount = 0;
          this.historySyncLastProgress = -1;
        }

        contacts = undefined;
        messages = undefined;
        chats = undefined;
      } catch (error) {
        this.logger.error(error);
      }
    },

    'messages.upsert': async (
      { messages, type, requestId }: { messages: WAMessage[]; type: MessageUpsertType; requestId?: string },
      settings: any,
    ) => {
      try {
        for (const received of messages) {
          if (
            received?.messageStubParameters?.some?.((param) =>
              [
                'No matching sessions found for message',
                'Bad MAC',
                'failed to decrypt message',
                'SessionError',
                'Invalid PreKey ID',
                'No session record',
                'No session found to decrypt message',
                'Message absent from node',
              ].some((err) => param?.includes?.(err)),
            )
          ) {
            this.logger.warn({
              local: 'messages.upsert.stub-ignored',
              received,
            });
            continue;
          }
          if (received.message?.conversation || received.message?.extendedTextMessage?.text) {
            const text = received.message?.conversation || received.message?.extendedTextMessage?.text;

            if (text == 'requestPlaceholder' && !requestId) {
              const messageId = await this.client.requestPlaceholderResend(received.key);

              this.logger.debug(`Requested placeholder resync for message id=${messageId}`);
            } else if (requestId) {
              this.logger.debug({
                local: 'messages.upsert.placeholder-received',
                requestId,
                received,
              });
            }

            if (text == 'onDemandHistSync') {
              const messageId = await this.client.fetchMessageHistory(50, received.key, received.messageTimestamp!);
              this.logger.debug(`Requested on-demand sync for message id=${messageId}`);
            }
          }

          const editedMessage =
            received?.message?.protocolMessage || received?.message?.editedMessage?.message?.protocolMessage;

          if (editedMessage) {
            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
              this.chatwootService.eventWhatsapp(
                'messages.edit',
                { instanceName: this.instance.name, instanceId: this.instance.id },
                editedMessage,
              );

            await this.sendDataWebhook(Events.MESSAGES_EDITED, editedMessage);

            if (received.key?.id && editedMessage.key?.id) {
              await this.baileysCache.set(`protocol_${received.key.id}`, editedMessage.key.id, 60 * 60 * 24);
            }

            const oldMessage = await this.getMessage(editedMessage.key, true);
            if ((oldMessage as any)?.id) {
              const editedMessageTimestamp = Long.isLong(received?.messageTimestamp)
                ? Math.floor(received?.messageTimestamp.toNumber())
                : Math.floor(received?.messageTimestamp as number);

              await this.prismaRepository.message.update({
                where: { id: (oldMessage as any).id },
                data: {
                  message: editedMessage.editedMessage as any,
                  messageTimestamp: editedMessageTimestamp,
                  status: 'EDITED',
                },
              });
              await this.prismaRepository.messageUpdate.create({
                data: {
                  fromMe: editedMessage.key.fromMe,
                  keyId: editedMessage.key.id,
                  remoteJid: editedMessage.key.remoteJid,
                  status: 'EDITED',
                  instanceId: this.instanceId,
                  messageId: (oldMessage as any).id,
                },
              });
            }
          }

          if ((type !== 'notify' && type !== 'append') || editedMessage || !received?.message) {
            continue;
          }

          if (Long.isLong(received.messageTimestamp)) {
            received.messageTimestamp = received.messageTimestamp?.toNumber();
          }

          if (settings?.groupsIgnore && received.key.remoteJid.includes('@g.us')) {
            continue;
          }

          const messageRaw = (await this.prepareMessageWithNative(received)) as any;
          const canonicalRemoteJid = (messageRaw.key as any).remoteJid;
          const chatDisplayName =
            received.key.fromMe || canonicalRemoteJid.includes('@g.us')
              ? undefined
              : this.pickPreferredName(received.pushName);

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: canonicalRemoteJid },
            select: { id: true, name: true },
          });

          if (
            existingChat &&
            chatDisplayName &&
            existingChat.name !== chatDisplayName &&
            !received.key.fromMe &&
            !canonicalRemoteJid.includes('@g.us')
          ) {
            this.sendDataWebhook(Events.CHATS_UPSERT, [
              { ...existingChat, remoteJid: canonicalRemoteJid, name: chatDisplayName },
            ]);
          }

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
            await this.reconcileIdentityAliases(messageRaw.key as any, {
              chat: { name: chatDisplayName, unreadMessages: 0 },
              syncMessages: false,
            });
          }

          if (messageRaw.messageType === 'pollUpdateMessage') {
            const pollCreationKey = (messageRaw.message as any).pollUpdateMessage.pollCreationMessageKey;
            const pollMessage = (await this.getMessage(pollCreationKey, true)) as proto.IWebMessageInfo;
            const pollMessageSecret = (await this.getMessage(pollCreationKey)) as any;

            if (pollMessage) {
              const pollOptions =
                (pollMessage.message as any).pollCreationMessage?.options ||
                (pollMessage.message as any).pollCreationMessageV3?.options ||
                [];
              const pollVote = (messageRaw.message as any).pollUpdateMessage.vote;

              const voterJid = received.key.fromMe
                ? this.instance.wuid
                : received.key.participant || received.key.remoteJid;

              let pollEncKey = pollMessageSecret?.messageContextInfo?.messageSecret;

              let successfulVoterJid = voterJid;

              if (typeof pollEncKey === 'string') {
                pollEncKey = Buffer.from(pollEncKey, 'base64');
              } else if (pollEncKey?.type === 'Buffer' && Array.isArray(pollEncKey.data)) {
                pollEncKey = Buffer.from(pollEncKey.data);
              }

              if (Buffer.isBuffer(pollEncKey) && pollEncKey.length === 44) {
                pollEncKey = Buffer.from(pollEncKey.toString('utf8'), 'base64');
              }

              if (pollVote.encPayload && pollEncKey) {
                const creatorCandidates = [
                  this.instance.wuid,
                  this.client.user?.lid,
                  pollMessage.key.participant,
                  (pollMessage.key as any).participantAlt,
                  pollMessage.key.remoteJid,
                ];

                const key = received.key as any;
                const voterCandidates = [
                  this.instance.wuid,
                  this.client.user?.lid,
                  key.participant,
                  key.participantAlt,
                  key.remoteJidAlt,
                  key.remoteJid,
                ];

                const uniqueCreators = [
                  ...new Set(creatorCandidates.filter(Boolean).map((id) => jidNormalizedUser(id))),
                ];
                const uniqueVoters = [...new Set(voterCandidates.filter(Boolean).map((id) => jidNormalizedUser(id)))];

                let decryptedVote;

                for (const creator of uniqueCreators) {
                  for (const voter of uniqueVoters) {
                    try {
                      decryptedVote = decryptPollVote(pollVote, {
                        pollCreatorJid: creator,
                        pollMsgId: pollMessage.key.id,
                        pollEncKey,
                        voterJid: voter,
                      } as any);
                      if (decryptedVote) {
                        successfulVoterJid = voter;
                        break;
                      }
                    } catch {
                      // Continue trying
                    }
                  }
                  if (decryptedVote) break;
                }

                if (decryptedVote) {
                  Object.assign(pollVote, decryptedVote);
                }
              }

              const selectedOptions = pollVote?.selectedOptions || [];

              const selectedOptionNames = pollOptions
                .filter((option) => {
                  const hash = createHash('sha256').update(option.optionName).digest();
                  return selectedOptions.some((selected) => Buffer.compare(selected, hash) === 0);
                })
                .map((option) => option.optionName);

              (messageRaw.message as any).pollUpdateMessage.vote.selectedOptions = selectedOptionNames;

              const pollUpdates = pollOptions.map((option) => ({
                name: option.optionName,
                voters: selectedOptionNames.includes(option.optionName) ? [successfulVoterJid] : [],
              }));

              (messageRaw as any).pollUpdates = pollUpdates;
            }
          }

          const isMedia =
            received?.message?.imageMessage ||
            received?.message?.videoMessage ||
            received?.message?.stickerMessage ||
            received?.message?.documentMessage ||
            received?.message?.documentWithCaptionMessage ||
            received?.message?.ptvMessage ||
            received?.message?.audioMessage;

          const isVideo = received?.message?.videoMessage;

          if (this.localSettings.readMessages && received.key.id !== 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }

          if (this.localSettings.readStatus && received.key.id === 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }

          if (
            this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
            this.localChatwoot?.enabled &&
            !received.key.id.includes('@broadcast')
          ) {
            const chatwootSentMessage = await this.chatwootService.eventWhatsapp(
              Events.MESSAGES_UPSERT,
              { instanceName: this.instance.name, instanceId: this.instanceId },
              messageRaw,
            );

            if (chatwootSentMessage?.id) {
              messageRaw.chatwootMessageId = chatwootSentMessage.id;
              messageRaw.chatwootInboxId = chatwootSentMessage.inbox_id;
              messageRaw.chatwootConversationId = chatwootSentMessage.conversation_id;
            }
          }

          if (this.configService.get<Openai>('OPENAI').ENABLED && received?.message?.audioMessage) {
            const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({
              where: { instanceId: this.instanceId },
              include: { OpenaiCreds: true },
            });

            if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
              (messageRaw.message as any).speechToText =
                `[audio] ${await this.openaiService.speechToText(received, this)}`;
            }
          }

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { pollUpdates, ...messageData } = messageRaw as any;
            const msg = await this.prismaRepository.message.create({ data: messageData });

            const remoteJid = canonicalRemoteJid;
            const timestamp = msg.messageTimestamp;
            const fromMe = received.key.fromMe.toString();
            const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;

            const cachedTimestamp = await this.baileysCache.get(messageKey);

            if (!cachedTimestamp) {
              if (!received.key.fromMe) {
                if (msg.status === status[3]) {
                  this.logger.log(`Update not read messages ${remoteJid}`);
                  await this.updateChatUnreadMessages(remoteJid);
                } else if (msg.status === status[4]) {
                  this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                  await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
                }
              } else {
                // is send message by me
                this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
              }

              await this.baileysCache.set(messageKey, true, this.MESSAGE_CACHE_TTL_SECONDS);
            } else {
              this.logger.info(`Update readed messages duplicated ignored [avoid deadlock]: ${messageKey}`);
            }

            if (isMedia) {
              if (this.configService.get<S3>('S3').ENABLE) {
                try {
                  if (isVideo && !this.configService.get<S3>('S3').SAVE_VIDEO) {
                    this.logger.warn('Video upload is disabled. Skipping video upload.');
                    // Skip video upload by returning early from this block
                    return;
                  }

                  const message: any = received;

                  // Verificação adicional para garantir que há conteúdo de mídia real
                  const hasRealMedia = this.hasValidMediaContent(message);

                  if (!hasRealMedia) {
                    this.logger.warn('Message detected as media but contains no valid media content');
                  } else {
                    const media = await this.getBase64FromMediaMessage({ message }, true);

                    if (!media) {
                      this.logger.verbose('No valid media to upload (messageContextInfo only), skipping MinIO');
                      return;
                    }

                    const { buffer, mediaType, fileName, size } = media;
                    const mimetype = mimeTypes.lookup(fileName).toString();
                    const fullName = join(
                      `${this.instance.id}`,
                      canonicalRemoteJid,
                      mediaType,
                      `${Date.now()}_${fileName}`,
                    );
                    await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });

                    const mediaUrl = await s3Service.getObjectUrl(fullName);

                    await this.prismaRepository.media.create({
                      data: {
                        messageId: msg.id,
                        instanceId: this.instanceId,
                        type: mediaType,
                        fileName: fullName,
                        mimetype,
                        fileUrl: mediaUrl,
                      },
                    });

                    (messageRaw.message as any).mediaUrl = mediaUrl;

                    await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
                  }
                } catch (error) {
                  this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
                }
              }
            }
          }

          if (this.localWebhook.enabled) {
            if (isMedia && this.localWebhook.webhookBase64) {
              try {
                const buffer = await downloadMediaMessage(
                  { key: received.key, message: received?.message },
                  'buffer',
                  {},
                  { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                );

                if (buffer) {
                  (messageRaw.message as any).base64 = buffer.toString('base64');
                } else {
                  // retry to download media
                  const buffer = await downloadMediaMessage(
                    { key: received.key, message: received?.message },
                    'buffer',
                    {},
                    { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                  );

                  if (buffer) {
                    (messageRaw.message as any).base64 = buffer.toString('base64');
                  }
                }
              } catch (error) {
                this.logger.error(['Error converting media to base64', error?.message]);
              }
            }
          }

          this.logger.verbose(messageRaw);

          sendTelemetry(`received.message.${messageRaw.messageType ?? 'unknown'}`);

          this.sendDataWebhook(Events.MESSAGES_UPSERT, messageRaw);

          await chatbotController.emit({
            instance: { instanceName: this.instance.name, instanceId: this.instanceId },
            remoteJid: canonicalRemoteJid,
            msg: messageRaw,
            pushName: messageRaw.pushName,
          });

          const contactIdentity = await this.resolveMessageContactIdentity(received);
          const contactCandidates = this.buildIdentityCandidates(
            contactIdentity,
            received.key.remoteJid,
            received.key.participant,
            (received.key as any).participantAlt,
          );
          const contactRemoteJid = contactIdentity.remoteJid;
          if (!contactRemoteJid || contactRemoteJid === 'status@broadcast') {
            continue;
          }

          const contact = await this.findBestContactByJids(contactCandidates);
          const contactRaw: any = {
            remoteJid: contactRemoteJid,
            canonicalJid: contactRemoteJid,
            rawRemoteJid:
              contactIdentity.rawRemoteJid && contactIdentity.rawRemoteJid !== contactRemoteJid
                ? contactIdentity.rawRemoteJid
                : undefined,
            remoteJidAlt: contactIdentity.remoteJidAlt,
            remoteLid: contactIdentity.remoteLid,
            pushName: received.key.fromMe
              ? undefined
              : this.pickPreferredName(received.pushName, contact?.pushName, contactRemoteJid.split('@')[0]),
            profilePicUrl:
              (await this.resolveProfilePictureUrlForIdentity(contactIdentity, received.key.remoteJid)) ??
              contact?.profilePicUrl ??
              undefined,
            instanceId: this.instanceId,
          };

          if (contactRaw.remoteJid.includes('@s.whatsapp') || contactRaw.remoteJid.includes('@lid')) {
            const cachePayload = this.cachePayloadForJid(contactIdentity);
            if (cachePayload) {
              await saveOnWhatsappCache([cachePayload]);
            }
          }

          if (contact) {
            this.sendDataWebhook(Events.CONTACTS_UPDATE, contactRaw);

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              await this.chatwootService.eventWhatsapp(
                Events.CONTACTS_UPDATE,
                { instanceName: this.instance.name, instanceId: this.instanceId },
                contactRaw,
              );
            }

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS) {
              await this.reconcileIdentityAliases(contactRaw, {
                contact: { pushName: contactRaw.pushName, profilePicUrl: contactRaw.profilePicUrl },
                chat: { name: chatDisplayName, unreadMessages: 0 },
              });
            }

            continue;
          }

          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS) {
            await this.reconcileIdentityAliases(contactRaw, {
              contact: { pushName: contactRaw.pushName, profilePicUrl: contactRaw.profilePicUrl },
              chat: { name: chatDisplayName, unreadMessages: 0 },
            });
          }
        }
      } catch (error) {
        this.logger.error(error);
      }
    },

    'messages.update': async (args: { update: Partial<WAMessage>; key: WAMessageKey }[], settings: any) => {
      this.logger.verbose({ local: 'messages.update', args });

      const readChatToUpdate: Record<string, true> = {}; // {remoteJid: true}

      for await (const { key, update } of args) {
        const keyAny = key as any;
        await this.applyCanonicalKeyIdentityWithNative(keyAny);

        const normalizedRemoteJid = keyAny.remoteJid;
        const normalizedParticipant = keyAny.participant;

        if (settings?.groupsIgnore && normalizedRemoteJid?.includes('@g.us')) {
          continue;
        }

        const updateKey = `${this.instance.id}_${key.id}_${update.status}`;

        const cached = await this.baileysCache.get(updateKey);

        const secondsSinceEpoch = Math.floor(Date.now() / 1000);
        this.logger.debug({
          local: 'messages.update.cache',
          cached,
          updateKey,
          messageTimestamp: update.messageTimestamp,
          secondsSinceEpoch,
        });

        if (
          (update.messageTimestamp && update.messageTimestamp === cached) ||
          (!update.messageTimestamp && secondsSinceEpoch === cached)
        ) {
          this.logger.info(`Update Message duplicated ignored [avoid deadlock]: ${updateKey}`);
          continue;
        }

        if (update.messageTimestamp) {
          await this.baileysCache.set(updateKey, update.messageTimestamp, 30 * 60);
        } else {
          await this.baileysCache.set(updateKey, secondsSinceEpoch, 30 * 60);
        }

        if (status[update.status] === 'READ' && key.fromMe) {
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
            this.chatwootService.eventWhatsapp(
              'messages.read',
              { instanceName: this.instance.name, instanceId: this.instanceId },
              { key: key },
            );
          }
        }

        if (key.remoteJid !== 'status@broadcast' && key.id !== undefined) {
          let pollUpdates: any;

          if (update.pollUpdates) {
            const pollCreation = await this.getMessage(key);

            if (pollCreation) {
              pollUpdates = getAggregateVotesInPollMessage({
                message: pollCreation as proto.IMessage,
                pollUpdates: update.pollUpdates,
              });
            }
          }

          const message: any = {
            keyId: key.id,
            remoteJid: normalizedRemoteJid,
            fromMe: key.fromMe,
            participant: normalizedParticipant,
            status: status[update.status] ?? 'SERVER_ACK',
            pollUpdates,
            instanceId: this.instanceId,
          };

          if (update.message) {
            message.message = update.message;
          }

          let findMessage: any;
          const configDatabaseData = this.configService.get<Database>('DATABASE').SAVE_DATA;
          if (configDatabaseData.HISTORIC || configDatabaseData.NEW_MESSAGE) {
            const protocolMapKey = `protocol_${key.id}`;
            const originalMessageId = (await this.baileysCache.get(protocolMapKey)) as string;

            if (originalMessageId) {
              message.keyId = originalMessageId;
            }

            const searchId = originalMessageId || key.id;
            findMessage = await this.findStoredMessageByKey(key as ExtendedIMessageKey, { searchId });

            if (!findMessage?.id) {
              this.logger.verbose({
                local: 'messages.update.original-not-found',
                key,
              });
              continue;
            }

            // Sync the incoming key.remoteJid with the stored one.
            // This mutation is safe and necessary because Baileys events might use LIDs while we store Phone JIDs (or vice versa).
            // Normalizing ensuring downstream logic uses the identifier that exists in our database.
            if (findMessage?.key?.remoteJid && key.remoteJid !== findMessage.key.remoteJid) {
              key.remoteJid = findMessage.key.remoteJid;
            }
            if (findMessage?.key?.remoteJid && findMessage.key.remoteJid !== key.remoteJid) {
              this.logger.verbose(
                `Updating key.remoteJid from ${key.remoteJid} to ${findMessage.key.remoteJid} based on stored message`,
              );
              key.remoteJid = findMessage.key.remoteJid;
            }
            message.messageId = findMessage.id;
          }

          if (update.message === null && update.status === undefined) {
            this.sendDataWebhook(Events.MESSAGES_DELETE, { ...key, status: 'DELETED' });

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE)
              await this.prismaRepository.messageUpdate.create({ data: message });

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              this.chatwootService.eventWhatsapp(
                Events.MESSAGES_DELETE,
                { instanceName: this.instance.name, instanceId: this.instanceId },
                { key: key },
              );
            }

            continue;
          }

          if (findMessage && update.status !== undefined && status[update.status] !== findMessage.status) {
            if (!key.fromMe && key.remoteJid) {
              readChatToUpdate[key.remoteJid] = true;

              const { remoteJid } = key;
              const timestamp = findMessage.messageTimestamp;
              const fromMe = key.fromMe.toString();
              const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;

              const cachedTimestamp = await this.baileysCache.get(messageKey);

              if (!cachedTimestamp) {
                if (status[update.status] === status[4]) {
                  this.logger.log(`Update as read in message.update ${remoteJid} - ${timestamp}`);
                  await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
                  await this.baileysCache.set(messageKey, true, this.MESSAGE_CACHE_TTL_SECONDS);
                }

                await this.prismaRepository.message.update({
                  where: { id: findMessage.id },
                  data: { status: status[update.status] },
                });
              } else {
                this.logger.info(
                  `Update readed messages duplicated ignored in message.update [avoid deadlock]: ${messageKey}`,
                );
              }
            }
          }

          this.sendDataWebhook(Events.MESSAGES_UPDATE, message);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { message: _msg, ...messageData } = message;
            await this.prismaRepository.messageUpdate.create({ data: messageData });
          }

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: message.remoteJid },
          });

          if (existingChat) {
            this.sendDataWebhook(Events.CHATS_UPSERT, [
              { remoteJid: message.remoteJid, instanceId: this.instanceId, unreadMessages: 0 },
            ]);
          }

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
            await this.reconcileIdentityAliases(
              {
                remoteJid: message.remoteJid,
                remoteJidAlt: keyAny.remoteJidAlt,
                remoteLid: keyAny.remoteLid,
              },
              {
                chat: { unreadMessages: 0 },
                syncMessages: false,
              },
            );
          }
        }
      }

      await Promise.all(Object.keys(readChatToUpdate).map((remoteJid) => this.updateChatUnreadMessages(remoteJid)));
    },
  };

  private readonly groupHandler = {
    'groups.upsert': (groupMetadata: GroupMetadata[]) => {
      this.sendDataWebhook(Events.GROUPS_UPSERT, groupMetadata);
    },

    'groups.update': (groupMetadataUpdate: Partial<GroupMetadata>[]) => {
      this.sendDataWebhook(Events.GROUPS_UPDATE, groupMetadataUpdate);

      groupMetadataUpdate.forEach((group) => {
        if (isJidGroup(group.id)) {
          this.updateGroupMetadataCache(group.id);
        }
      });
    },

    'group-participants.update': async (participantsUpdate: {
      id: string;
      participants: string[];
      action: ParticipantAction;
    }) => {
      // ENHANCEMENT: Adds participantsData field while maintaining backward compatibility
      // MAINTAINS: participants: string[] (original JID strings)
      // ADDS: participantsData: { jid: string, phoneNumber: string, name?: string, imgUrl?: string }[]
      // This enables LID to phoneNumber conversion without breaking existing webhook consumers

      // Helper to normalize participantId as phone number
      const normalizePhoneNumber = (id: string | null | undefined): string => {
        // Remove @lid, @s.whatsapp.net suffixes and extract just the number part
        return String(id || '').split('@')[0];
      };

      try {
        // Usa o mesmo método que o endpoint /group/participants
        const groupParticipants = await this.findParticipants({ groupJid: participantsUpdate.id });

        // Validação para garantir que temos dados válidos
        if (!groupParticipants?.participants || !Array.isArray(groupParticipants.participants)) {
          throw new Error('Invalid participant data received from findParticipants');
        }

        // Filtra apenas os participantes que estão no evento
        const resolvedParticipants = participantsUpdate.participants.map((participantId) => {
          const participantData = groupParticipants.participants.find((p) => p.id === participantId);

          let phoneNumber: string;
          if (participantData?.phoneNumber) {
            phoneNumber = participantData.phoneNumber;
          } else {
            phoneNumber = normalizePhoneNumber(participantId);
          }

          return {
            jid: participantId,
            phoneNumber,
            name: participantData?.name,
            imgUrl: participantData?.imgUrl,
          };
        });

        // Mantém formato original + adiciona dados resolvidos
        const enhancedParticipantsUpdate = {
          ...participantsUpdate,
          participants: participantsUpdate.participants, // Mantém array original de strings
          // Adiciona dados resolvidos em campo separado
          participantsData: resolvedParticipants,
        };

        this.sendDataWebhook(Events.GROUP_PARTICIPANTS_UPDATE, enhancedParticipantsUpdate);
      } catch (error) {
        this.logger.error(
          `Failed to resolve participant data for GROUP_PARTICIPANTS_UPDATE webhook: ${error.message} | Group: ${participantsUpdate.id} | Participants: ${participantsUpdate.participants.length}`,
        );
        // Fallback - envia sem conversão
        this.sendDataWebhook(Events.GROUP_PARTICIPANTS_UPDATE, participantsUpdate);
      }

      this.updateGroupMetadataCache(participantsUpdate.id);
    },
  };

  private readonly labelHandle = {
    [Events.LABELS_EDIT]: async (label: Label) => {
      this.sendDataWebhook(Events.LABELS_EDIT, { ...label, instance: this.instance.name });

      const labelsRepository = await this.prismaRepository.label.findMany({ where: { instanceId: this.instanceId } });

      const savedLabel = labelsRepository.find((l) => l.labelId === label.id);
      if (label.deleted && savedLabel) {
        await this.prismaRepository.label.delete({
          where: { labelId_instanceId: { instanceId: this.instanceId, labelId: label.id } },
        });
        this.sendDataWebhook(Events.LABELS_EDIT, { ...label, instance: this.instance.name });
        return;
      }

      const labelName = label.name.replace(/[^\x20-\x7E]/g, '');
      if (!savedLabel || savedLabel.color !== `${label.color}` || savedLabel.name !== labelName) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.LABELS) {
          const labelData = {
            color: `${label.color}`,
            name: labelName,
            labelId: label.id,
            predefinedId: label.predefinedId,
            instanceId: this.instanceId,
          };
          await this.prismaRepository.label.upsert({
            where: { labelId_instanceId: { instanceId: labelData.instanceId, labelId: labelData.labelId } },
            update: labelData,
            create: labelData,
          });
        }
      }
    },

    [Events.LABELS_ASSOCIATION]: async (
      data: { association: LabelAssociation; type: 'remove' | 'add' },
      database: Database,
    ) => {
      this.logger.info(
        `labels association - ${data?.association?.chatId} (${data.type}-${data?.association?.type}): ${data?.association?.labelId}`,
      );
      if (database.SAVE_DATA.CHATS) {
        const instanceId = this.instanceId;
        const chatId = data.association.chatId;
        const labelId = data.association.labelId;

        if (data.type === 'add') {
          await this.addLabel(labelId, instanceId, chatId);
        } else if (data.type === 'remove') {
          await this.removeLabel(labelId, instanceId, chatId);
        }
      }

      this.sendDataWebhook(Events.LABELS_ASSOCIATION, {
        instance: this.instance.name,
        type: data.type,
        chatId: data.association.chatId,
        labelId: data.association.labelId,
      });
    },
  };

  private eventHandler() {
    const eventClient = this.client;
    const eventAuthState = this.instance.authState;
    eventClient.ev.process(async (events) => {
      this.eventProcessingQueue = this.eventProcessingQueue.then(async () => {
        if (eventClient !== this.client) {
          return;
        }

        try {
          if (!this.endSession) {
            const database = this.configService.get<Database>('DATABASE');
            const settings = await this.findSettings();
            if (eventClient !== this.client) {
              return;
            }

            if (events.call) {
              const call = events.call[0];

              if (settings?.rejectCall && call.status == 'offer') {
                eventClient.rejectCall(call.id, call.from);
              }

              if (settings?.msgCall?.trim().length > 0 && call.status == 'offer') {
                if (call.from.endsWith('@lid')) {
                  call.from = await eventClient.signalRepository.lidMapping.getPNForLID(call.from as string);
                }
                const msg = await eventClient.sendMessage(call.from, { text: settings.msgCall });

                eventClient.ev.emit('messages.upsert', { messages: [msg], type: 'notify' });
              }

              this.sendDataWebhook(Events.CALL, call);
            }

            if (events['connection.update']) {
              await this.connectionUpdate(events['connection.update']);
              if (eventClient !== this.client) {
                return;
              }
            }

            if (events['creds.update']) {
              Object.assign(eventAuthState.state.creds, events['creds.update']);
              await eventAuthState.saveCreds();
              if (eventClient !== this.client) {
                return;
              }
              this.persistedAuthRegistered = Boolean(eventAuthState.state.creds.registered);
              this.persistedAuthCheckedAt = Date.now();
            }

            if (events['messaging-history.set']) {
              const payload = events['messaging-history.set'];
              await this.messageHandle['messaging-history.set'](payload);
            }

            if (events['messages.upsert']) {
              const payload = events['messages.upsert'];

              // this.messageProcessor.processMessage(payload, settings);
              await this.messageHandle['messages.upsert'](payload, settings);
            }

            if (events['messages.update']) {
              const payload = events['messages.update'];
              await this.messageHandle['messages.update'](payload, settings);
            }

            if (events['message-receipt.update']) {
              const payload = events['message-receipt.update'] as MessageUserReceiptUpdate[];
              const remotesJidMap: Record<string, number> = {};

              for (const event of payload) {
                const keyAny = event.key as ExtendedIMessageKey;
                await this.applyCanonicalKeyIdentityWithNative(keyAny);

                if (typeof keyAny.remoteJid === 'string' && typeof event.receipt.readTimestamp === 'number') {
                  remotesJidMap[keyAny.remoteJid] = event.receipt.readTimestamp;
                }
              }

              await Promise.all(
                Object.keys(remotesJidMap).map(async (remoteJid) =>
                  this.updateMessagesReadedByTimestamp(remoteJid, remotesJidMap[remoteJid]),
                ),
              );
            }

            if (events['lid-mapping.update']) {
              const rawPayload = events['lid-mapping.update'] as
                | { lid?: string | null; pn?: string | null }
                | Array<{ lid?: string | null; pn?: string | null }>;
              const payload = Array.isArray(rawPayload) ? rawPayload : [rawPayload];
              const normalizedMappings = await this.ingestIdentityMappings(payload, {
                reconcileDatabase: true,
                syncMessages: true,
              });
              await this.emitContactUpdatesForIdentityMappings(normalizedMappings);
            }

            if (events['presence.update']) {
              const payload = events['presence.update'];

              if (settings?.groupsIgnore && payload.id.includes('@g.us')) {
                return;
              }

              this.sendDataWebhook(Events.PRESENCE_UPDATE, payload);
            }

            if (!settings?.groupsIgnore) {
              if (events['groups.upsert']) {
                const payload = events['groups.upsert'];
                this.groupHandler['groups.upsert'](payload);
              }

              if (events['groups.update']) {
                const payload = events['groups.update'];
                this.groupHandler['groups.update'](payload);
              }

              if (events['group-participants.update']) {
                const payload = events['group-participants.update'] as any;
                await this.groupHandler['group-participants.update'](payload);
              }
            }

            if (events['chats.upsert']) {
              const payload = events['chats.upsert'];
              await this.chatHandle['chats.upsert'](payload);
            }

            if (events['chats.update']) {
              const payload = events['chats.update'];
              await this.chatHandle['chats.update'](payload);
            }

            if (events['chats.delete']) {
              const payload = events['chats.delete'];
              await this.chatHandle['chats.delete'](payload);
            }

            if (events['contacts.upsert']) {
              const payload = events['contacts.upsert'];
              await this.contactHandle['contacts.upsert'](payload);
            }

            if (events['contacts.update']) {
              const payload = events['contacts.update'];
              await this.contactHandle['contacts.update'](payload);
            }

            if (events[Events.LABELS_ASSOCIATION]) {
              const payload = events[Events.LABELS_ASSOCIATION];
              await this.labelHandle[Events.LABELS_ASSOCIATION](payload, database);
              return;
            }

            if (events[Events.LABELS_EDIT]) {
              const payload = events[Events.LABELS_EDIT];
              await this.labelHandle[Events.LABELS_EDIT](payload);
              return;
            }
          }
        } catch (error) {
          this.logger.error(error);
        }
      });
    });
  }

  private historySyncNotification(msg: proto.Message.IHistorySyncNotification) {
    const instance: InstanceDto = { instanceName: this.instance.name };

    if (
      this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
      this.localChatwoot?.enabled &&
      this.localChatwoot.importMessages &&
      this.isSyncNotificationFromUsedSyncType(msg)
    ) {
      if (msg.chunkOrder === 1) {
        this.chatwootService.startImportHistoryMessages(instance);
      }

      if (msg.progress === 100) {
        setTimeout(() => {
          this.chatwootService.importHistoryMessages(instance);
        }, 10000);
      }
    }

    return true;
  }

  private isSyncNotificationFromUsedSyncType(msg: proto.Message.IHistorySyncNotification) {
    return (
      (this.localSettings.syncFullHistory && msg?.syncType === 2) ||
      (!this.localSettings.syncFullHistory && msg?.syncType === 3)
    );
  }

  public async profilePicture(number: string) {
    const jid = createJid(number);

    try {
      const profilePictureUrl = await this.client.profilePictureUrl(jid, 'image');

      return { wuid: jid, profilePictureUrl };
    } catch {
      return { wuid: jid, profilePictureUrl: null };
    }
  }

  public async getStatus(number: string) {
    const jid = createJid(number);

    try {
      return { wuid: jid, status: (await this.client.fetchStatus(jid))[0]?.status };
    } catch {
      return { wuid: jid, status: null };
    }
  }

  public async fetchProfile(instanceName: string, number?: string) {
    const jid = number ? createJid(number) : this.client?.user?.id;

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      if (number) {
        const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
        const picture = await this.profilePicture(info?.jid);
        const status = await this.getStatus(info?.jid);
        const business = await this.fetchBusinessProfile(info?.jid);

        return {
          wuid: info?.jid || jid,
          name: info?.name,
          numberExists: info?.exists,
          picture: picture?.profilePictureUrl,
          status: status?.status,
          isBusiness: business.isBusiness,
          email: business?.email,
          description: business?.description,
          website: business?.website?.shift(),
        };
      } else {
        const instanceNames = instanceName ? [instanceName] : null;
        const info: Instance = await waMonitor.instanceInfo(instanceNames);
        const business = await this.fetchBusinessProfile(jid);

        return {
          wuid: jid,
          name: info?.profileName,
          numberExists: true,
          picture: info?.profilePicUrl,
          status: info?.connectionStatus,
          isBusiness: business.isBusiness,
          email: business?.email,
          description: business?.description,
          website: business?.website?.shift(),
        };
      }
    } catch {
      return { wuid: jid, name: null, picture: null, status: null, os: null, isBusiness: false };
    }
  }

  public async offerCall({ number, isVideo, callDuration }: OfferCallDto) {
    const jid = createJid(number);

    try {
      // const call = await this.client.offerCall(jid, isVideo);
      // setTimeout(() => this.client.terminateCall(call.id, call.to), callDuration * 1000);

      // return call;
      return { id: '123', jid, isVideo, callDuration };
    } catch (error) {
      return error;
    }
  }
  public generateMessageID() {
    return {
      id: generateMessageIDV2(this.client.user?.id),
    };
  }

  private async generateLinkPreview(text: string) {
    try {
      const linkRegex = /https?:\/\/[^\s]+/;
      const match = text.match(linkRegex);

      if (!match) return undefined;

      // Trim common trailing punctuation that may follow URLs in natural text
      const url = match[0].replace(/[.,);\]]+$/u, '');
      if (!url) return undefined;

      const previewData = (await getLinkPreview(url, {
        imagesPropertyType: 'og', // fetches only open-graph images
        headers: {
          'user-agent': 'googlebot', // fetches with googlebot to prevent login pages
        },
      })) as any;

      if (!previewData || !previewData.title) return undefined;

      const image = previewData.images && previewData.images.length > 0 ? previewData.images[0] : undefined;

      return {
        externalAdReply: {
          title: previewData.title,
          body: previewData.description,
          mediaType: 2, // 2 for video/image preview, though usually 1 is for thumbnail
          thumbnailUrl: image,
          sourceUrl: url,
          mediaUrl: url,
          renderLargerThumbnail: true,
          // showAdAttribution: true // Removed to prevent "Sent via ad" label
        },
      };
    } catch (error) {
      this.logger.debug({ local: 'linkPreview', error });
      return undefined;
    }
  }

  private async sendMessage(
    sender: string,
    message: any,
    mentions: any,
    linkPreview: any,
    quoted: any,
    messageId?: string,
    ephemeralExpiration?: number,
    contextInfo?: any,
    // participants?: GroupParticipant[],
  ) {
    sender = sender.toLowerCase();

    const option: any = { quoted };

    if (isJidGroup(sender)) {
      option.useCachedGroupMetadata = true;
      // if (participants)
      //   option.cachedGroupMetadata = async () => {
      //     return { participants: participants as GroupParticipant[] };
      //   };
    }

    if (ephemeralExpiration) option.ephemeralExpiration = ephemeralExpiration;

    // NOTE: NÃO DEVEMOS GERAR O messageId AQUI, SOMENTE SE VIER INFORMADO POR PARAMETRO. A GERAÇÃO ANTERIOR IMPEDE O WZAP DE IDENTIFICAR A SOURCE.
    if (messageId) option.messageId = messageId;

    if (message['viewOnceMessage']) {
      const m = generateWAMessageFromContent(sender, message, {
        timestamp: new Date(),
        userJid: this.instance.wuid,
        messageId,
        quoted,
      });
      const id = await this.client.relayMessage(sender, message, { messageId });
      m.key = { id: id, remoteJid: sender, participant: isPnUser(sender) ? sender : undefined, fromMe: true };
      for (const [key, value] of Object.entries(m)) {
        if (!value || (isArray(value) && value.length) === 0) {
          delete m[key];
        }
      }
      return m;
    }

    if (
      !message['audio'] &&
      !message['poll'] &&
      !message['sticker'] &&
      !message['conversation'] &&
      sender !== 'status@broadcast'
    ) {
      if (message['reactionMessage']) {
        return await this.client.sendMessage(
          sender,
          {
            react: { text: message['reactionMessage']['text'], key: message['reactionMessage']['key'] },
          } as unknown as AnyMessageContent,
          option as unknown as MiscMessageGenerationOptions,
        );
      }
    }

    if (contextInfo) {
      message['contextInfo'] = contextInfo;
    }

    if (message['conversation']) {
      return await this.client.sendMessage(
        sender,
        {
          text: message['conversation'],
          mentions,
          linkPreview: linkPreview,
          contextInfo: message['contextInfo'],
        } as unknown as AnyMessageContent,
        option as unknown as MiscMessageGenerationOptions,
      );
    }

    if (!message['audio'] && !message['poll'] && !message['sticker'] && sender != 'status@broadcast') {
      return await this.client.sendMessage(
        sender,
        {
          forward: { key: { remoteJid: this.instance.wuid, fromMe: true }, message },
          mentions,
          contextInfo: message['contextInfo'],
        },
        option as unknown as MiscMessageGenerationOptions,
      );
    }

    if (sender === 'status@broadcast') {
      let jidList;
      if (message['status'].option.allContacts) {
        const contacts = await this.prismaRepository.contact.findMany({
          where: { instanceId: this.instanceId, remoteJid: { not: { endsWith: '@g.us' } } },
        });

        jidList = contacts.map((contact) => contact.remoteJid);
      } else {
        jidList = message['status'].option.statusJidList;
      }

      const batchSize = 10;

      const batches = Array.from({ length: Math.ceil(jidList.length / batchSize) }, (_, i) =>
        jidList.slice(i * batchSize, i * batchSize + batchSize),
      );

      let msgId: string | null = null;

      let firstMessage: WAMessage;

      const firstBatch = batches.shift();

      if (firstBatch) {
        firstMessage = await this.client.sendMessage(
          sender,
          message['status'].content as unknown as AnyMessageContent,
          {
            backgroundColor: message['status'].option.backgroundColor,
            font: message['status'].option.font,
            statusJidList: firstBatch,
          } as unknown as MiscMessageGenerationOptions,
        );

        msgId = firstMessage.key.id;
      }

      if (batches.length === 0) return firstMessage;

      await Promise.allSettled(
        batches.map(async (batch) => {
          const messageSent = await this.client.sendMessage(
            sender,
            message['status'].content as unknown as AnyMessageContent,
            {
              backgroundColor: message['status'].option.backgroundColor,
              font: message['status'].option.font,
              statusJidList: batch,
              messageId: msgId,
            } as unknown as MiscMessageGenerationOptions,
          );

          return messageSent;
        }),
      );

      return firstMessage;
    }

    return await this.client.sendMessage(
      sender,
      message as unknown as AnyMessageContent,
      option as unknown as MiscMessageGenerationOptions,
    );
  }

  private async sendMessageWithTyping<T = proto.IMessage>(
    number: string,
    message: T,
    options?: Options,
    isIntegration = false,
  ) {
    const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

    if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
      throw new BadRequestException(isWA);
    }

    const sender = isWA.jid.toLowerCase();

    this.logger.verbose(`Sending message to ${sender}`);

    try {
      if (options?.delay) {
        this.logger.verbose(`Typing for ${options.delay}ms to ${sender}`);
        if (options.delay > 20000) {
          let remainingDelay = options.delay;
          while (remainingDelay > 20000) {
            await this.client.presenceSubscribe(sender);

            await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

            await delay(20000);

            await this.client.sendPresenceUpdate('paused', sender);

            remainingDelay -= 20000;
          }
          if (remainingDelay > 0) {
            await this.client.presenceSubscribe(sender);

            await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

            await delay(remainingDelay);

            await this.client.sendPresenceUpdate('paused', sender);
          }
        } else {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

          await delay(options.delay);

          await this.client.sendPresenceUpdate('paused', sender);
        }
      }

      const linkPreview = options?.linkPreview === false ? false : undefined;

      let previewContext: any = undefined;
      if (linkPreview !== false && (message as any)?.conversation) {
        previewContext = await this.generateLinkPreview((message as any).conversation);
      }

      let quoted: WAMessage;

      if (options?.quoted) {
        const m = options?.quoted;

        const msg = m?.message ? m : ((await this.getMessage(m.key, true)) as WAMessage);

        if (msg) {
          quoted = msg;
        }
      }

      let messageSent: WAMessage;

      let mentions: string[];
      let contextInfo: any;

      if (isJidGroup(sender)) {
        let group;
        try {
          const cache = this.configService.get<CacheConf>('CACHE');
          if (!cache.REDIS.ENABLED && !cache.LOCAL.ENABLED) group = await this.findGroup({ groupJid: sender }, 'inner');
          else group = await this.getGroupMetadataCache(sender);
          // group = await this.findGroup({ groupJid: sender }, 'inner');
        } catch {
          throw new NotFoundException('Group not found');
        }

        if (!group) {
          throw new NotFoundException('Group not found');
        }

        if (options?.mentionsEveryOne) {
          mentions = group.participants.map((participant) => participant.id);
        } else if (options?.mentioned?.length) {
          mentions = options.mentioned.map((mention) => {
            const jid = createJid(mention);
            if (isJidGroup(jid)) {
              return null;
            }
            return jid;
          });
        }

        messageSent = await this.sendMessage(
          sender,
          message,
          mentions,
          linkPreview,
          quoted,
          options?.messageId ?? null,
          group?.ephemeralDuration,
          previewContext,
          // group?.participants,
        );
      } else {
        contextInfo = {
          mentionedJid: [],
          groupMentions: [],
          //expiration: 7776000,
          ephemeralSettingTimestamp: {
            low: Math.floor(Date.now() / 1000) - 172800,
            high: 0,
            unsigned: false,
          },
          disappearingMode: { initiator: 0 },
          ...previewContext,
        };
        messageSent = await this.sendMessage(
          sender,
          message,
          mentions,
          linkPreview,
          quoted,
          options?.messageId ?? null,
          undefined,
          contextInfo,
        );
      }

      if (Long.isLong(messageSent?.messageTimestamp)) {
        messageSent.messageTimestamp = messageSent.messageTimestamp?.toNumber();
      }

      const messageRaw = (await this.prepareMessageWithNative(messageSent)) as any;
      const messageForMediaProcessing = this.buildMessageForMediaPayload(messageRaw);

      const isMedia = this.hasValidMediaContent(messageForMediaProcessing);
      const isVideo = !!messageForMediaProcessing.message?.videoMessage;

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled && !isIntegration) {
        this.chatwootService.eventWhatsapp(
          Events.SEND_MESSAGE,
          { instanceName: this.instance.name, instanceId: this.instanceId },
          messageRaw,
        );
      }

      if (this.configService.get<Openai>('OPENAI').ENABLED && (messageRaw as any)?.message?.audioMessage) {
        const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({
          where: { instanceId: this.instanceId },
          include: { OpenaiCreds: true },
        });

        if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
          (messageRaw.message as any).speechToText =
            `[audio] ${await this.openaiService.speechToText(messageRaw, this)}`;
        }
      }

      if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        const msg = await this.prismaRepository.message.create({ data: messageRaw });

        if (isMedia && this.configService.get<S3>('S3').ENABLE) {
          try {
            if (isVideo && !this.configService.get<S3>('S3').SAVE_VIDEO) {
              throw new Error('Video upload is disabled.');
            }

            // Verificação adicional para garantir que há conteúdo de mídia real
            const hasRealMedia = this.hasValidMediaContent(messageForMediaProcessing);

            if (!hasRealMedia) {
              this.logger.warn('Message detected as media but contains no valid media content');
            } else {
              const media = await this.getBase64FromMediaMessage({ message: messageForMediaProcessing }, true);

              if (!media) {
                this.logger.verbose('No valid media to upload (messageContextInfo only), skipping MinIO');
                return;
              }

              const { buffer, mediaType, fileName, size } = media;

              const mimetype = mimeTypes.lookup(fileName).toString();

              const fullName = join(
                `${this.instance.id}`,
                messageRaw.key.remoteJid,
                `${messageRaw.key.id}`,
                mediaType,
                fileName,
              );

              await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });

              const mediaUrl = await s3Service.getObjectUrl(fullName);

              await this.prismaRepository.media.create({
                data: {
                  messageId: msg.id,
                  instanceId: this.instanceId,
                  type: mediaType,
                  fileName: fullName,
                  mimetype,
                  fileUrl: mediaUrl,
                },
              });

              messageRaw.message.mediaUrl = mediaUrl;

              await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
            }
          } catch (error) {
            this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
          }
        }
      }

      if (this.localWebhook.enabled) {
        if (isMedia && this.localWebhook.webhookBase64) {
          try {
            const buffer = await downloadMediaMessage(
              { key: messageRaw.key, message: messageRaw?.message },
              'buffer',
              {},
              { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
            );

            if (buffer) {
              messageRaw.message.base64 = buffer.toString('base64');
            } else {
              // retry to download media
              const buffer = await downloadMediaMessage(
                { key: messageRaw.key, message: messageRaw?.message },
                'buffer',
                {},
                { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
              );

              if (buffer) {
                messageRaw.message.base64 = buffer.toString('base64');
              }
            }
          } catch (error) {
            this.logger.error(['Error converting media to base64', error?.message]);
          }
        }
      }

      this.logger.verbose(messageSent);

      this.sendDataWebhook(Events.SEND_MESSAGE, messageRaw);

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled && isIntegration) {
        await chatbotController.emit({
          instance: { instanceName: this.instance.name, instanceId: this.instanceId },
          remoteJid: messageRaw.key.remoteJid,
          msg: messageRaw,
          pushName: messageRaw.pushName,
          isIntegration,
        });
      }

      return messageRaw;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Instance Controller
  public async sendPresence(data: SendPresenceDto) {
    try {
      const { number } = data;

      const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

      if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
        throw new BadRequestException(isWA);
      }

      const sender = isWA.jid;

      if (data?.delay && data?.delay > 20000) {
        let remainingDelay = data?.delay;
        while (remainingDelay > 20000) {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

          await delay(20000);

          await this.client.sendPresenceUpdate('paused', sender);

          remainingDelay -= 20000;
        }
        if (remainingDelay > 0) {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

          await delay(remainingDelay);

          await this.client.sendPresenceUpdate('paused', sender);
        }
      } else {
        await this.client.presenceSubscribe(sender);

        await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

        await delay(data?.delay);

        await this.client.sendPresenceUpdate('paused', sender);
      }

      return { presence: data.presence };
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Presence Controller
  public async setPresence(data: SetPresenceDto) {
    try {
      await this.client.sendPresenceUpdate(data.presence);

      return { presence: data.presence };
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Send Message Controller
  public async textMessage(data: SendTextDto, isIntegration = false) {
    const text = data.text;

    if (!text || text.trim().length === 0) {
      throw new BadRequestException('Text is required');
    }

    return await this.sendMessageWithTyping(
      data.number,
      { conversation: data.text },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        linkPreview: data?.linkPreview,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
        messageId: data?.messageId,
      },
      isIntegration,
    );
  }

  public async pollMessage(data: SendPollDto) {
    return await this.sendMessageWithTyping(
      data.number,
      { poll: { name: data.name, selectableCount: data.selectableCount, values: data.values } },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        linkPreview: data?.linkPreview,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
        messageId: data?.messageId,
      },
    );
  }

  private async formatStatusMessage(status: StatusMessage) {
    if (!status.type) {
      throw new BadRequestException('Type is required');
    }

    if (!status.content) {
      throw new BadRequestException('Content is required');
    }

    if (status.allContacts) {
      const contacts = await this.prismaRepository.contact.findMany({ where: { instanceId: this.instanceId } });

      if (!contacts.length) {
        throw new BadRequestException('Contacts not found');
      }

      status.statusJidList = contacts.filter((contact) => contact.pushName).map((contact) => contact.remoteJid);
    }

    if (!status.statusJidList?.length && !status.allContacts) {
      throw new BadRequestException('StatusJidList is required');
    }

    if (status.type === 'text') {
      if (!status.backgroundColor) {
        throw new BadRequestException('Background color is required');
      }

      if (!status.font) {
        throw new BadRequestException('Font is required');
      }

      return {
        content: { text: status.content },
        option: { backgroundColor: status.backgroundColor, font: status.font, statusJidList: status.statusJidList },
      };
    }
    if (status.type === 'image') {
      return {
        content: { image: { url: status.content }, caption: status.caption },
        option: { statusJidList: status.statusJidList },
      };
    }

    if (status.type === 'video') {
      return {
        content: { video: { url: status.content }, caption: status.caption },
        option: { statusJidList: status.statusJidList },
      };
    }

    if (status.type === 'audio') {
      const convert = await this.processAudioMp4(status.content);
      if (Buffer.isBuffer(convert)) {
        const result = {
          content: { audio: convert, ptt: true, mimetype: 'audio/ogg; codecs=opus' },
          option: { statusJidList: status.statusJidList },
        };

        return result;
      } else {
        throw new InternalServerErrorException(convert);
      }
    }

    throw new BadRequestException('Type not found');
  }

  public async statusMessage(data: SendStatusDto, file?: any) {
    const mediaData: SendStatusDto = { ...data };

    if (file) mediaData.content = file.buffer.toString('base64');

    const status = await this.formatStatusMessage(mediaData);

    const statusSent = await this.sendMessageWithTyping('status@broadcast', { status });

    return statusSent;
  }

  private async prepareMediaMessage(mediaMessage: MediaMessage) {
    try {
      const type = mediaMessage.mediatype === 'ptv' ? 'video' : mediaMessage.mediatype;

      let mediaInput: any;
      if (mediaMessage.mediatype === 'image') {
        let imageBuffer: Buffer;
        if (isURL(mediaMessage.media)) {
          let config: any = { responseType: 'arraybuffer' };

          if (this.localProxy?.enabled) {
            config = {
              ...config,
              httpsAgent: makeProxyAgent({
                host: this.localProxy.host,
                port: this.localProxy.port,
                protocol: this.localProxy.protocol,
                username: this.localProxy.username,
                password: this.localProxy.password,
              }),
            };
          }

          const response = await axios.get(mediaMessage.media, config);
          imageBuffer = Buffer.from(response.data, 'binary');
        } else {
          imageBuffer = Buffer.from(mediaMessage.media, 'base64');
        }

        mediaInput = await sharp(imageBuffer).jpeg().toBuffer();
        mediaMessage.fileName ??= 'image.jpg';
        mediaMessage.mimetype = 'image/jpeg';
      } else {
        mediaInput = isURL(mediaMessage.media)
          ? { url: mediaMessage.media }
          : Buffer.from(mediaMessage.media, 'base64');
      }

      const prepareMedia = await prepareWAMessageMedia(
        {
          [type]: mediaInput,
        } as any,
        { upload: this.client.waUploadToServer },
      );

      const mediaType = mediaMessage.mediatype + 'Message';

      if (mediaMessage.mediatype === 'document' && !mediaMessage.fileName) {
        const regex = new RegExp(/.*\/(.+?)\./);
        const arrayMatch = regex.exec(mediaMessage.media);
        mediaMessage.fileName = arrayMatch[1];
      }

      if (mediaMessage.mediatype === 'image' && !mediaMessage.fileName) {
        mediaMessage.fileName = 'image.jpg';
      }

      if (mediaMessage.mediatype === 'video' && !mediaMessage.fileName) {
        mediaMessage.fileName = 'video.mp4';
      }

      let mimetype: string | false;

      if (mediaMessage.mimetype) {
        mimetype = mediaMessage.mimetype;
      } else {
        mimetype = mimeTypes.lookup(mediaMessage.fileName);

        if (!mimetype && isURL(mediaMessage.media)) {
          let config: any = { responseType: 'arraybuffer' };

          if (this.localProxy?.enabled) {
            config = {
              ...config,
              httpsAgent: makeProxyAgent({
                host: this.localProxy.host,
                port: this.localProxy.port,
                protocol: this.localProxy.protocol,
                username: this.localProxy.username,
                password: this.localProxy.password,
              }),
            };
          }

          const response = await axios.get(mediaMessage.media, config);

          mimetype = response.headers['content-type'];
        }
      }

      if (mediaMessage.mediatype === 'ptv') {
        prepareMedia[mediaType] = prepareMedia[type + 'Message'];
        mimetype = 'video/mp4';

        if (!prepareMedia[mediaType]) {
          throw new Error('Failed to prepare video message');
        }

        try {
          let mediaInput;
          if (isURL(mediaMessage.media)) {
            mediaInput = mediaMessage.media;
          } else {
            const mediaBuffer = Buffer.from(mediaMessage.media, 'base64');
            if (!mediaBuffer || mediaBuffer.length === 0) {
              throw new Error('Invalid media buffer');
            }
            mediaInput = mediaBuffer;
          }

          const duration = await getVideoDuration(mediaInput);
          if (!duration || duration <= 0) {
            throw new Error('Invalid media duration');
          }

          this.logger.verbose(`Video duration: ${duration} seconds`);
          prepareMedia[mediaType].seconds = duration;
        } catch (error) {
          this.logger.error('Error getting video duration:');
          this.logger.error(error);
          throw new Error(`Failed to get video duration: ${error.message}`);
        }
      }

      if (mediaMessage?.fileName) {
        mimetype = mimeTypes.lookup(mediaMessage.fileName).toString();
        if (mimetype === 'application/mp4') {
          mimetype = 'video/mp4';
        }
      }

      prepareMedia[mediaType].caption = mediaMessage?.caption;
      prepareMedia[mediaType].mimetype = mimetype;
      prepareMedia[mediaType].fileName = mediaMessage.fileName;

      if (mediaMessage.mediatype === 'video') {
        prepareMedia[mediaType].gifPlayback = false;
      }

      return generateWAMessageFromContent(
        '',
        { [mediaType]: { ...prepareMedia[mediaType] } },
        { userJid: this.instance.wuid },
      );
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString() || error);
    }
  }

  private async convertToWebP(image: string): Promise<Buffer> {
    try {
      let imageBuffer: Buffer;

      if (isBase64(image)) {
        const base64Data = image.replace(/^data:image\/(jpeg|png|gif);base64,/, '');
        imageBuffer = Buffer.from(base64Data, 'base64');
      } else {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(image);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        const response = await axios.get(url, config);
        imageBuffer = Buffer.from(response.data, 'binary');
      }

      const isAnimated = this.isAnimated(image, imageBuffer);

      if (isAnimated) {
        return await sharp(imageBuffer, { animated: true }).webp({ quality: 80 }).toBuffer();
      } else {
        return await sharp(imageBuffer).webp().toBuffer();
      }
    } catch (error) {
      this.logger.error({ local: 'convertToWebP', error });
      throw error;
    }
  }

  private isAnimatedWebp(buffer: Buffer): boolean {
    if (buffer.length < 12) return false;

    return buffer.indexOf(Buffer.from('ANIM')) !== -1;
  }

  private isAnimated(image: string, buffer: Buffer): boolean {
    const lowerCaseImage = image.toLowerCase();

    if (lowerCaseImage.includes('.gif')) return true;

    if (lowerCaseImage.includes('.webp')) return this.isAnimatedWebp(buffer);

    return false;
  }

  public async mediaSticker(data: SendStickerDto, file?: any) {
    const mediaData: SendStickerDto = { ...data };

    if (file) mediaData.sticker = file.buffer.toString('base64');

    const convert = data?.notConvertSticker
      ? Buffer.from(data.sticker, 'base64')
      : await this.convertToWebP(data.sticker);
    const gifPlayback = data.sticker.includes('.gif');
    const result = await this.sendMessageWithTyping(
      data.number,
      { sticker: convert, gifPlayback },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
        messageId: data?.messageId,
      },
    );

    return result;
  }

  public async mediaMessage(data: SendMediaDto, file?: any, isIntegration = false) {
    const mediaData: SendMediaDto = { ...data };

    if (file) mediaData.media = file.buffer.toString('base64');

    const generate = await this.prepareMediaMessage(mediaData);

    const mediaSent = await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
        messageId: data?.messageId,
      },
      isIntegration,
    );

    return mediaSent;
  }

  public async ptvMessage(data: SendPtvDto, file?: any, isIntegration = false) {
    const mediaData: SendMediaDto = {
      number: data.number,
      media: data.video,
      mediatype: 'ptv',
      delay: data?.delay,
      quoted: data?.quoted,
      mentionsEveryOne: data?.mentionsEveryOne,
      mentioned: data?.mentioned,
      messageId: data?.messageId,
    };

    if (file) mediaData.media = file.buffer.toString('base64');

    const generate = await this.prepareMediaMessage(mediaData);

    const mediaSent = await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
        messageId: data?.messageId,
      },
      isIntegration,
    );

    return mediaSent;
  }

  public async processAudioMp4(audio: string) {
    let inputStream: PassThrough;

    if (isURL(audio)) {
      const response = await axios.get(audio, { responseType: 'stream' });
      inputStream = response.data;
    } else {
      const audioBuffer = Buffer.from(audio, 'base64');
      inputStream = new PassThrough();
      inputStream.end(audioBuffer);
    }

    return new Promise<Buffer>((resolve, reject) => {
      const ffmpegProcess = spawn(ffmpegPath.path, [
        '-i',
        'pipe:0',
        '-vn',
        '-ab',
        '128k',
        '-ar',
        '44100',
        '-f',
        'mp4',
        '-movflags',
        'frag_keyframe+empty_moov',
        'pipe:1',
      ]);

      const outputChunks: Buffer[] = [];
      let stderrData = '';

      ffmpegProcess.stdout.on('data', (chunk) => {
        outputChunks.push(chunk);
      });

      ffmpegProcess.stderr.on('data', (data) => {
        stderrData += data.toString();
        this.logger.verbose(`ffmpeg stderr: ${data}`);
      });

      ffmpegProcess.on('error', (error) => {
        this.logger.error({ local: 'processAudioMp4.ffmpeg', error });
        reject(error);
      });

      ffmpegProcess.on('close', (code) => {
        if (code === 0) {
          this.logger.verbose('Audio converted to mp4');
          const outputBuffer = Buffer.concat(outputChunks);
          resolve(outputBuffer);
        } else {
          this.logger.error(`ffmpeg exited with code ${code}`);
          this.logger.error(`ffmpeg stderr: ${stderrData}`);
          reject(new Error(`ffmpeg exited with code ${code}: ${stderrData}`));
        }
      });

      inputStream.pipe(ffmpegProcess.stdin);

      inputStream.on('error', (err) => {
        this.logger.error({ local: 'processAudioMp4.inputStream', error: err });
        ffmpegProcess.stdin.end();
        reject(err);
      });
    });
  }

  public async processAudio(audio: string): Promise<Buffer> {
    const audioConverterConfig = this.configService.get<AudioConverter>('AUDIO_CONVERTER');
    if (audioConverterConfig.API_URL) {
      this.logger.verbose('Using audio converter API');
      const formData = new FormData();

      if (isURL(audio)) {
        formData.append('url', audio);
      } else {
        formData.append('base64', audio);
      }

      const { data } = await axios.post(audioConverterConfig.API_URL, formData, {
        headers: { ...formData.getHeaders(), apikey: audioConverterConfig.API_KEY },
      });

      if (!data.audio) {
        throw new InternalServerErrorException('Failed to convert audio');
      }

      this.logger.verbose('Audio converted');
      return Buffer.from(data.audio, 'base64');
    } else {
      let inputAudioStream: PassThrough;

      if (isURL(audio)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(audio);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        const config: any = { responseType: 'stream' };

        const response = await axios.get(url, config);
        inputAudioStream = response.data.pipe(new PassThrough());
      } else {
        const audioBuffer = Buffer.from(audio, 'base64');
        inputAudioStream = new PassThrough();
        inputAudioStream.end(audioBuffer);
      }

      const isLpcm = isURL(audio) && /\.lpcm($|\?)/i.test(audio);

      return new Promise((resolve, reject) => {
        const outputAudioStream = new PassThrough();
        const chunks: Buffer[] = [];

        outputAudioStream.on('data', (chunk) => chunks.push(chunk));
        outputAudioStream.on('end', () => {
          const outputBuffer = Buffer.concat(chunks);
          resolve(outputBuffer);
        });

        outputAudioStream.on('error', (error) => {
          this.logger.error({ local: 'processAudio.outputStream', error });
          reject(error);
        });

        ffmpeg.setFfmpegPath(ffmpegPath.path);

        let command = ffmpeg(inputAudioStream);

        if (isLpcm) {
          this.logger.verbose('Detected LPCM input – applying raw PCM settings');
          command = command.inputFormat('s16le').inputOptions(['-ar', '24000', '-ac', '1']);
        }

        command
          .outputFormat('ogg')
          .noVideo()
          .audioCodec('libopus')
          .addOutputOptions('-avoid_negative_ts make_zero')
          .audioBitrate('48k')
          .audioFrequency(48000)
          .audioChannels(1)
          .outputOptions([
            '-write_xing',
            '0',
            '-compression_level',
            '10',
            '-application',
            'voip',
            '-fflags',
            '+bitexact',
            '-flags',
            '+bitexact',
            '-id3v2_version',
            '0',
            '-map_metadata',
            '-1',
            '-map_chapters',
            '-1',
            '-write_bext',
            '0',
          ])
          .pipe(outputAudioStream, { end: true })
          .on('error', (error) => {
            this.logger.error({ local: 'processAudio.ffmpeg', error });
            reject(error);
          });
      });
    }
  }

  private async getAudioMetadata(audioBuffer: Buffer): Promise<{ seconds: number; waveform: Uint8Array }> {
    try {
      this.logger.debug('Decoding audio buffer for metadata extraction...');
      const audioData = await audioDecode(audioBuffer);

      // Extract duration
      const seconds = Math.ceil(audioData.duration);
      this.logger.debug(`Audio duration: ${seconds} seconds`);

      // Generate waveform
      const samples = audioData.getChannelData(0);
      const waveformLength = 64;
      const samplesPerWaveform = Math.max(1, Math.floor(samples.length / waveformLength));

      // First pass: calculate raw averages
      const rawValues: number[] = [];
      for (let i = 0; i < waveformLength; i++) {
        const start = i * samplesPerWaveform;
        const end = start + samplesPerWaveform;
        let sum = 0;
        for (let j = start; j < end && j < samples.length; j++) {
          sum += Math.abs(samples[j]);
        }
        const avg = sum / samplesPerWaveform;
        rawValues.push(avg);
      }

      // Find max value for normalization
      const maxValue = Math.max(...rawValues);

      // Second pass: normalize to 0-100 range
      const waveform = new Uint8Array(waveformLength);
      if (maxValue > 0) {
        for (let i = 0; i < waveformLength; i++) {
          const normalized = Math.floor((rawValues[i] / maxValue) * 100);
          waveform[i] = rawValues[i] > 0 ? Math.max(5, Math.min(100, normalized)) : 0;
        }
      } else {
        waveform.fill(50);
      }

      this.logger.debug(`Generated waveform with ${waveform.length} values`);

      return { seconds, waveform };
    } catch (error) {
      this.logger.warn(`Failed to extract audio metadata: ${error.message}, using defaults`);
      const defaultWaveform = new Uint8Array(64);
      defaultWaveform.fill(50);
      return { seconds: 1, waveform: defaultWaveform };
    }
  }

  public async audioWhatsapp(data: SendAudioDto, file?: any, isIntegration = false) {
    const mediaData: SendAudioDto = { ...data };

    if (file?.buffer) {
      mediaData.audio = file.buffer.toString('base64');
    } else if (!isURL(data.audio) && !isBase64(data.audio)) {
      this.logger.warn('Invalid file or audio source');
      throw new BadRequestException('File buffer, URL, or base64 audio is required');
    }

    if (!data?.encoding && data?.encoding !== false) {
      data.encoding = true;
    }

    if (data?.encoding) {
      const convert = await this.processAudio(mediaData.audio);

      if (Buffer.isBuffer(convert)) {
        const { seconds, waveform } = await this.getAudioMetadata(convert);

        const messageContent = { audio: convert, ptt: true, mimetype: 'audio/ogg; codecs=opus', seconds, waveform };

        const result = this.sendMessageWithTyping<AnyMessageContent>(
          data.number,
          messageContent as any,
          { presence: 'recording', delay: data?.delay },
          isIntegration,
        );

        return result;
      } else {
        throw new InternalServerErrorException('Failed to convert audio');
      }
    }

    const audioBuffer = isURL(data.audio) ? { url: data.audio } : Buffer.from(data.audio, 'base64');
    let metadata: { seconds: number; waveform: Uint8Array } | undefined;

    // Only generate waveform for buffers, not URLs
    if (Buffer.isBuffer(audioBuffer)) {
      metadata = await this.getAudioMetadata(audioBuffer);
    }

    return await this.sendMessageWithTyping<AnyMessageContent>(
      data.number,
      {
        audio: audioBuffer,
        ptt: true,
        mimetype: 'audio/ogg; codecs=opus',
        ...(metadata && { seconds: metadata.seconds, waveform: metadata.waveform }),
      },
      { presence: 'recording', delay: data?.delay },
      isIntegration,
    );
  }

  private generateRandomId(length = 11) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
      result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
  }

  private toJSONString(button: Button): string {
    const toString = (obj: any) => JSON.stringify(obj);

    const json = {
      call: () => toString({ display_text: button.displayText, phone_number: button.phoneNumber }),
      reply: () => toString({ display_text: button.displayText, id: button.id }),
      copy: () => toString({ display_text: button.displayText, copy_code: button.copyCode }),
      url: () => toString({ display_text: button.displayText, url: button.url, merchant_url: button.url }),
      pix: () =>
        toString({
          currency: button.currency,
          total_amount: { value: 0, offset: 100 },
          reference_id: this.generateRandomId(),
          type: 'physical-goods',
          order: {
            status: 'pending',
            subtotal: { value: 0, offset: 100 },
            order_type: 'ORDER',
            items: [
              { name: '', amount: { value: 0, offset: 100 }, quantity: 0, sale_amount: { value: 0, offset: 100 } },
            ],
          },
          payment_settings: [
            {
              type: 'pix_static_code',
              pix_static_code: {
                merchant_name: button.name,
                key: button.key,
                key_type: this.mapKeyType.get(button.keyType),
              },
            },
          ],
          share_payment_status: false,
        }),
    };

    return json[button.type]?.() || '';
  }

  private readonly mapType = new Map<TypeButton, string>([
    ['reply', 'quick_reply'],
    ['copy', 'cta_copy'],
    ['url', 'cta_url'],
    ['call', 'cta_call'],
    ['pix', 'payment_info'],
  ]);

  private readonly mapKeyType = new Map<KeyType, string>([
    ['phone', 'PHONE'],
    ['email', 'EMAIL'],
    ['cpf', 'CPF'],
    ['cnpj', 'CNPJ'],
    ['random', 'EVP'],
  ]);

  public async buttonMessage(data: SendButtonsDto) {
    if (!data.buttons || data.buttons.length === 0) {
      throw new BadRequestException('At least one button is required');
    }

    const hasReplyButtons = data.buttons.some((btn) => btn.type === 'reply');
    const hasPixButton = data.buttons.some((btn) => btn.type === 'pix');
    const hasCTAButtons = data.buttons.some((btn) => btn.type === 'url' || btn.type === 'call' || btn.type === 'copy');

    /* =========================
     * REGRAS DE VALIDAÇÃO
     * ========================= */

    // Reply
    if (hasReplyButtons) {
      if (data.buttons.length > 3) {
        throw new BadRequestException('Maximum of 3 reply buttons allowed');
      }
      if (hasCTAButtons || hasPixButton) {
        throw new BadRequestException('Reply buttons cannot be mixed with CTA or PIX buttons');
      }
    }

    // PIX
    if (hasPixButton) {
      if (data.buttons.length > 1) {
        throw new BadRequestException('Only one PIX button is allowed');
      }
      if (hasReplyButtons || hasCTAButtons) {
        throw new BadRequestException('PIX button cannot be mixed with other button types');
      }

      const message: proto.IMessage = {
        viewOnceMessage: {
          message: {
            interactiveMessage: {
              nativeFlowMessage: {
                buttons: [
                  {
                    name: this.mapType.get('pix'),
                    buttonParamsJson: this.toJSONString(data.buttons[0]),
                  },
                ],
                messageParamsJson: JSON.stringify({
                  from: 'api',
                  templateId: v4(),
                }),
              },
            },
          },
        },
      };

      return await this.sendMessageWithTyping(data.number, message, {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      });
    }

    // CTA (url / call / copy)
    if (hasCTAButtons) {
      if (data.buttons.length > 2) {
        throw new BadRequestException('Maximum of 2 CTA buttons allowed');
      }
      if (hasReplyButtons) {
        throw new BadRequestException('CTA buttons cannot be mixed with reply buttons');
      }
    }

    /* =========================
     * HEADER (opcional)
     * ========================= */

    const generatedMedia = data?.thumbnailUrl
      ? await this.prepareMediaMessage({ mediatype: 'image', media: data.thumbnailUrl })
      : null;

    /* =========================
     * BOTÕES
     * ========================= */

    const buttons = data.buttons.map((btn) => ({
      name: this.mapType.get(btn.type),
      buttonParamsJson: this.toJSONString(btn),
    }));

    /* =========================
     * MENSAGEM FINAL
     * ========================= */

    const message: proto.IMessage = {
      viewOnceMessage: {
        message: {
          interactiveMessage: {
            body: {
              text: (() => {
                let text = `*${data.title}*`;
                if (data?.description) {
                  text += `\n\n${data.description}`;
                }
                return text;
              })(),
            },
            footer: data?.footer ? { text: data.footer } : undefined,
            header: generatedMedia?.message?.imageMessage
              ? {
                  hasMediaAttachment: true,
                  imageMessage: generatedMedia.message.imageMessage,
                }
              : undefined,
            nativeFlowMessage: {
              buttons,
              messageParamsJson: JSON.stringify({
                from: 'api',
                templateId: v4(),
              }),
            },
          },
        },
      },
    };

    return await this.sendMessageWithTyping(data.number, message, {
      delay: data?.delay,
      presence: 'composing',
      quoted: data?.quoted,
      mentionsEveryOne: data?.mentionsEveryOne,
      mentioned: data?.mentioned,
    });
  }

  public async locationMessage(data: SendLocationDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        locationMessage: {
          degreesLatitude: data.latitude,
          degreesLongitude: data.longitude,
          name: data?.name,
          address: data?.address,
        },
      },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  public async listMessage(data: SendListDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        listMessage: {
          title: data.title,
          description: data.description,
          buttonText: data?.buttonText,
          footerText: data?.footerText,
          sections: data.sections,
          listType: 2,
        },
      },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  public async contactMessage(data: SendContactDto) {
    const message: proto.IMessage = {};

    const vcard = (contact: ContactMessage) => {
      let result = 'BEGIN:VCARD\n' + 'VERSION:3.0\n' + `N:${contact.fullName}\n` + `FN:${contact.fullName}\n`;

      if (contact.organization) {
        result += `ORG:${contact.organization};\n`;
      }

      if (contact.email) {
        result += `EMAIL:${contact.email}\n`;
      }

      if (contact.url) {
        result += `URL:${contact.url}\n`;
      }

      if (!contact.wuid) {
        contact.wuid = createJid(contact.phoneNumber);
      }

      result += `item1.TEL;waid=${contact.wuid}:${contact.phoneNumber}\n` + 'item1.X-ABLabel:Celular\n' + 'END:VCARD';

      return result;
    };

    if (data.contact.length === 1) {
      message.contactMessage = { displayName: data.contact[0].fullName, vcard: vcard(data.contact[0]) };
    } else {
      message.contactsArrayMessage = {
        displayName: `${data.contact.length} contacts`,
        contacts: data.contact.map((contact) => {
          return { displayName: contact.fullName, vcard: vcard(contact) };
        }),
      };
    }

    return await this.sendMessageWithTyping(data.number, { ...message }, {});
  }

  public async reactionMessage(data: SendReactionDto) {
    return await this.sendMessageWithTyping(data.key.remoteJid, {
      reactionMessage: { key: data.key, text: data.reaction },
    });
  }

  // Chat Controller
  public async whatsappNumber(data: WhatsAppNumberDto) {
    const jids: {
      groups: { number: string; jid: string }[];
      broadcast: { number: string; jid: string }[];
      users: { number: string; jid: string; name?: string }[];
    } = { groups: [], broadcast: [], users: [] };

    const onWhatsapp: OnWhatsAppDto[] = [];

    data.numbers.forEach((number) => {
      const jid = createJid(number);

      if (isJidNewsletter(jid)) {
        onWhatsapp.push(
          new OnWhatsAppDto(
            jid,
            true, // Newsletters are always valid
            number,
            undefined, // Can be fetched later if needed
            'newsletter', // Indicate it's a newsletter type
          ),
        );
        return;
      }

      if (isJidGroup(jid)) {
        jids.groups.push({ number, jid });
      } else if (jid === 'status@broadcast') {
        jids.broadcast.push({ number, jid });
      } else {
        jids.users.push({ number, jid });
      }
    });

    // BROADCAST
    onWhatsapp.push(...jids.broadcast.map(({ jid, number }) => new OnWhatsAppDto(jid, false, number)));

    // GROUPS
    const groups = await Promise.all(
      jids.groups.map(async ({ jid, number }) => {
        const group = await this.findGroup({ groupJid: jid }, 'inner');

        if (!group) {
          return new OnWhatsAppDto(jid, false, number);
        }

        return new OnWhatsAppDto(group.id, true, number, group?.subject);
      }),
    );
    onWhatsapp.push(...groups);

    // USERS
    const userContexts = await mapWithConcurrencyLimit(jids.users, CONTACT_PROFILE_LOOKUP_CONCURRENCY, async (user) => {
      const normalizedInputJid = this.normalizeJid(user.jid) ?? user.jid;
      const nativePnJid = await this.resolveLidToPnJid(normalizedInputJid);
      const identity = await this.resolveCanonicalJidWithNative(
        nativePnJid ?? normalizedInputJid,
        nativePnJid && nativePnJid !== normalizedInputJid ? normalizedInputJid : undefined,
        {
          remoteLid: this.isLidJid(normalizedInputJid) ? normalizedInputJid : undefined,
        },
      );

      return {
        ...user,
        jid: normalizedInputJid,
        nativePnJid,
        identity,
        candidates: this.buildIdentityCandidates(identity, normalizedInputJid, nativePnJid),
      };
    });

    const allIdentityCandidates = [...new Set(userContexts.flatMap((user) => user.candidates))];
    const contacts = allIdentityCandidates.length
      ? await this.prismaRepository.contact.findMany({
          where: { instanceId: this.instanceId, remoteJid: { in: allIdentityCandidates } },
        })
      : [];

    const contactByJid = new Map<string, (typeof contacts)[number]>(
      contacts.map((contact) => [contact.remoteJid, contact] as const),
    );

    const cachedNumbers = await getOnWhatsappCache(allIdentityCandidates);

    const findCachedIdentity = (candidates: string[]) =>
      cachedNumbers.find((cached) =>
        candidates.some((candidate) => cached.remoteJid === candidate || cached.jidOptions.includes(candidate)),
      );

    const pickContactName = (candidates: string[]) => {
      for (const candidate of candidates) {
        const name = this.pickPreferredName(contactByJid.get(candidate)?.pushName);
        if (name) {
          return name;
        }
      }

      return undefined;
    };

    const usersNeedingVerification = userContexts.filter((user) => {
      if (user.nativePnJid || user.jid.includes('@lid')) {
        return false;
      }

      return !findCachedIdentity(user.candidates);
    });

    let verify: { jid: string; exists: boolean }[] = [];
    const normalNumbersNotInCache = usersNeedingVerification.map((user) => user.jid);

    if (normalNumbersNotInCache.length > 0) {
      this.logger.verbose(`Checking ${normalNumbersNotInCache.length} numbers via Baileys (not found in cache)`);
      verify = await this.client.onWhatsApp(...normalNumbersNotInCache);
    }

    const verifiedUsers = await Promise.all(
      userContexts.map(async (user) => {
        const cached = findCachedIdentity(user.candidates);
        if (cached) {
          this.logger.verbose(`Number ${user.number} found in cache`);
          return new OnWhatsAppDto(
            cached.remoteJid,
            true,
            user.number,
            pickContactName(this.uniqueNormalizedJids(...user.candidates, cached.remoteJid)),
            cached.lid || user.candidates.some((candidate) => candidate.includes('@lid')) ? 'lid' : undefined,
          );
        }

        if (user.nativePnJid) {
          return new OnWhatsAppDto(
            user.nativePnJid,
            true,
            user.number,
            pickContactName(this.uniqueNormalizedJids(...user.candidates, user.nativePnJid)),
            'lid',
          );
        }

        if (user.jid.includes('@lid')) {
          const localContact = await this.findBestContactByJids(user.candidates);
          const resolvedJid = localContact?.remoteJid ?? user.identity.remoteJid ?? user.jid;

          return new OnWhatsAppDto(
            resolvedJid,
            !!localContact,
            user.number,
            this.pickPreferredName(localContact?.pushName),
            'lid',
          );
        }

        let numberVerified: (typeof verify)[0] | null = null;

        if (user.number.startsWith('55')) {
          const numberWithDigit =
            user.number.slice(4, 5) === '9' && user.number.length === 13
              ? user.number
              : `${user.number.slice(0, 4)}9${user.number.slice(4)}`;
          const numberWithoutDigit =
            user.number.length === 12 ? user.number : user.number.slice(0, 4) + user.number.slice(5);

          numberVerified = verify.find(
            (v) => v.jid === `${numberWithDigit}@s.whatsapp.net` || v.jid === `${numberWithoutDigit}@s.whatsapp.net`,
          );
        }

        if (!numberVerified && (user.number.startsWith('52') || user.number.startsWith('54'))) {
          let prefix = '';
          if (user.number.startsWith('52')) {
            prefix = '1';
          }
          if (user.number.startsWith('54')) {
            prefix = '9';
          }

          const numberWithDigit =
            user.number.slice(2, 3) === prefix && user.number.length === 13
              ? user.number
              : `${user.number.slice(0, 2)}${prefix}${user.number.slice(2)}`;
          const numberWithoutDigit =
            user.number.length === 12 ? user.number : user.number.slice(0, 2) + user.number.slice(3);

          numberVerified = verify.find(
            (v) => v.jid === `${numberWithDigit}@s.whatsapp.net` || v.jid === `${numberWithoutDigit}@s.whatsapp.net`,
          );
        }

        if (!numberVerified) {
          numberVerified = verify.find((v) => v.jid === user.jid);
        }

        const numberJid = numberVerified?.jid || user.identity.remoteJid || user.jid;

        return new OnWhatsAppDto(
          numberJid,
          !!numberVerified?.exists,
          user.number,
          pickContactName(this.uniqueNormalizedJids(...user.candidates, numberJid)),
          undefined,
        );
      }),
    );

    onWhatsapp.push(...verifiedUsers);

    const numbersToCache = userContexts
      .map((user, index) => ({ user, result: verifiedUsers[index] }))
      .filter(({ user, result }) => result.exists && !findCachedIdentity(user.candidates))
      .map(({ user, result }) => ({
        remoteJid: result.jid,
        remoteJidAlt:
          user.jid.includes('@lid') && result.jid !== user.jid ? user.jid : (user.identity.remoteJidAlt ?? undefined),
        lid:
          user.jid.includes('@lid') ||
          user.identity.remoteLid ||
          user.identity.remoteJidAlt?.includes?.('@lid') ||
          result.jid.includes('@lid')
            ? ('lid' as const)
            : undefined,
      }));

    if (numbersToCache.length > 0) {
      this.logger.verbose(`Salvando ${numbersToCache.length} números no cache`);
      await saveOnWhatsappCache(numbersToCache);
    }

    return onWhatsapp;
  }

  public async markMessageAsRead(data: ReadMessageDto) {
    try {
      const keys: proto.IMessageKey[] = [];
      data.readMessages.forEach((read) => {
        if (read.remoteJid?.includes('@') && !read.remoteJid.includes('@broadcast')) {
          keys.push({ remoteJid: read.remoteJid, fromMe: read.fromMe, id: read.id });
        }
      });
      if (keys.length === 0) {
        return { message: 'No valid message keys to mark as read', read: 'skipped' };
      }
      await this.client.readMessages(keys);
      return { message: 'Read messages', read: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Read messages fail', error.toString());
    }
  }

  public async getLastMessage(number: string) {
    const where: any = { key: { remoteJid: number }, instanceId: this.instance.id };

    const messages = await this.prismaRepository.message.findMany({
      where,
      orderBy: { messageTimestamp: 'desc' },
      take: 1,
    });

    if (messages.length === 0) {
      throw new NotFoundException('Messages not found');
    }

    let lastMessage = messages.pop();

    for (const message of messages) {
      if (message.messageTimestamp >= lastMessage.messageTimestamp) {
        lastMessage = message;
      }
    }

    return lastMessage as unknown as LastMessage;
  }

  public async archiveChat(data: ArchiveChatDto) {
    try {
      let last_message = data.lastMessage;
      let number = data.chat;

      if (!last_message && number) {
        last_message = await this.getLastMessage(number);
      } else {
        last_message = data.lastMessage;
        last_message.messageTimestamp = last_message?.messageTimestamp ?? Date.now();
        number = last_message?.key?.remoteJid;
      }

      if (!last_message || Object.keys(last_message).length === 0) {
        throw new NotFoundException('Last message not found');
      }

      await this.client.chatModify({ archive: data.archive, lastMessages: [last_message] }, createJid(number));

      return { chatId: number, archived: true };
    } catch (error) {
      throw new InternalServerErrorException({
        archived: false,
        message: ['An error occurred while archiving the chat. Open a calling.', error.toString()],
      });
    }
  }

  public async markChatUnread(data: MarkChatUnreadDto) {
    try {
      let last_message = data.lastMessage;
      let number = data.chat;

      if (!last_message && number) {
        last_message = await this.getLastMessage(number);
      } else {
        last_message = data.lastMessage;
        last_message.messageTimestamp = last_message?.messageTimestamp ?? Date.now();
        number = last_message?.key?.remoteJid;
      }

      if (!last_message || Object.keys(last_message).length === 0) {
        throw new NotFoundException('Last message not found');
      }

      await this.client.chatModify({ markRead: false, lastMessages: [last_message] }, createJid(number));

      return { chatId: number, markedChatUnread: true };
    } catch (error) {
      throw new InternalServerErrorException({
        markedChatUnread: false,
        message: ['An error occurred while marked unread the chat. Open a calling.', error.toString()],
      });
    }
  }

  public async deleteMessage(del: DeleteMessage) {
    try {
      const response = await this.client.sendMessage(del.remoteJid, { delete: del });
      if (response) {
        const messageId = response.message?.protocolMessage?.key?.id;
        if (messageId) {
          const isLogicalDeleted = configService.get<Database>('DATABASE').DELETE_DATA.LOGICAL_MESSAGE_DELETE;
          let message = await this.prismaRepository.message.findFirst({
            where: { key: { path: ['id'], equals: messageId } },
          });
          if (isLogicalDeleted) {
            if (!message) return response;
            const existingKey = typeof message?.key === 'object' && message.key !== null ? message.key : {};
            message = await this.prismaRepository.message.update({
              where: { id: message.id },
              data: { key: { ...existingKey, deleted: true }, status: 'DELETED' },
            });
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              const messageUpdate: any = {
                messageId: message.id,
                keyId: messageId,
                remoteJid: response.key.remoteJid,
                fromMe: response.key.fromMe,
                participant: response.key?.participant,
                status: 'DELETED',
                instanceId: this.instanceId,
              };
              await this.prismaRepository.messageUpdate.create({ data: messageUpdate });
            }
          } else {
            if (!message) return response;
            await this.prismaRepository.message.deleteMany({ where: { id: message.id } });
          }
          this.sendDataWebhook(Events.MESSAGES_DELETE, {
            id: message.id,
            instanceId: message.instanceId,
            key: message.key,
            messageType: message.messageType,
            status: 'DELETED',
            source: message.source,
            messageTimestamp: message.messageTimestamp,
            pushName: message.pushName,
            participant: message.participant,
            message: message.message,
          });
        }
      }

      return response;
    } catch (error) {
      throw new InternalServerErrorException('Error while deleting message for everyone', error?.toString());
    }
  }

  public async mapMediaType(mediaType) {
    const map = {
      imageMessage: 'image',
      videoMessage: 'video',
      documentMessage: 'document',
      stickerMessage: 'sticker',
      audioMessage: 'audio',
      ptvMessage: 'video',
    };
    return map[mediaType] || null;
  }

  public async getBase64FromMediaMessage(data: getBase64FromMediaMessageDto, getBuffer = false) {
    try {
      const m = data?.message;
      const convertToMp4 = data?.convertToMp4 ?? false;

      let msg = m?.message ? m : ((await this.getMessage(m.key, true)) as proto.IWebMessageInfo);

      if (!msg) {
        throw 'Message not found';
      }

      for (const subtype of MessageSubtype) {
        if (msg.message[subtype]) {
          msg.message = msg.message[subtype].message;
        }
      }

      if ('messageContextInfo' in msg.message && Object.keys(msg.message).length === 1) {
        // Иногда приходят апдейты, где тело содержит только messageContextInfo (например, статусы/ACK).
        // Пытаемся подтянуть полное сообщение из базы перед тем как падать.
        if (m?.key) {
          const stored = (await this.getMessage(m.key, true)) as proto.IWebMessageInfo;
          if (stored?.message) {
            msg = stored;
          }
        }

        if ('messageContextInfo' in msg.message && Object.keys(msg.message).length === 1) {
          this.logger.verbose('Message contains only messageContextInfo, skipping media processing');
          return null;
        }
      }

      let mediaMessage: any;
      let mediaType: string;

      if (msg.message?.templateMessage) {
        const template =
          msg.message.templateMessage.hydratedTemplate || msg.message.templateMessage.hydratedFourRowTemplate;

        for (const type of TypeMediaMessage) {
          if (template[type]) {
            mediaMessage = template[type];
            mediaType = type;
            msg.message = { [type]: { ...template[type], url: template[type].staticUrl } };
            break;
          }
        }

        if (!mediaMessage) {
          throw new MediaUnavailableError(422, undefined, 'not_media_message');
        }
      } else {
        for (const type of TypeMediaMessage) {
          mediaMessage = msg.message[type];
          if (mediaMessage) {
            mediaType = type;
            break;
          }
        }

        if (!mediaMessage) {
          throw new MediaUnavailableError(422, undefined, 'not_media_message');
        }
      }

      if (typeof mediaMessage['mediaKey'] === 'object') {
        msg.message[mediaType].mediaKey = Uint8Array.from(Object.values(mediaMessage['mediaKey']));
      }

      let buffer: Buffer;

      try {
        buffer = await downloadMediaMessage(
          { key: msg?.key, message: msg?.message },
          'buffer',
          {},
          { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
        );
      } catch (error) {
        const mediaUnavailableError = this.mediaUnavailableError(error);
        if (mediaUnavailableError) {
          this.logger.warn(
            `Historical media download unavailable with status ${mediaUnavailableError.statusCode}; skipping retry`,
          );
          throw mediaUnavailableError;
        }

        this.logger.error('Download Media failed, trying to retry in 5 seconds...');
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const mediaType = Object.keys(msg.message).find((key) => key.endsWith('Message'));
        const mappedMediaType = mediaType ? await this.mapMediaType(mediaType) : null;
        const mediaKey = mediaType ? msg.message?.[mediaType]?.mediaKey : null;
        const directPath = mediaType ? msg.message?.[mediaType]?.directPath : null;

        if (!mediaType || !mappedMediaType || !mediaKey || !directPath) {
          throw new MediaUnavailableError(
            422,
            directPath ? `https://mmg.whatsapp.net${directPath}` : undefined,
            'missing_media_descriptor',
          );
        }

        try {
          const media = await downloadContentFromMessage(
            {
              mediaKey,
              directPath,
              url: `https://mmg.whatsapp.net${directPath}`,
            },
            mappedMediaType,
            {},
          );
          const chunks = [];
          for await (const chunk of media) {
            chunks.push(chunk);
          }
          buffer = Buffer.concat(chunks);
          this.logger.info('Download Media with downloadContentFromMessage was successful!');
        } catch (fallbackErr) {
          const mediaUnavailableError = this.mediaUnavailableError(fallbackErr);
          if (mediaUnavailableError) {
            this.logger.warn(
              `Historical media fallback unavailable with status ${mediaUnavailableError.statusCode}; giving up`,
            );
            throw mediaUnavailableError;
          }
          this.logger.error('Download Media with downloadContentFromMessage also failed!');
          throw fallbackErr;
        }
      }
      const typeMessage = getContentType(msg.message);

      const ext = mimeTypes.extension(mediaMessage?.['mimetype']);
      const fileName = mediaMessage?.['fileName'] || `${msg.key.id}.${ext}` || `${v4()}.${ext}`;

      if (convertToMp4 && typeMessage === 'audioMessage') {
        try {
          const convert = await this.processAudioMp4(buffer.toString('base64'));

          if (Buffer.isBuffer(convert)) {
            const result = {
              mediaType,
              fileName,
              caption: mediaMessage['caption'],
              size: {
                fileLength: mediaMessage['fileLength'],
                height: mediaMessage['height'],
                width: mediaMessage['width'],
              },
              mimetype: 'audio/mp4',
              base64: convert.toString('base64'),
              buffer: getBuffer ? convert : null,
            };

            return result;
          }
        } catch (error) {
          this.logger.error('Error converting audio to mp4:');
          this.logger.error(error);
          throw new BadRequestException('Failed to convert audio to MP4');
        }
      }

      return {
        mediaType,
        fileName,
        caption: mediaMessage['caption'],
        size: { fileLength: mediaMessage['fileLength'], height: mediaMessage['height'], width: mediaMessage['width'] },
        mimetype: mediaMessage['mimetype'],
        base64: buffer.toString('base64'),
        buffer: getBuffer ? buffer : null,
      };
    } catch (error) {
      if (error instanceof MediaUnavailableError) {
        if (getBuffer) {
          this.logger.warn(`Skipping media upload because the historical file is unavailable (${error.statusCode})`);
          return null;
        }

        throw error;
      }

      this.logger.error('Error processing media message:');
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  private mediaUnavailableError(error: unknown) {
    const statusCode = this.mediaFetchStatusCode(error);
    if (![403, 404, 410].includes(statusCode || 0)) {
      return null;
    }

    return new MediaUnavailableError(statusCode!, this.mediaFetchUrl(error));
  }

  private mediaFetchStatusCode(error: unknown) {
    const candidate = error as {
      output?: { statusCode?: number };
      statusCode?: number;
      status?: number;
      response?: { status?: number };
    };

    return (
      candidate?.output?.statusCode || candidate?.response?.status || candidate?.statusCode || candidate?.status || null
    );
  }

  private mediaFetchUrl(error: unknown) {
    const candidate = error as {
      data?: { url?: string };
      output?: { payload?: { message?: string } };
    };

    return candidate?.data?.url || candidate?.output?.payload?.message;
  }

  public async fetchPrivacySettings() {
    const privacy = await this.client.fetchPrivacySettings();

    return {
      readreceipts: privacy.readreceipts,
      profile: privacy.profile,
      status: privacy.status,
      online: privacy.online,
      last: privacy.last,
      groupadd: privacy.groupadd,
    };
  }

  public async updatePrivacySettings(settings: PrivacySettingDto) {
    try {
      await this.client.updateReadReceiptsPrivacy(settings.readreceipts);
      await this.client.updateProfilePicturePrivacy(settings.profile);
      await this.client.updateStatusPrivacy(settings.status);
      await this.client.updateOnlinePrivacy(settings.online);
      await this.client.updateLastSeenPrivacy(settings.last);
      await this.client.updateGroupsAddPrivacy(settings.groupadd);

      this.reloadConnection();

      return {
        update: 'success',
        data: {
          readreceipts: settings.readreceipts,
          profile: settings.profile,
          status: settings.status,
          online: settings.online,
          last: settings.last,
          groupadd: settings.groupadd,
        },
      };
    } catch (error) {
      throw new InternalServerErrorException('Error updating privacy settings', error.toString());
    }
  }

  public async fetchBusinessProfile(number: string): Promise<NumberBusiness> {
    try {
      const jid = number ? createJid(number) : this.instance.wuid;

      const profile = await this.client.getBusinessProfile(jid);

      if (!profile) {
        const info = await this.whatsappNumber({ numbers: [jid] });

        return { isBusiness: false, message: 'Not is business profile', ...info?.shift() };
      }

      return { isBusiness: true, ...profile };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile name', error.toString());
    }
  }

  public async updateProfileName(name: string) {
    try {
      await this.client.updateProfileName(name);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile name', error.toString());
    }
  }

  public async updateProfileStatus(status: string) {
    try {
      await this.client.updateProfileStatus(status);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile status', error.toString());
    }
  }

  public async updateProfilePicture(picture: string) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(picture);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        pic = (await axios.get(url, config)).data;
      } else if (isBase64(picture)) {
        pic = Buffer.from(picture, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }

      await this.client.updateProfilePicture(this.instance.wuid, pic);

      this.reloadConnection();

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile picture', error.toString());
    }
  }

  public async removeProfilePicture() {
    try {
      await this.client.removeProfilePicture(this.instance.wuid);

      this.reloadConnection();

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error removing profile picture', error.toString());
    }
  }

  public async blockUser(data: BlockUserDto) {
    try {
      const { number } = data;

      const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

      if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
        throw new BadRequestException(isWA);
      }

      const sender = isWA.jid;

      await this.client.updateBlockStatus(sender, data.status);

      return { block: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error blocking user', error.toString());
    }
  }

  private async formatUpdateMessage(data: UpdateMessageDto) {
    try {
      if (!this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        return data;
      }

      const msg: any = await this.getMessage(data.key, true);

      if (msg?.messageType === 'conversation' || msg?.messageType === 'extendedTextMessage') {
        return { text: data.text };
      }

      if (msg?.messageType === 'imageMessage') {
        return { image: msg?.message?.imageMessage, caption: data.text };
      }

      if (msg?.messageType === 'videoMessage') {
        return { video: msg?.message?.videoMessage, caption: data.text };
      }

      return null;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  public async updateMessage(data: UpdateMessageDto) {
    const jid = createJid(data.number);

    const options = await this.formatUpdateMessage(data);

    if (!options) {
      this.logger.error('Message not compatible');
      throw new BadRequestException('Message not compatible');
    }

    try {
      const oldMessage: any = await this.getMessage(data.key, true);
      if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        if (!oldMessage) throw new NotFoundException('Message not found');
        if (oldMessage?.key?.remoteJid !== jid) {
          throw new BadRequestException('RemoteJid does not match');
        }
        const messageTimestamp = Number(oldMessage?.messageTimestamp || 0);
        const messageTimestampMs = messageTimestamp > 1000000000000 ? messageTimestamp : messageTimestamp * 1000;
        if (messageTimestampMs > 0 && Date.now() - messageTimestampMs > 900000) {
          // 15 minutes in milliseconds
          throw new BadRequestException('Message is older than 15 minutes');
        }
      }

      const messageSent = await this.client.sendMessage(jid, { ...(options as any), edit: data.key });
      if (messageSent) {
        const editedMessage =
          messageSent?.message?.protocolMessage || messageSent?.message?.editedMessage?.message?.protocolMessage;

        if (editedMessage) {
          this.sendDataWebhook(Events.SEND_MESSAGE_UPDATE, editedMessage);
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
            this.chatwootService.eventWhatsapp(
              'send.message.update',
              { instanceName: this.instance.name, instanceId: this.instance.id },
              editedMessage,
            );

          const messageId = messageSent.message?.protocolMessage?.key?.id;
          if (messageId && this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            let message = await this.prismaRepository.message.findFirst({
              where: { key: { path: ['id'], equals: messageId } },
            });
            if (!message) throw new NotFoundException('Message not found');

            if (!(message.key.valueOf() as any).fromMe) {
              new BadRequestException('You cannot edit others messages');
            }
            if ((message.key.valueOf() as any)?.deleted) {
              new BadRequestException('You cannot edit deleted messages');
            }

            if (oldMessage.messageType === 'conversation' || oldMessage.messageType === 'extendedTextMessage') {
              oldMessage.message.conversation = data.text;
            } else {
              oldMessage.message[oldMessage.messageType].caption = data.text;
            }
            message = await this.prismaRepository.message.update({
              where: { id: message.id },
              data: {
                message: oldMessage.message,
                status: 'EDITED',
                messageTimestamp: Math.floor(Date.now() / 1000), // Convert to int32 by dividing by 1000 to get seconds
              },
            });

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              const messageUpdate: any = {
                messageId: message.id,
                keyId: messageId,
                remoteJid: messageSent.key.remoteJid,
                fromMe: messageSent.key.fromMe,
                participant: messageSent.key?.participant,
                status: 'EDITED',
                instanceId: this.instanceId,
              };
              await this.prismaRepository.messageUpdate.create({ data: messageUpdate });
            }
          }
        }
      }

      return messageSent;
    } catch (error) {
      this.logger.error(error);
      throw error;
    }
  }

  public async fetchLabels(): Promise<LabelDto[]> {
    const labels = await this.prismaRepository.label.findMany({ where: { instanceId: this.instanceId } });

    return labels.map((label) => ({
      color: label.color,
      name: label.name,
      id: label.labelId,
      predefinedId: label.predefinedId,
    }));
  }

  public async handleLabel(data: HandleLabelDto) {
    const whatsappContact = await this.whatsappNumber({ numbers: [data.number] });
    if (whatsappContact.length === 0) {
      throw new NotFoundException('Number not found');
    }
    const contact = whatsappContact[0];
    if (!contact.exists) {
      throw new NotFoundException('Number is not on WhatsApp');
    }

    try {
      if (data.action === 'add') {
        await this.client.addChatLabel(contact.jid, data.labelId);
        await this.addLabel(data.labelId, this.instanceId, contact.jid);

        return { numberJid: contact.jid, labelId: data.labelId, add: true };
      }
      if (data.action === 'remove') {
        await this.client.removeChatLabel(contact.jid, data.labelId);
        await this.removeLabel(data.labelId, this.instanceId, contact.jid);

        return { numberJid: contact.jid, labelId: data.labelId, remove: true };
      }
    } catch (error) {
      throw new BadRequestException(`Unable to ${data.action} label to chat`, error.toString());
    }
  }

  // Group
  private async updateGroupMetadataCache(groupJid: string) {
    try {
      const meta = await this.client.groupMetadata(groupJid);

      const cacheConf = this.configService.get<CacheConf>('CACHE');

      if ((cacheConf?.REDIS?.ENABLED && cacheConf?.REDIS?.URI !== '') || cacheConf?.LOCAL?.ENABLED) {
        this.logger.verbose(`Updating cache for group: ${groupJid}`);
        await groupMetadataCache.set(groupJid, { timestamp: Date.now(), data: meta });
      }

      return meta;
    } catch (error) {
      this.logger.error(error);
      return null;
    }
  }

  private getGroupMetadataCache = async (groupJid: string) => {
    if (!isJidGroup(groupJid)) return null;

    const cacheConf = this.configService.get<CacheConf>('CACHE');

    if ((cacheConf?.REDIS?.ENABLED && cacheConf?.REDIS?.URI !== '') || cacheConf?.LOCAL?.ENABLED) {
      if (await groupMetadataCache?.has(groupJid)) {
        this.logger.debug(`Group metadata cache hit for ${groupJid}`);
        const meta = await groupMetadataCache.get(groupJid);

        if (Date.now() - meta.timestamp > 3600000) {
          await this.updateGroupMetadataCache(groupJid);
        }

        return meta.data;
      }

      this.logger.debug(`Group metadata cache miss for ${groupJid}`);
      return await this.updateGroupMetadataCache(groupJid);
    }

    return await this.findGroup({ groupJid }, 'inner');
  };

  public async createGroup(create: CreateGroupDto) {
    try {
      const participants = (await this.whatsappNumber({ numbers: create.participants }))
        .filter((participant) => participant.exists)
        .map((participant) => participant.jid);
      const { id } = await this.client.groupCreate(create.subject, participants);

      if (create?.description) {
        await this.client.groupUpdateDescription(id, create.description);
      }

      if (create?.promoteParticipants) {
        await this.updateGParticipant({ groupJid: id, action: 'promote', participants: participants });
      }

      const group = await this.client.groupMetadata(id);

      return group;
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException('Error creating group', error.toString());
    }
  }

  public async updateGroupPicture(picture: GroupPictureDto) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture.image)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(picture.image);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        pic = (await axios.get(url, config)).data;
      } else if (isBase64(picture.image)) {
        pic = Buffer.from(picture.image, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }
      await this.client.updateProfilePicture(picture.groupJid, pic);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error update group picture', error.toString());
    }
  }

  public async updateGroupSubject(data: GroupSubjectDto) {
    try {
      await this.client.groupUpdateSubject(data.groupJid, data.subject);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating group subject', error.toString());
    }
  }

  public async updateGroupDescription(data: GroupDescriptionDto) {
    try {
      await this.client.groupUpdateDescription(data.groupJid, data.description);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating group description', error.toString());
    }
  }

  public async findGroup(id: GroupJid, reply: 'inner' | 'out' = 'out') {
    try {
      const group = await this.client.groupMetadata(id.groupJid);

      if (!group) {
        this.logger.error('Group not found');
        return null;
      }

      const picture = await this.profilePicture(group.id);

      return {
        id: group.id,
        subject: group.subject,
        subjectOwner: group.subjectOwner,
        subjectTime: group.subjectTime,
        pictureUrl: picture.profilePictureUrl,
        size: group.participants.length,
        creation: group.creation,
        owner: group.owner,
        desc: group.desc,
        descId: group.descId,
        restrict: group.restrict,
        announce: group.announce,
        participants: group.participants,
        isCommunity: group.isCommunity,
        isCommunityAnnounce: group.isCommunityAnnounce,
        linkedParent: group.linkedParent,
      };
    } catch (error) {
      if (reply === 'inner') {
        return;
      }
      throw new NotFoundException('Error fetching group', error.toString());
    }
  }

  public async fetchAllGroups(getParticipants: GetParticipant) {
    const fetch = Object.values(await this?.client?.groupFetchAllParticipating());

    let groups = [];
    for (const group of fetch) {
      const picture = await this.profilePicture(group.id);

      const result = {
        id: group.id,
        subject: group.subject,
        subjectOwner: group.subjectOwner,
        subjectTime: group.subjectTime,
        pictureUrl: picture?.profilePictureUrl,
        size: group.participants.length,
        creation: group.creation,
        owner: group.owner,
        desc: group.desc,
        descId: group.descId,
        restrict: group.restrict,
        announce: group.announce,
        isCommunity: group.isCommunity,
        isCommunityAnnounce: group.isCommunityAnnounce,
        linkedParent: group.linkedParent,
      };

      if (getParticipants.getParticipants == 'true') {
        result['participants'] = group.participants;
      }

      groups = [...groups, result];
    }

    return groups;
  }

  public async inviteCode(id: GroupJid) {
    try {
      const code = await this.client.groupInviteCode(id.groupJid);
      return { inviteUrl: `https://chat.whatsapp.com/${code}`, inviteCode: code };
    } catch (error) {
      throw new NotFoundException('No invite code', error.toString());
    }
  }

  public async inviteInfo(id: GroupInvite) {
    try {
      return await this.client.groupGetInviteInfo(id.inviteCode);
    } catch {
      throw new NotFoundException('No invite info', id.inviteCode);
    }
  }

  public async sendInvite(id: GroupSendInvite) {
    try {
      const inviteCode = await this.inviteCode({ groupJid: id.groupJid });

      const inviteUrl = inviteCode.inviteUrl;

      const numbers = id.numbers.map((number) => createJid(number));
      const description = id.description ?? '';

      const msg = `${description}\n\n${inviteUrl}`;

      const message = { conversation: msg };

      for await (const number of numbers) {
        await this.sendMessageWithTyping(number, message);
      }

      return { send: true, inviteUrl };
    } catch {
      throw new NotFoundException('No send invite');
    }
  }

  public async acceptInviteCode(id: AcceptGroupInvite) {
    try {
      const groupJid = await this.client.groupAcceptInvite(id.inviteCode);
      return { accepted: true, groupJid: groupJid };
    } catch (error) {
      throw new NotFoundException('Accept invite error', error.toString());
    }
  }

  public async revokeInviteCode(id: GroupJid) {
    try {
      const inviteCode = await this.client.groupRevokeInvite(id.groupJid);
      return { revoked: true, inviteCode };
    } catch (error) {
      throw new NotFoundException('Revoke error', error.toString());
    }
  }

  public async findParticipants(id: GroupJid) {
    try {
      const participants = (await this.client.groupMetadata(id.groupJid)).participants;

      const participantContexts = await mapWithConcurrencyLimit(
        participants as any[],
        CONTACT_PROFILE_LOOKUP_CONCURRENCY,
        async (participant: any) => {
          const resolution = await this.resolveCanonicalJidWithNative(participant.id, participant.lid, {
            phoneNumber: participant.phoneNumber,
            remoteLid: this.isLidJid(participant.id) ? participant.id : participant.lid,
          });

          return {
            participant,
            resolution,
            candidates: this.buildIdentityCandidates(
              resolution,
              participant.id,
              participant.phoneNumber ? createJid(participant.phoneNumber) : undefined,
              participant.lid,
            ),
          };
        },
      );

      const contactCandidates = [...new Set(participantContexts.flatMap((participant) => participant.candidates))];
      const contacts = contactCandidates.length
        ? await this.prismaRepository.contact.findMany({
            where: { instanceId: this.instanceId, remoteJid: { in: contactCandidates } },
          })
        : [];

      const contactByJid = new Map<string, (typeof contacts)[number]>(
        contacts.map((contact) => [contact.remoteJid, contact] as const),
      );

      const parsedParticipants = participantContexts.map(({ participant, resolution, candidates }) => {
        const contact =
          candidates.map((candidate) => contactByJid.get(candidate)).find((candidate) => !!candidate) ?? null;

        return {
          ...participant,
          canonicalJid: resolution.remoteJid,
          phoneNumber:
            participant.phoneNumber ??
            (!resolution.remoteJid || this.isLidJid(resolution.remoteJid)
              ? undefined
              : resolution.remoteJid.split('@')[0]),
          lid: participant.lid ?? resolution.remoteLid,
          name: this.pickPreferredName(participant.name, contact?.pushName),
          imgUrl: participant.imgUrl ?? contact?.profilePicUrl,
        };
      });

      const usersContacts = parsedParticipants
        .filter((participant) => participant.canonicalJid || participant.id)
        .map((participant) => ({
          remoteJid: participant.canonicalJid ?? participant.id,
          remoteJidAlt:
            participant.lid && participant.canonicalJid && participant.canonicalJid !== participant.lid
              ? participant.lid
              : undefined,
          lid: participant.lid ? ('lid' as const) : undefined,
        }));

      if (usersContacts.length > 0) {
        await saveOnWhatsappCache(usersContacts);
      }

      return { participants: parsedParticipants };
    } catch (error) {
      this.logger.error({ local: 'participants.find', error });
      throw new NotFoundException('No participants', error.toString());
    }
  }

  public async updateGParticipant(update: GroupUpdateParticipantDto) {
    try {
      const participants = update.participants.map((p) => createJid(p));
      const updateParticipants = await this.client.groupParticipantsUpdate(
        update.groupJid,
        participants,
        update.action,
      );
      return { updateParticipants: updateParticipants };
    } catch (error) {
      throw new BadRequestException('Error updating participants', error.toString());
    }
  }

  public async updateGSetting(update: GroupUpdateSettingDto) {
    try {
      const updateSetting = await this.client.groupSettingUpdate(update.groupJid, update.action);
      return { updateSetting: updateSetting };
    } catch (error) {
      throw new BadRequestException('Error updating setting', error.toString());
    }
  }

  public async toggleEphemeral(update: GroupToggleEphemeralDto) {
    try {
      await this.client.groupToggleEphemeral(update.groupJid, update.expiration);
      return { success: true };
    } catch (error) {
      throw new BadRequestException('Error updating setting', error.toString());
    }
  }

  public async leaveGroup(id: GroupJid) {
    try {
      await this.client.groupLeave(id.groupJid);
      return { groupJid: id.groupJid, leave: true };
    } catch (error) {
      throw new BadRequestException('Unable to leave the group', error.toString());
    }
  }

  public async templateMessage() {
    throw new Error('Method not available in the Baileys service');
  }

  private deserializeMessageBuffers(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }

    if (typeof obj === 'object' && !Array.isArray(obj) && !Buffer.isBuffer(obj)) {
      const keys = Object.keys(obj);
      const isIndexedObject = keys.every((key) => !isNaN(Number(key)));

      if (isIndexedObject && keys.length > 0) {
        const values = keys.sort((a, b) => Number(a) - Number(b)).map((key) => obj[key]);
        return new Uint8Array(values);
      }
    }

    // Is Buffer?, converter to Uint8Array
    if (Buffer.isBuffer(obj)) {
      return new Uint8Array(obj);
    }

    // Process arrays recursively
    if (Array.isArray(obj)) {
      return obj.map((item) => this.deserializeMessageBuffers(item));
    }

    // Process objects recursively
    if (typeof obj === 'object') {
      const converted: any = {};
      for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
          converted[key] = this.deserializeMessageBuffers(obj[key]);
        }
      }
      return converted;
    }

    return obj;
  }

  private prepareMessage(message: WAMessage): Message {
    const keyAny = message.key as any;
    this.applyCanonicalKeyIdentity(keyAny);
    const messageRaw: any = {
      key: {
        ...message.key,
        remoteJid: keyAny.remoteJid,
        remoteJidAlt: keyAny.remoteJidAlt,
        remoteLid: keyAny.remoteLid,
        addressingMode: keyAny.addressingMode,
        participant: keyAny.participant,
      },
      pushName:
        message.pushName ||
        (message.key.fromMe
          ? 'Вы'
          : message?.participant || (message.key?.participant ? message.key.participant.split('@')[0] : null)),
      message: this.deserializeMessageBuffers({ ...message.message }),
      messageType: getContentType(message.message),
      messageTimestamp: Long.isLong(message.messageTimestamp)
        ? message.messageTimestamp.toNumber()
        : (message.messageTimestamp as number),
      source: getDevice(keyAny.id),
      instanceId: this.instanceId,
      status: status[message.status],
      contextInfo: this.deserializeMessageBuffers(message.message?.messageContextInfo),
    };

    if (!messageRaw.status && message.key.fromMe === false) {
      messageRaw.status = status[3]; // DELIVERED MESSAGE
    }

    if (messageRaw.message.extendedTextMessage) {
      messageRaw.messageType = 'conversation';
      messageRaw.message.conversation = messageRaw.message.extendedTextMessage.text;
      delete messageRaw.message.extendedTextMessage;
    }

    if (messageRaw.message.documentWithCaptionMessage) {
      messageRaw.messageType = 'documentMessage';
      messageRaw.message.documentMessage = messageRaw.message.documentWithCaptionMessage.message.documentMessage;
      delete messageRaw.message.documentWithCaptionMessage;
    }

    const quotedMessage = messageRaw?.contextInfo?.quotedMessage;
    if (quotedMessage) {
      if (quotedMessage.extendedTextMessage) {
        quotedMessage.conversation = quotedMessage.extendedTextMessage.text;
        delete quotedMessage.extendedTextMessage;
      }

      if (quotedMessage.documentWithCaptionMessage) {
        quotedMessage.documentMessage = quotedMessage.documentWithCaptionMessage.message.documentMessage;
        delete quotedMessage.documentWithCaptionMessage;
      }
    }

    if (isJidNewsletter(message.key.remoteJid) && message.key.fromMe) {
      messageRaw.status = status[3]; // DELIVERED MESSAGE TO NEWSLETTER CHANNEL
    }

    return messageRaw;
  }

  private buildMessageForMediaPayload(messageRaw: any) {
    return {
      ...messageRaw,
      message: this.getInnerMediaMessage(messageRaw?.message),
    };
  }

  private getInnerMediaMessage(message: any) {
    if (!message) return message;

    if (message.forward?.message) {
      return message.forward.message;
    }

    if (message.ephemeralMessage?.message) {
      return message.ephemeralMessage.message;
    }

    return message;
  }

  private async prepareMessageWithNative(
    message: WAMessage,
    options: { phoneNumber?: string | null; remoteLid?: string | null } = {},
  ): Promise<Message> {
    const keyAny = message.key as any;
    await this.applyCanonicalKeyIdentityWithNative(keyAny, options);
    return this.prepareMessage(message);
  }

  private async syncChatwootLostMessages() {
    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      const chatwootConfig = await this.findChatwoot();
      const prepare = (message: any) => this.prepareMessage(message);
      this.chatwootService.syncLostMessages({ instanceName: this.instance.name }, chatwootConfig, prepare);

      // Generate ID for this cron task and store in cache
      const cronId = cuid();
      const cronKey = `chatwoot:syncLostMessages`;
      await this.chatwootService.getCache()?.hSet(cronKey, this.instance.name, cronId);

      const task = cron.schedule('0,30 * * * *', async () => {
        // Check ID before executing (only if cache is available)
        const cache = this.chatwootService.getCache();
        if (cache) {
          const storedId = await cache.hGet(cronKey, this.instance.name);
          if (storedId && storedId !== cronId) {
            this.logger.info(`Stopping syncChatwootLostMessages cron - ID mismatch: ${cronId} vs ${storedId}`);
            task.stop();
            return;
          }
        }
        this.chatwootService.syncLostMessages({ instanceName: this.instance.name }, chatwootConfig, prepare);
      });
      task.start();
    }
  }

  private async updateMessagesReadedByTimestamp(remoteJid: string, timestamp?: number): Promise<number> {
    if (timestamp === undefined || timestamp === null) return 0;

    const provider = this.configService.get<Database>('DATABASE').PROVIDER;
    let result: number;

    if (provider === 'mysql') {
      // MySQL version
      result = await this.prismaRepository.$executeRaw`
        UPDATE Message
        SET status = ${status[4]}
        WHERE instanceId = ${this.instanceId}
        AND JSON_UNQUOTE(JSON_EXTRACT(\`key\`, '$.remoteJid')) = ${remoteJid}
        AND JSON_UNQUOTE(JSON_EXTRACT(\`key\`, '$.fromMe')) = 'false'
        AND messageTimestamp <= ${timestamp}
        AND (status IS NULL OR status = ${status[3]})
      `;
    } else {
      // PostgreSQL version
      result = await this.prismaRepository.$executeRaw`
        UPDATE "Message"
        SET "status" = ${status[4]}
        WHERE "instanceId" = ${this.instanceId}
        AND "key"->>'remoteJid' = ${remoteJid}
        AND ("key"->>'fromMe')::boolean = false
        AND "messageTimestamp" <= ${timestamp}
        AND ("status" IS NULL OR "status" = ${status[3]})
      `;
    }

    if (result) {
      if (result > 0) {
        this.updateChatUnreadMessages(remoteJid);
      }

      return result;
    }

    return 0;
  }

  private async updateChatUnreadMessages(remoteJid: string): Promise<number> {
    const provider = this.configService.get<Database>('DATABASE').PROVIDER;

    let unreadMessagesPromise: Promise<number>;

    if (provider === 'mysql') {
      // MySQL version
      unreadMessagesPromise = this.prismaRepository.$queryRaw`
        SELECT COUNT(*) as count FROM Message
        WHERE instanceId = ${this.instanceId}
        AND JSON_UNQUOTE(JSON_EXTRACT(\`key\`, '$.remoteJid')) = ${remoteJid}
        AND JSON_UNQUOTE(JSON_EXTRACT(\`key\`, '$.fromMe')) = 'false'
        AND status = ${status[3]}
      `.then((result: any[]) => Number(result[0]?.count) || 0);
    } else {
      // PostgreSQL version
      unreadMessagesPromise = this.prismaRepository.$queryRaw`
        SELECT COUNT(*)::int as count FROM "Message"
        WHERE "instanceId" = ${this.instanceId}
        AND "key"->>'remoteJid' = ${remoteJid}
        AND ("key"->>'fromMe')::boolean = false
        AND "status" = ${status[3]}
      `.then((result: any[]) => result[0]?.count || 0);
    }

    const [chat, unreadMessages] = await Promise.all([
      this.prismaRepository.chat.findFirst({ where: { instanceId: this.instanceId, remoteJid } }),
      unreadMessagesPromise,
    ]);

    if (chat && chat.unreadMessages !== unreadMessages) {
      await this.prismaRepository.chat.update({ where: { id: chat.id }, data: { unreadMessages } });
    }

    return unreadMessages;
  }

  private async addLabel(labelId: string, instanceId: string, chatId: string) {
    const id = cuid();
    const provider = this.configService.get<Database>('DATABASE').PROVIDER;

    if (provider === 'mysql') {
      // MySQL version - use INSERT ... ON DUPLICATE KEY UPDATE
      await this.prismaRepository.$executeRawUnsafe(
        `INSERT INTO Chat (id, instanceId, remoteJid, labels, createdAt, updatedAt)
         VALUES (?, ?, ?, JSON_ARRAY(?), NOW(), NOW())
         ON DUPLICATE KEY UPDATE
           labels = JSON_ARRAY_APPEND(
             COALESCE(labels, JSON_ARRAY()),
             '$',
             ?
           ),
           updatedAt = NOW()`,
        id,
        instanceId,
        chatId,
        labelId,
        labelId,
      );
    } else {
      // PostgreSQL version
      await this.prismaRepository.$executeRawUnsafe(
        `INSERT INTO "Chat" ("id", "instanceId", "remoteJid", "labels", "createdAt", "updatedAt")
         VALUES ($4, $2, $3, to_jsonb(ARRAY[$1]::text[]), NOW(), NOW()) ON CONFLICT ("instanceId", "remoteJid")
       DO
        UPDATE
            SET "labels" = (
            SELECT to_jsonb(array_agg(DISTINCT elem))
            FROM (
            SELECT jsonb_array_elements_text("Chat"."labels") AS elem
            UNION
            SELECT $1::text AS elem
            ) sub
            ),
            "updatedAt" = NOW();`,
        labelId,
        instanceId,
        chatId,
        id,
      );
    }
  }

  private async removeLabel(labelId: string, instanceId: string, chatId: string) {
    const id = cuid();
    const provider = this.configService.get<Database>('DATABASE').PROVIDER;

    if (provider === 'mysql') {
      // MySQL version - use INSERT ... ON DUPLICATE KEY UPDATE
      await this.prismaRepository.$executeRawUnsafe(
        `INSERT INTO Chat (id, instanceId, remoteJid, labels, createdAt, updatedAt)
         VALUES (?, ?, ?, JSON_ARRAY(), NOW(), NOW())
         ON DUPLICATE KEY UPDATE
           labels = COALESCE(
             JSON_REMOVE(
               labels,
               JSON_UNQUOTE(JSON_SEARCH(labels, 'one', ?))
             ),
             JSON_ARRAY()
           ),
           updatedAt = NOW()`,
        id,
        instanceId,
        chatId,
        labelId,
      );
    } else {
      // PostgreSQL version
      await this.prismaRepository.$executeRawUnsafe(
        `INSERT INTO "Chat" ("id", "instanceId", "remoteJid", "labels", "createdAt", "updatedAt")
         VALUES ($4, $2, $3, '[]'::jsonb, NOW(), NOW()) ON CONFLICT ("instanceId", "remoteJid")
       DO
        UPDATE
            SET "labels" = COALESCE (
            (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements_text("Chat"."labels") AS elem
            WHERE elem <> $1
            ),
            '[]'::jsonb
            ),
            "updatedAt" = NOW();`,
        labelId,
        instanceId,
        chatId,
        id,
      );
    }
  }

  public async baileysOnWhatsapp(jid: string) {
    const response = await this.client.onWhatsApp(jid);

    return response;
  }

  public async baileysProfilePictureUrl(jid: string, type: 'image' | 'preview', timeoutMs: number) {
    const response = await this.client.profilePictureUrl(jid, type, timeoutMs);

    return response;
  }

  public async baileysAssertSessions(jids: string[]) {
    const response = await this.client.assertSessions(jids);

    return response;
  }

  public async baileysCreateParticipantNodes(jids: string[], message: proto.IMessage, extraAttrs: any) {
    const response = await this.client.createParticipantNodes(jids, message, extraAttrs);

    const convertedResponse = {
      ...response,
      nodes: response.nodes.map((node: any) => ({
        ...node,
        content: node.content?.map((c: any) => ({
          ...c,
          content: c.content instanceof Uint8Array ? Buffer.from(c.content).toString('base64') : c.content,
        })),
      })),
    };

    return convertedResponse;
  }

  public async baileysSendNode(stanza: any) {
    this.logger.debug({ local: 'baileys.sendNode', stanza });
    const response = await this.client.sendNode(stanza);

    return response;
  }

  public async baileysGetUSyncDevices(jids: string[], useCache: boolean, ignoreZeroDevices: boolean) {
    const response = await this.client.getUSyncDevices(jids, useCache, ignoreZeroDevices);

    return response;
  }

  public async baileysGenerateMessageTag() {
    const response = await this.client.generateMessageTag();

    return response;
  }

  public async baileysSignalRepositoryDecryptMessage(jid: string, type: 'pkmsg' | 'msg', ciphertext: string) {
    try {
      const ciphertextBuffer = Buffer.from(ciphertext, 'base64');

      const response = await this.client.signalRepository.decryptMessage({ jid, type, ciphertext: ciphertextBuffer });

      return response instanceof Uint8Array ? Buffer.from(response).toString('base64') : response;
    } catch (error) {
      this.logger.error('Error decrypting message:');
      this.logger.error(error);
      throw error;
    }
  }

  public async baileysGetAuthState() {
    const response = { me: this.client.authState.creds.me, account: this.client.authState.creds.account };

    return response;
  }

  //Business Controller
  public async fetchCatalog(instanceName: string, data: getCollectionsDto) {
    const jid = data.number ? createJid(data.number) : this.client?.user?.id;
    const limit = data.limit || 10;
    const cursor = null;

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
      const business = await this.fetchBusinessProfile(info?.jid);

      let catalog = await this.getCatalog({ jid: info?.jid, limit, cursor });
      let nextPageCursor = catalog.nextPageCursor;
      let nextPageCursorJson = nextPageCursor ? JSON.parse(atob(nextPageCursor)) : null;
      let pagination = nextPageCursorJson?.pagination_cursor
        ? JSON.parse(atob(nextPageCursorJson.pagination_cursor))
        : null;
      let fetcherHasMore = pagination?.fetcher_has_more === true ? true : false;

      let productsCatalog = catalog.products || [];
      let countLoops = 0;
      while (fetcherHasMore && countLoops < 4) {
        catalog = await this.getCatalog({ jid: info?.jid, limit, cursor: nextPageCursor });
        nextPageCursor = catalog.nextPageCursor;
        nextPageCursorJson = nextPageCursor ? JSON.parse(atob(nextPageCursor)) : null;
        pagination = nextPageCursorJson?.pagination_cursor
          ? JSON.parse(atob(nextPageCursorJson.pagination_cursor))
          : null;
        fetcherHasMore = pagination?.fetcher_has_more === true ? true : false;
        productsCatalog = [...productsCatalog, ...catalog.products];
        countLoops++;
      }

      return {
        wuid: info?.jid || jid,
        numberExists: info?.exists,
        isBusiness: business.isBusiness,
        catalogLength: productsCatalog.length,
        catalog: productsCatalog,
      };
    } catch (error) {
      this.logger.warn({ local: 'fetchCatalog', error });
      return { wuid: jid, name: null, isBusiness: false };
    }
  }

  public async getCatalog({
    jid,
    limit,
    cursor,
  }: GetCatalogOptions): Promise<{ products: Product[]; nextPageCursor: string | undefined }> {
    try {
      jid = jid ? createJid(jid) : this.instance.wuid;

      const catalog = await this.client.getCatalog({ jid, limit: limit, cursor: cursor });

      if (!catalog) {
        return { products: undefined, nextPageCursor: undefined };
      }

      return catalog;
    } catch (error) {
      throw new InternalServerErrorException('Error getCatalog', error.toString());
    }
  }

  public async fetchCollections(instanceName: string, data: getCollectionsDto) {
    const jid = data.number ? createJid(data.number) : this.client?.user?.id;
    const limit = data.limit <= 20 ? data.limit : 20; //(tem esse limite, não sei porque)

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
      const business = await this.fetchBusinessProfile(info?.jid);
      const collections = await this.getCollections(info?.jid, limit);

      return {
        wuid: info?.jid || jid,
        name: info?.name,
        numberExists: info?.exists,
        isBusiness: business.isBusiness,
        collectionsLength: collections?.length,
        collections: collections,
      };
    } catch {
      return { wuid: jid, name: null, isBusiness: false };
    }
  }

  public async getCollections(jid?: string | undefined, limit?: number): Promise<CatalogCollection[]> {
    try {
      jid = jid ? createJid(jid) : this.instance.wuid;

      const result = await this.client.getCollections(jid, limit);

      if (!result) {
        return [{ id: undefined, name: undefined, products: [], status: undefined }];
      }

      return result.collections;
    } catch (error) {
      throw new InternalServerErrorException('Error getCatalog', error.toString());
    }
  }

  public async fetchMessages(query: Query<Message>) {
    const keyFilters = query?.where?.key as ExtendedIMessageKey;

    const timestampFilter = {};
    if (query?.where?.messageTimestamp) {
      if (query.where.messageTimestamp['gte'] && query.where.messageTimestamp['lte']) {
        timestampFilter['messageTimestamp'] = {
          gte: Math.floor(new Date(query.where.messageTimestamp['gte']).getTime() / 1000),
          lte: Math.floor(new Date(query.where.messageTimestamp['lte']).getTime() / 1000),
        };
      }
    }

    const count = await this.prismaRepository.message.count({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          typeof keyFilters?.fromMe === 'boolean' ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.participant || keyFilters?.participantAlt
            ? {
                OR: [
                  keyFilters?.participant ? { key: { path: ['participant'], equals: keyFilters?.participant } } : {},
                  keyFilters?.participantAlt
                    ? { key: { path: ['participantAlt'], equals: keyFilters?.participantAlt } }
                    : {},
                ],
              }
            : {},
          {
            OR: [
              keyFilters?.remoteJid ? { key: { path: ['remoteJid'], equals: keyFilters?.remoteJid } } : {},
              keyFilters?.remoteJidAlt ? { key: { path: ['remoteJidAlt'], equals: keyFilters?.remoteJidAlt } } : {},
            ],
          },
        ],
      },
    });

    if (!query?.offset) {
      query.offset = 50;
    }

    if (!query?.page) {
      query.page = 1;
    }

    const messages = await this.prismaRepository.message.findMany({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          typeof keyFilters?.fromMe === 'boolean' ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.participant || keyFilters?.participantAlt
            ? {
                OR: [
                  keyFilters?.participant ? { key: { path: ['participant'], equals: keyFilters?.participant } } : {},
                  keyFilters?.participantAlt
                    ? { key: { path: ['participantAlt'], equals: keyFilters?.participantAlt } }
                    : {},
                ],
              }
            : {},
          {
            OR: [
              keyFilters?.remoteJid ? { key: { path: ['remoteJid'], equals: keyFilters?.remoteJid } } : {},
              keyFilters?.remoteJidAlt ? { key: { path: ['remoteJidAlt'], equals: keyFilters?.remoteJidAlt } } : {},
            ],
          },
        ],
      },
      orderBy: { messageTimestamp: 'desc' },
      skip: query.offset * (query?.page === 1 ? 0 : (query?.page as number) - 1),
      take: query.offset,
      select: {
        id: true,
        key: true,
        pushName: true,
        messageType: true,
        message: true,
        messageTimestamp: true,
        instanceId: true,
        source: true,
        contextInfo: true,
        MessageUpdate: { select: { status: true } },
      },
    });

    const formattedMessages = messages.map((message) => {
      const messageKey = message.key as { fromMe: boolean; remoteJid: string; id: string; participant?: string };

      if (!message.pushName) {
        if (messageKey.fromMe) {
          message.pushName = 'Вы';
        } else if (message.contextInfo) {
          const contextInfo = message.contextInfo as { participant?: string };
          if (contextInfo.participant) {
            message.pushName = contextInfo.participant.split('@')[0];
          } else if (messageKey.participant) {
            message.pushName = messageKey.participant.split('@')[0];
          }
        }
      }

      return message;
    });

    return {
      messages: {
        total: count,
        pages: Math.ceil(count / query.offset),
        currentPage: query.page,
        records: formattedMessages,
      },
    };
  }

  public async baileysDecryptPollVote(pollCreationMessageKey: proto.IMessageKey) {
    try {
      this.logger.verbose('Starting poll vote decryption process');

      // Buscar a mensagem de criação da enquete
      const pollCreationMessage = (await this.getMessage(pollCreationMessageKey, true)) as proto.IWebMessageInfo;

      if (!pollCreationMessage) {
        throw new NotFoundException('Poll creation message not found');
      }

      // Extrair opções da enquete
      const pollOptions =
        (pollCreationMessage.message as any)?.pollCreationMessage?.options ||
        (pollCreationMessage.message as any)?.pollCreationMessageV3?.options ||
        [];

      if (!pollOptions || pollOptions.length === 0) {
        throw new NotFoundException('Poll options not found');
      }

      // Recuperar chave de criptografia
      const pollMessageSecret = (await this.getMessage(pollCreationMessageKey)) as any;
      let pollEncKey = pollMessageSecret?.messageContextInfo?.messageSecret;

      if (!pollEncKey) {
        throw new NotFoundException('Poll encryption key not found');
      }

      // Normalizar chave de criptografia
      if (typeof pollEncKey === 'string') {
        pollEncKey = Buffer.from(pollEncKey, 'base64');
      } else if (pollEncKey?.type === 'Buffer' && Array.isArray(pollEncKey.data)) {
        pollEncKey = Buffer.from(pollEncKey.data);
      }

      if (Buffer.isBuffer(pollEncKey) && pollEncKey.length === 44) {
        pollEncKey = Buffer.from(pollEncKey.toString('utf8'), 'base64');
      }

      // Buscar todas as mensagens de atualização de votos
      const allPollUpdateMessages = await this.prismaRepository.message.findMany({
        where: {
          instanceId: this.instanceId,
          messageType: 'pollUpdateMessage',
        },
        select: {
          id: true,
          key: true,
          message: true,
          messageTimestamp: true,
        },
      });

      this.logger.verbose(`Found ${allPollUpdateMessages.length} pollUpdateMessage messages in database`);

      // Filtrar apenas mensagens relacionadas a esta enquete específica
      const pollUpdateMessages = allPollUpdateMessages.filter((msg) => {
        const pollUpdate = (msg.message as any)?.pollUpdateMessage;
        if (!pollUpdate) return false;

        const creationKey = pollUpdate.pollCreationMessageKey;
        if (!creationKey) return false;

        return (
          creationKey.id === pollCreationMessageKey.id &&
          jidNormalizedUser(creationKey.remoteJid || '') === jidNormalizedUser(pollCreationMessageKey.remoteJid || '')
        );
      });

      this.logger.verbose(`Filtered to ${pollUpdateMessages.length} matching poll update messages`);

      // Preparar candidatos de JID para descriptografia
      const creatorCandidates = [
        this.instance.wuid,
        this.client.user?.lid,
        pollCreationMessage.key.participant,
        (pollCreationMessage.key as any).participantAlt,
        pollCreationMessage.key.remoteJid,
        (pollCreationMessage.key as any).remoteJidAlt,
      ].filter(Boolean);

      const uniqueCreators = [...new Set(creatorCandidates.map((id) => jidNormalizedUser(id)))];

      // Processar votos
      const votesByUser = new Map<string, { timestamp: number; selectedOptions: string[]; voterJid: string }>();

      this.logger.verbose(`Processing ${pollUpdateMessages.length} poll update messages for decryption`);

      for (const pollUpdateMsg of pollUpdateMessages) {
        const pollVote = (pollUpdateMsg.message as any)?.pollUpdateMessage?.vote;
        if (!pollVote) continue;

        const key = pollUpdateMsg.key as any;
        const voterCandidates = [
          this.instance.wuid,
          this.client.user?.lid,
          key.participant,
          key.participantAlt,
          key.remoteJidAlt,
          key.remoteJid,
        ].filter(Boolean);

        const uniqueVoters = [...new Set(voterCandidates.map((id) => jidNormalizedUser(id)))];

        let selectedOptionNames: string[] = [];
        let successfulVoterJid: string | undefined;

        // Verificar se o voto já está descriptografado
        if (pollVote.selectedOptions && Array.isArray(pollVote.selectedOptions)) {
          const selectedOptions = pollVote.selectedOptions;
          this.logger.verbose('Vote already has selectedOptions, checking format');

          // Verificar se são strings (já descriptografado) ou buffers (precisa descriptografar)
          if (selectedOptions.length > 0 && typeof selectedOptions[0] === 'string') {
            // Já está descriptografado como nomes de opções
            selectedOptionNames = selectedOptions;
            successfulVoterJid = uniqueVoters[0];
            this.logger.verbose(
              `Using already decrypted vote: voter=${successfulVoterJid}, options=${selectedOptionNames.join(',')}`,
            );
          } else {
            // Está como hash, precisa converter para nomes
            selectedOptionNames = pollOptions
              .filter((option: any) => {
                const hash = createHash('sha256').update(option.optionName).digest();
                return selectedOptions.some((selected: any) => {
                  if (Buffer.isBuffer(selected)) {
                    return Buffer.compare(selected, hash) === 0;
                  }
                  return false;
                });
              })
              .map((option: any) => option.optionName);
            successfulVoterJid = uniqueVoters[0];
          }
        } else if (pollVote.encPayload && pollEncKey) {
          // Tentar descriptografar
          let decryptedVote: any = null;

          for (const creator of uniqueCreators) {
            for (const voter of uniqueVoters) {
              try {
                decryptedVote = decryptPollVote(pollVote, {
                  pollCreatorJid: creator,
                  pollMsgId: pollCreationMessage.key.id,
                  pollEncKey,
                  voterJid: voter,
                } as any);

                if (decryptedVote) {
                  successfulVoterJid = voter;
                  break;
                }
              } catch {
                // Continue tentando outras combinações
              }
            }
            if (decryptedVote) break;
          }

          if (decryptedVote && decryptedVote.selectedOptions) {
            // Converter hashes para nomes de opções
            selectedOptionNames = pollOptions
              .filter((option: any) => {
                const hash = createHash('sha256').update(option.optionName).digest();
                return decryptedVote.selectedOptions.some((selected: any) => {
                  if (Buffer.isBuffer(selected)) {
                    return Buffer.compare(selected, hash) === 0;
                  }
                  return false;
                });
              })
              .map((option: any) => option.optionName);

            this.logger.verbose(
              `Successfully decrypted vote for voter: ${successfulVoterJid}, creator: ${uniqueCreators[0]}`,
            );
          } else {
            this.logger.warn(`Failed to decrypt vote. Last error: Could not decrypt with any combination`);
            continue;
          }
        } else {
          this.logger.warn('Vote has no encPayload and no selectedOptions, skipping');
          continue;
        }

        if (selectedOptionNames.length > 0 && successfulVoterJid) {
          const normalizedVoterJid = jidNormalizedUser(successfulVoterJid);
          const existingVote = votesByUser.get(normalizedVoterJid);

          // Manter apenas o voto mais recente de cada usuário
          if (!existingVote || pollUpdateMsg.messageTimestamp > existingVote.timestamp) {
            votesByUser.set(normalizedVoterJid, {
              timestamp: pollUpdateMsg.messageTimestamp,
              selectedOptions: selectedOptionNames,
              voterJid: successfulVoterJid,
            });
          }
        }
      }

      // Agrupar votos por opção
      const results: Record<string, { votes: number; voters: string[] }> = {};

      // Inicializar todas as opções com zero votos
      pollOptions.forEach((option: any) => {
        results[option.optionName] = {
          votes: 0,
          voters: [],
        };
      });

      // Agregar votos
      votesByUser.forEach((voteData) => {
        voteData.selectedOptions.forEach((optionName) => {
          if (results[optionName]) {
            results[optionName].votes++;
            if (!results[optionName].voters.includes(voteData.voterJid)) {
              results[optionName].voters.push(voteData.voterJid);
            }
          }
        });
      });

      // Obter nome da enquete
      const pollName =
        (pollCreationMessage.message as any)?.pollCreationMessage?.name ||
        (pollCreationMessage.message as any)?.pollCreationMessageV3?.name ||
        'Enquete sem nome';

      // Calcular total de votos únicos
      const totalVotes = votesByUser.size;

      return {
        poll: {
          name: pollName,
          totalVotes,
          results,
        },
      };
    } catch (error) {
      this.logger.error(`Error decrypting poll votes: ${error}`);
      throw new InternalServerErrorException('Error decrypting poll votes', error.toString());
    }
  }

  public async fetchChannels(query: Query<Contact>) {
    const page = Number((query as any)?.page ?? 1);
    const limit = Number((query as any)?.limit ?? (query as any)?.rows ?? 50);
    const skip = (page - 1) * limit;

    const messages = await this.prismaRepository.message.findMany({
      where: {
        instanceId: this.instanceId,
        AND: [{ key: { path: ['remoteJid'], not: null } }],
      },
      orderBy: { messageTimestamp: 'desc' },
      select: {
        key: true,
        messageTimestamp: true,
      },
    });

    const channelMap = new Map<string, { remoteJid: string; pushName: undefined; lastMessageTimestamp: number }>();

    for (const msg of messages) {
      const key = msg.key as any;
      const remoteJid = key?.remoteJid as string | undefined;
      if (!remoteJid || !isJidNewsletter(remoteJid)) continue;

      if (!channelMap.has(remoteJid)) {
        channelMap.set(remoteJid, {
          remoteJid,
          pushName: undefined, // Push name is never stored for channels, so we set it as undefined
          lastMessageTimestamp: msg.messageTimestamp,
        });
      }
    }

    const allChannels = Array.from(channelMap.values());

    const total = allChannels.length;
    const pages = Math.ceil(total / limit);
    const records = allChannels.slice(skip, skip + limit);

    return {
      total,
      pages,
      currentPage: page,
      limit,
      records,
    };
  }
}
