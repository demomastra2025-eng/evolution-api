import { InstanceDto, SetPresenceDto } from '@api/dto/instance.dto';
import { ChatwootService } from '@api/integrations/chatbot/chatwoot/services/chatwoot.service';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository } from '@api/repository/repository.service';
import { channelController, eventManager } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { SettingsService } from '@api/services/settings.service';
import { Events, Integration, wa } from '@api/types/wa.types';
import { Auth, Chatwoot, ConfigService, HttpServer, WaBusiness } from '@config/env.config';
import { Logger } from '@config/logger.config';
import {
  BadRequestException,
  InternalServerErrorException,
  NotFoundException,
  UnauthorizedException,
} from '@exceptions';
import { delay } from 'baileys';
import { isArray, isURL } from 'class-validator';
import EventEmitter2 from 'eventemitter2';
import { v4 } from 'uuid';

import { ProxyController } from './proxy.controller';

export class InstanceController {
  constructor(
    private readonly waMonitor: WAMonitoringService,
    private readonly configService: ConfigService,
    private readonly prismaRepository: PrismaRepository,
    private readonly eventEmitter: EventEmitter2,
    private readonly chatwootService: ChatwootService,
    private readonly settingsService: SettingsService,
    private readonly proxyService: ProxyController,
    private readonly cache: CacheService,
    private readonly chatwootCache: CacheService,
    private readonly baileysCache: CacheService,
    private readonly providerFiles: ProviderFiles,
  ) {}

  private readonly logger = new Logger('InstanceController');
  private static readonly CONNECT_OUTCOME_TIMEOUT_MS = 12000;
  private static readonly CONNECT_OUTCOME_POLL_MS = 500;
  private readonly reauthorizationInFlight = new Map<string, Promise<unknown>>();
  private readonly lifecycleOperations = new Map<string, Promise<void>>();

  private async enqueueLifecycleOperation<T>(instanceName: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.lifecycleOperations.get(instanceName) ?? Promise.resolve();
    const result = previous.catch(() => undefined).then(operation);
    const settled = result.then(
      () => undefined,
      () => undefined,
    );

    this.lifecycleOperations.set(instanceName, settled);

    try {
      return await result;
    } finally {
      if (this.lifecycleOperations.get(instanceName) === settled) {
        this.lifecycleOperations.delete(instanceName);
      }
    }
  }

  private async cleanupOrphanedRuntime(instanceName: string) {
    this.logger.warn({
      message: 'Cleaning orphaned runtime instance without persisted record',
      instanceName,
    });

    this.waMonitor.clearDelInstanceTime(instanceName);

    try {
      await this.waMonitor.waInstances[instanceName]?.logoutInstance({ permanent: true });
    } catch (error) {
      this.logger.warn({
        message: 'Failed to logout orphaned runtime instance',
        instanceName,
        error: error?.toString(),
      });
    }

    try {
      await this.waMonitor.cleaningUp(instanceName);
    } catch (error) {
      this.logger.warn({
        message: 'Failed to clean orphaned runtime session data',
        instanceName,
        error: error?.toString(),
      });
    }

    try {
      await this.waMonitor.cleaningStoreData(instanceName);
    } catch (error) {
      this.logger.warn({
        message: 'Failed to clean orphaned runtime store data',
        instanceName,
        error: error?.toString(),
      });
    }

    try {
      delete this.waMonitor.waInstances[instanceName];
    } catch (error) {
      this.logger.warn({
        message: 'Failed to remove orphaned runtime instance from monitor',
        instanceName,
        error: error?.toString(),
      });
    }
  }

  public async createInstance(instanceData: InstanceDto) {
    try {
      const instance = channelController.init(instanceData, {
        configService: this.configService,
        eventEmitter: this.eventEmitter,
        prismaRepository: this.prismaRepository,
        cache: this.cache,
        chatwootCache: this.chatwootCache,
        baileysCache: this.baileysCache,
        providerFiles: this.providerFiles,
      });

      if (!instance) {
        throw new BadRequestException('Invalid integration');
      }

      const instanceId = v4();

      instanceData.instanceId = instanceId;

      let hash: string;

      if (!instanceData.token) hash = v4().toUpperCase();
      else hash = instanceData.token;

      await this.waMonitor.saveInstance({
        instanceId,
        integration: instanceData.integration,
        instanceName: instanceData.instanceName,
        ownerJid: instanceData.ownerJid,
        profileName: instanceData.profileName,
        profilePicUrl: instanceData.profilePicUrl,
        hash,
        number: instanceData.number,
        businessId: instanceData.businessId,
        status: instanceData.status,
      });

      instance.setInstance({
        instanceName: instanceData.instanceName,
        instanceId,
        integration: instanceData.integration,
        token: hash,
        number: instanceData.number,
        businessId: instanceData.businessId,
      });

      this.waMonitor.waInstances[instance.instanceName] = instance;
      this.waMonitor.delInstanceTime(instance.instanceName);

      // set events
      await eventManager.setInstance(instance.instanceName, instanceData);

      instance.sendDataWebhook(Events.INSTANCE_CREATE, {
        instanceName: instanceData.instanceName,
        instanceId: instanceId,
      });

      const instanceDto: InstanceDto = {
        instanceName: instance.instanceName,
        instanceId: instance.instanceId,
        connectionStatus:
          typeof instance.connectionStatus === 'string'
            ? instance.connectionStatus
            : instance.connectionStatus?.state || 'unknown',
      };

      if (instanceData.proxyHost && instanceData.proxyPort && instanceData.proxyProtocol) {
        const testProxy = await this.proxyService.testProxy({
          host: instanceData.proxyHost,
          port: instanceData.proxyPort,
          protocol: instanceData.proxyProtocol,
          username: instanceData.proxyUsername,
          password: instanceData.proxyPassword,
        });
        if (!testProxy) {
          throw new BadRequestException('Invalid proxy');
        }
        await this.proxyService.createProxy(instanceDto, {
          enabled: true,
          host: instanceData.proxyHost,
          port: instanceData.proxyPort,
          protocol: instanceData.proxyProtocol,
          username: instanceData.proxyUsername,
          password: instanceData.proxyPassword,
        });
      }

      const settings: wa.LocalSettings = {
        rejectCall: instanceData.rejectCall === true,
        msgCall: instanceData.msgCall || '',
        groupsIgnore: instanceData.groupsIgnore === true,
        alwaysOnline: instanceData.alwaysOnline === true,
        readMessages: instanceData.readMessages === true,
        readStatus: instanceData.readStatus === true,
        syncFullHistory: true,
        wavoipToken: instanceData.wavoipToken || '',
      };

      await this.settingsService.create(instanceDto, settings);

      let webhookWaBusiness = null,
        accessTokenWaBusiness = '';

      if (instanceData.integration === Integration.WHATSAPP_BUSINESS) {
        if (!instanceData.number) {
          throw new BadRequestException('number is required');
        }
        const urlServer = this.configService.get<HttpServer>('SERVER').URL;
        webhookWaBusiness = `${urlServer}/webhook/meta`;
        accessTokenWaBusiness = this.configService.get<WaBusiness>('WA_BUSINESS').TOKEN_WEBHOOK;
      }

      if (!instanceData.chatwootAccountId || !instanceData.chatwootToken || !instanceData.chatwootUrl) {
        let getQrcode: wa.QrCode;

        if (instanceData.qrcode && instanceData.integration === Integration.WHATSAPP_BAILEYS) {
          await instance.connectToWhatsapp(instanceData.number);
          await delay(5000);
          getQrcode = instance.qrCode;
        }

        const result = {
          instance: {
            instanceName: instance.instanceName,
            instanceId: instanceId,
            integration: instanceData.integration,
            webhookWaBusiness,
            accessTokenWaBusiness,
            status:
              typeof instance.connectionStatus === 'string'
                ? instance.connectionStatus
                : instance.connectionStatus?.state || 'unknown',
          },
          hash,
          webhook: {
            webhookUrl: instanceData?.webhook?.url,
            webhookHeaders: instanceData?.webhook?.headers,
            webhookByEvents: instanceData?.webhook?.byEvents,
            webhookBase64: instanceData?.webhook?.base64,
          },
          websocket: {
            enabled: instanceData?.websocket?.enabled,
          },
          rabbitmq: {
            enabled: instanceData?.rabbitmq?.enabled,
          },
          nats: {
            enabled: instanceData?.nats?.enabled,
          },
          sqs: {
            enabled: instanceData?.sqs?.enabled,
          },
          settings,
          qrcode: getQrcode,
        };

        return result;
      }

      if (!this.configService.get<Chatwoot>('CHATWOOT').ENABLED)
        throw new BadRequestException('Chatwoot is not enabled');

      if (!instanceData.chatwootAccountId) {
        throw new BadRequestException('accountId is required');
      }

      if (!instanceData.chatwootToken) {
        throw new BadRequestException('token is required');
      }

      if (!instanceData.chatwootUrl) {
        throw new BadRequestException('url is required');
      }

      if (!isURL(instanceData.chatwootUrl, { require_tld: false })) {
        throw new BadRequestException('Invalid "url" property in chatwoot');
      }

      if (instanceData.chatwootSignMsg !== true && instanceData.chatwootSignMsg !== false) {
        throw new BadRequestException('signMsg is required');
      }

      if (instanceData.chatwootReopenConversation !== true && instanceData.chatwootReopenConversation !== false) {
        throw new BadRequestException('reopenConversation is required');
      }

      if (instanceData.chatwootConversationPending !== true && instanceData.chatwootConversationPending !== false) {
        throw new BadRequestException('conversationPending is required');
      }

      const urlServer = this.configService.get<HttpServer>('SERVER').URL;

      try {
        this.chatwootService.create(instanceDto, {
          enabled: true,
          accountId: instanceData.chatwootAccountId,
          token: instanceData.chatwootToken,
          url: instanceData.chatwootUrl,
          signMsg: instanceData.chatwootSignMsg || false,
          nameInbox: instanceData.chatwootNameInbox ?? instance.instanceName.split('-cwId-')[0],
          number: instanceData.number,
          reopenConversation: instanceData.chatwootReopenConversation || false,
          conversationPending: instanceData.chatwootConversationPending || false,
          importContacts: instanceData.chatwootImportContacts ?? true,
          mergeBrazilContacts: instanceData.chatwootMergeBrazilContacts ?? false,
          importMessages: instanceData.chatwootImportMessages ?? true,
          daysLimitImportMessages: instanceData.chatwootDaysLimitImportMessages ?? 0,
          organization: instanceData.chatwootOrganization,
          logo: instanceData.chatwootLogo,
          autoCreate: instanceData.chatwootAutoCreate !== false,
        });
      } catch (error) {
        this.logger.log(error);
      }

      return {
        instance: {
          instanceName: instance.instanceName,
          instanceId: instanceId,
          integration: instanceData.integration,
          webhookWaBusiness,
          accessTokenWaBusiness,
          status:
            typeof instance.connectionStatus === 'string'
              ? instance.connectionStatus
              : instance.connectionStatus?.state || 'unknown',
        },
        hash,
        webhook: {
          webhookUrl: instanceData?.webhook?.url,
          webhookHeaders: instanceData?.webhook?.headers,
          webhookByEvents: instanceData?.webhook?.byEvents,
          webhookBase64: instanceData?.webhook?.base64,
        },
        websocket: {
          enabled: instanceData?.websocket?.enabled,
        },
        rabbitmq: {
          enabled: instanceData?.rabbitmq?.enabled,
        },
        nats: {
          enabled: instanceData?.nats?.enabled,
        },
        sqs: {
          enabled: instanceData?.sqs?.enabled,
        },
        settings,
        chatwoot: {
          enabled: true,
          accountId: instanceData.chatwootAccountId,
          token: instanceData.chatwootToken,
          url: instanceData.chatwootUrl,
          signMsg: instanceData.chatwootSignMsg || false,
          reopenConversation: instanceData.chatwootReopenConversation || false,
          conversationPending: instanceData.chatwootConversationPending || false,
          mergeBrazilContacts: instanceData.chatwootMergeBrazilContacts ?? false,
          importContacts: instanceData.chatwootImportContacts ?? true,
          importMessages: instanceData.chatwootImportMessages ?? true,
          daysLimitImportMessages: instanceData.chatwootDaysLimitImportMessages ?? 0,
          number: instanceData.number,
          nameInbox: instanceData.chatwootNameInbox ?? instance.instanceName,
          webhookUrl: `${urlServer}/chatwoot/webhook/${encodeURIComponent(instance.instanceName)}`,
        },
      };
    } catch (error) {
      this.waMonitor.deleteInstance(instanceData.instanceName);
      this.logger.error(isArray(error.message) ? error.message[0] : error.message);
      throw new BadRequestException(isArray(error.message) ? error.message[0] : error.message);
    }
  }

  public async connectToWhatsapp({ instanceName, number = null }: InstanceDto) {
    return this.enqueueLifecycleOperation(instanceName, () => this.performConnectToWhatsapp({ instanceName, number }));
  }

  private async performConnectToWhatsapp({ instanceName, number = null }: InstanceDto) {
    try {
      const instance = this.waMonitor.waInstances[instanceName] as
        | (wa.Instance & {
            qrCode?: wa.QrCode;
            connectionStatus?: { state?: string; statusReason?: number };
            connectToWhatsapp?: (number?: string) => Promise<unknown>;
            prepareForFreshConnectAttempt?: () => Promise<void>;
            hasAuthenticationArtifacts?: () => boolean;
          })
        | undefined;
      const rawState = instance?.connectionStatus?.state;
      const state = await this.runtimeConnectionState(instance);

      if (!state) {
        throw new BadRequestException('The "' + instanceName + '" instance does not exist');
      }

      if (state == 'open') {
        return await this.connectionState({ instanceName });
      }

      if (state == 'connecting') {
        if (this.hasAuthenticationArtifacts(instance)) {
          return await this.connectOutcomeResponse(instanceName, instance);
        }

        await this.waitForConnectOutcome(instance);
        return await this.connectOutcomeResponse(instanceName, instance);
      }

      if (state == 'close') {
        if (rawState == 'open') {
          await instance.prepareForFreshConnectAttempt?.();
          await instance.connectToWhatsapp(number);
          await this.waitForConnectOutcome(instance);
          return await this.connectOutcomeResponse(instanceName, instance);
        }

        if (this.hasAuthenticationArtifacts(instance)) {
          return await this.connectOutcomeResponse(instanceName, instance);
        }

        await instance.prepareForFreshConnectAttempt?.();
        await instance.connectToWhatsapp(number);
        await this.waitForConnectOutcome(instance);
        return await this.connectOutcomeResponse(instanceName, instance);
      }

      await this.waitForConnectOutcome(instance, 3000);
      return await this.connectOutcomeResponse(instanceName, instance);
    } catch (error) {
      this.logger.error(error);
      return { error: true, message: this.errorMessage(error) };
    }
  }

  public async restartInstance({ instanceName }: InstanceDto) {
    return this.enqueueLifecycleOperation(instanceName, () => this.performRestartInstance({ instanceName }));
  }

  private async performRestartInstance({ instanceName }: InstanceDto) {
    try {
      const instance = this.waMonitor.waInstances[instanceName];
      const state = await this.runtimeConnectionState(instance);
      const rawState = instance?.connectionStatus?.state;

      if (!state) {
        throw new BadRequestException('The "' + instanceName + '" instance does not exist');
      }

      if (state === 'close' && rawState !== 'open') {
        throw new BadRequestException('The "' + instanceName + '" instance is not connected');
      }
      this.logger.info(`Restarting instance: ${instanceName}`);

      if (typeof instance.restart === 'function') {
        await instance.restart();
        // Wait a bit for the reconnection to be established
        await new Promise((r) => setTimeout(r, 2000));
        return await this.performConnectToWhatsapp({
          instanceName,
          number: instance.phoneNumber || null,
        });
      }

      // Fallback for Baileys (uses different mechanism)
      if (state === 'open' || state === 'connecting' || state === 'reconnecting') {
        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) instance.clearCacheChatwoot();

        instance.client?.ws?.close();
        instance.client?.end(new Error('restart'));
        return await this.performConnectToWhatsapp({ instanceName });
      }

      return {
        instance: {
          instanceName: instanceName,
          status: state,
        },
      };
    } catch (error) {
      this.logger.error(error);
      return { error: true, message: error.toString() };
    }
  }

  public async connectionState({ instanceName }: InstanceDto) {
    const runtimeInstance = this.waMonitor.waInstances[instanceName];
    const persistedInstance = await this.prismaRepository.instance.findFirst({
      where: { name: instanceName },
      select: { id: true },
    });

    if (runtimeInstance && !persistedInstance) {
      await this.cleanupOrphanedRuntime(instanceName);
      throw new NotFoundException(`Instance "${instanceName}" not found`);
    }

    if (!runtimeInstance || !persistedInstance) {
      throw new NotFoundException(`Instance "${instanceName}" not found`);
    }

    const state = await this.runtimeConnectionState(runtimeInstance);

    return {
      instance: {
        instanceName: instanceName,
        state,
        live: state === 'open',
      },
    };
  }

  public async reauthorizeInstance({ instanceName, number = null }: InstanceDto) {
    const inFlight = this.reauthorizationInFlight.get(instanceName);
    if (inFlight) {
      return inFlight;
    }

    const operation = this.enqueueLifecycleOperation(instanceName, () =>
      this.performReauthorizeInstance({ instanceName, number }),
    );
    this.reauthorizationInFlight.set(instanceName, operation);

    try {
      return await operation;
    } finally {
      if (this.reauthorizationInFlight.get(instanceName) === operation) {
        this.reauthorizationInFlight.delete(instanceName);
      }
    }
  }

  private async performReauthorizeInstance({ instanceName, number = null }: InstanceDto) {
    const instance = this.waMonitor.waInstances[instanceName] as
      | (wa.Instance & {
          phoneNumber?: string;
          connectionStatus?: { state?: string; statusReason?: number };
          forceReauthentication?: (number?: string) => Promise<unknown>;
          qrCode?: wa.QrCode;
          hasAuthenticationArtifacts?: () => boolean;
          hasDurableLiveConnection?: () => Promise<boolean>;
        })
      | undefined;

    if (!instance || typeof instance.forceReauthentication !== 'function') {
      throw new BadRequestException(`The "${instanceName}" instance does not support reauthorization`);
    }

    try {
      await instance.forceReauthentication(number || instance.phoneNumber || null);
      await this.waitForConnectOutcome(instance);
      const response = await this.connectOutcomeResponse(instanceName, instance);
      const outcomeStatus = response.instance.status;

      if (outcomeStatus !== 'open' && outcomeStatus !== 'qr_ready') {
        throw new InternalServerErrorException(
          response.message || 'Authentication artifacts were not generated after reconnect',
        );
      }

      return response;
    } catch (error) {
      this.logger.error(error);
      if (error instanceof InternalServerErrorException) {
        throw error;
      }

      throw new InternalServerErrorException(this.errorMessage(error));
    }
  }

  private async runtimeConnectionState(
    instance:
      | (wa.Instance & {
          connectionStatus?: { state?: string };
          hasLiveConnection?: () => boolean;
          hasPersistedAuthenticationCredentials?: () => Promise<boolean | null>;
        })
      | undefined,
  ) {
    const state = instance?.connectionStatus?.state;
    if (state === 'open' && typeof instance?.hasLiveConnection === 'function') {
      const hasLiveConnection = instance.hasLiveConnection();
      const hasPersistedCredentials =
        typeof instance.hasPersistedAuthenticationCredentials === 'function'
          ? await instance.hasPersistedAuthenticationCredentials()
          : null;

      if (!hasLiveConnection || hasPersistedCredentials === false) {
        return 'close';
      }
    }

    return state;
  }

  private hasAuthenticationArtifacts(
    instance:
      | (wa.Instance & {
          qrCode?: wa.QrCode;
          hasAuthenticationArtifacts?: () => boolean;
        })
      | undefined,
  ) {
    if (!instance) {
      return false;
    }

    if (typeof instance.hasAuthenticationArtifacts === 'function') {
      return instance.hasAuthenticationArtifacts();
    }

    return Boolean(instance.qrCode?.base64 || instance.qrCode?.code || instance.qrCode?.pairingCode);
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

  private async waitForConnectOutcome(
    instance:
      | (wa.Instance & {
          qrCode?: wa.QrCode;
          connectionStatus?: { state?: string };
          hasAuthenticationArtifacts?: () => boolean;
          hasDurableLiveConnection?: () => Promise<boolean>;
        })
      | undefined,
    timeoutMs = InstanceController.CONNECT_OUTCOME_TIMEOUT_MS,
  ) {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      if (!instance) {
        return;
      }

      const state = instance.connectionStatus?.state;
      if (this.hasAuthenticationArtifacts(instance)) {
        return;
      }

      if (state === 'open') {
        const hasDurableLiveConnection =
          typeof instance.hasDurableLiveConnection === 'function' ? await instance.hasDurableLiveConnection() : true;
        if (hasDurableLiveConnection) {
          return;
        }
      }

      await delay(InstanceController.CONNECT_OUTCOME_POLL_MS);
    }
  }

  private async connectOutcomeResponse(
    instanceName: string,
    instance:
      | (wa.Instance & {
          qrCode?: wa.QrCode;
          connectionStatus?: { state?: string; statusReason?: number };
          hasAuthenticationArtifacts?: () => boolean;
          hasDurableLiveConnection?: () => Promise<boolean>;
        })
      | undefined,
  ) {
    const state = instance?.connectionStatus?.state ?? 'close';
    const hasAuthArtifacts = this.hasAuthenticationArtifacts(instance);
    const statusCode = instance?.connectionStatus?.statusReason;
    const hasDurableLiveConnection =
      state === 'open' && typeof instance?.hasDurableLiveConnection === 'function'
        ? await instance.hasDurableLiveConnection()
        : state === 'open';

    if (hasDurableLiveConnection) {
      return {
        instance: {
          instanceName,
          state: 'open',
          status: 'open',
        },
      };
    }

    if (hasAuthArtifacts) {
      return {
        instance: {
          instanceName,
          state: state === 'close' ? 'connecting' : state,
          status: 'qr_ready',
        },
        qrcode: instance?.qrCode,
      };
    }

    const reauthRequired = state === 'close' || state === 'open' || statusCode === 401 || statusCode === 428;
    if (reauthRequired) {
      return {
        instance: {
          instanceName,
          state: 'close',
          status: 'reauth_required',
        },
        status: 'reauth_required',
        message: 'Authentication artifacts were not generated after reconnect',
        statusCode,
        auth: {
          recoveryRequired: true,
          hasAuthenticationArtifacts: false,
        },
        qrcode: instance?.qrCode ?? { count: 0 },
      };
    }

    return {
      instance: {
        instanceName,
        state,
        status: state,
      },
      qrcode: instance?.qrCode,
    };
  }

  public async fetchInstances({ instanceName, instanceId, number }: InstanceDto, key: string) {
    const env = this.configService.get<Auth>('AUTHENTICATION').API_KEY;

    if (env.KEY !== key) {
      const instancesByKey = await this.prismaRepository.instance.findMany({
        where: {
          token: key,
          name: instanceName || undefined,
          id: instanceId || undefined,
        },
      });

      if (instancesByKey.length > 0) {
        const names = instancesByKey.map((instance) => instance.name);

        return this.waMonitor.instanceInfo(names);
      } else {
        throw new UnauthorizedException();
      }
    }

    if (instanceId || number) {
      return this.waMonitor.instanceInfoById(instanceId, number);
    }

    const instanceNames = instanceName ? [instanceName] : null;

    return this.waMonitor.instanceInfo(instanceNames);
  }

  public async setPresence({ instanceName }: InstanceDto, data: SetPresenceDto) {
    return await this.waMonitor.waInstances[instanceName].setPresence(data);
  }

  public async logout({ instanceName }: InstanceDto, { permanent = false }: { permanent?: boolean } = {}) {
    return this.enqueueLifecycleOperation(instanceName, () => this.performLogout({ instanceName }, { permanent }));
  }

  private async performLogout({ instanceName }: InstanceDto, { permanent = false }: { permanent?: boolean } = {}) {
    await this.connectionState({ instanceName });

    try {
      await this.waMonitor.waInstances[instanceName]?.logoutInstance({ permanent });
      await this.prismaRepository.instance.update({
        where: { name: instanceName },
        data: {
          connectionStatus: 'close',
          ownerJid: null,
          profileName: null,
          profilePicUrl: null,
          disconnectionAt: new Date(),
        },
      });

      return {
        status: 'SUCCESS',
        error: false,
        instance: { instanceName, state: 'close' },
        response: { message: 'Instance logged out' },
      };
    } catch (error) {
      throw new InternalServerErrorException(error.toString());
    }
  }

  public async deleteInstance({ instanceName }: InstanceDto) {
    return this.enqueueLifecycleOperation(instanceName, () => this.performDeleteInstance({ instanceName }));
  }

  private async performDeleteInstance({ instanceName }: InstanceDto) {
    await this.connectionState({ instanceName });
    try {
      const waInstances = this.waMonitor.waInstances[instanceName];
      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) waInstances?.clearCacheChatwoot();

      if (waInstances) {
        await this.performLogout({ instanceName }, { permanent: true });
      }

      try {
        waInstances?.sendDataWebhook(Events.INSTANCE_DELETE, {
          instanceName,
          instanceId: waInstances.instanceId,
        });
      } catch (error) {
        this.logger.error(error);
      }

      this.eventEmitter.emit('remove.instance', instanceName, 'inner');
      return { status: 'SUCCESS', error: false, response: { message: 'Instance deleted' } };
    } catch (error) {
      throw new BadRequestException(error.toString());
    }
  }
}
