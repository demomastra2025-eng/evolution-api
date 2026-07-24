import { EventDto } from '@api/integrations/event/event.dto';
import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { wa } from '@api/types/wa.types';
import { configService, Log, Webhook } from '@config/env.config';
import { Logger } from '@config/logger.config';
// import { BadRequestException } from '@exceptions';
import axios, { AxiosInstance } from 'axios';
import * as jwt from 'jsonwebtoken';

import { EmitData, EventController, EventControllerInterface } from '../event.controller';

export class WebhookController extends EventController implements EventControllerInterface {
  private readonly logger = new Logger('WebhookController');

  constructor(prismaRepository: PrismaRepository, waMonitor: WAMonitoringService) {
    super(prismaRepository, waMonitor, true, 'webhook');
  }

  override async set(instanceName: string, data: EventDto): Promise<wa.LocalWebHook> {
    // if (!/^(https?:\/\/)/.test(data.webhook.url)) {
    //   throw new BadRequestException('Invalid "url" property');
    // }

    if (!data.webhook?.enabled) {
      data.webhook.events = [];
    } else {
      if (0 === data.webhook.events.length) {
        data.webhook.events = EventController.events;
      }
    }

    return this.prisma.webhook.upsert({
      where: {
        instanceId: this.monitor.waInstances[instanceName].instanceId,
      },
      update: {
        enabled: data.webhook?.enabled,
        events: data.webhook?.events,
        url: data.webhook?.url,
        headers: data.webhook?.headers,
        webhookBase64: data.webhook.base64,
        webhookByEvents: data.webhook.byEvents,
      },
      create: {
        enabled: data.webhook?.enabled,
        events: data.webhook?.events,
        instanceId: this.monitor.waInstances[instanceName].instanceId,
        url: data.webhook?.url,
        headers: data.webhook?.headers,
        webhookBase64: data.webhook.base64,
        webhookByEvents: data.webhook.byEvents,
      },
    });
  }

  public async emit({
    instanceName,
    origin,
    event,
    data,
    serverUrl,
    dateTime,
    sender,
    apiKey,
    local,
    integration,
    extra,
  }: EmitData): Promise<void> {
    if (integration && !integration.includes('webhook')) {
      return;
    }

    const instance = (await this.get(instanceName)) as wa.LocalWebHook;

    const webhookConfig = configService.get<Webhook>('WEBHOOK');
    const webhookLocal = instance?.events;
    const webhookHeaders = { ...((instance?.headers as Record<string, string>) || {}) };

    if (webhookHeaders && 'jwt_key' in webhookHeaders) {
      const jwtKey = webhookHeaders['jwt_key'];
      const jwtToken = this.generateJwtToken(jwtKey);
      webhookHeaders['Authorization'] = `Bearer ${jwtToken}`;

      delete webhookHeaders['jwt_key'];
    }

    const we = event.replace(/[.-]/gm, '_').toUpperCase();
    const transformedWe = we.replace(/_/gm, '-').toLowerCase();
    const enabledLog = configService.get<Log>('LOG').LEVEL.includes('WEBHOOKS');
    const regex = /^(https?:\/\/)/;

    const webhookData = {
      ...(extra ?? {}),
      event,
      instance: instanceName,
      data,
      destination: instance?.url || `${webhookConfig.GLOBAL.URL}/${transformedWe}`,
      date_time: dateTime,
      sender,
      server_url: serverUrl,
      apikey: apiKey,
    };

    if (local && instance?.enabled) {
      if (Array.isArray(webhookLocal) && webhookLocal.includes(we)) {
        let baseURL: string;

        if (instance?.webhookByEvents) {
          baseURL = `${instance?.url}/${transformedWe}`;
        } else {
          baseURL = instance?.url;
        }

        if (enabledLog) {
          const logData = this.sanitizedLogPayload(`${origin}.sendData-Webhook`, baseURL, webhookData);

          this.logger.log(logData);
        }

        try {
          if (instance?.enabled && regex.test(instance.url)) {
            // Add custom headers for better webhook tracking and debugging
            const enhancedHeaders = {
              ...webhookHeaders,
              'Content-Type': 'application/json',
              'X-Instance-ID': this.monitor.waInstances[instanceName].instanceId,
              'X-Instance-Name': instanceName,
              'X-Event-Type': event,
              'X-Timestamp': Date.now().toString(),
              'User-Agent': 'EvolutionAPI-Webhook/2.3.7',
            };

            const httpService = axios.create({
              baseURL,
              headers: enhancedHeaders as Record<string, string>,
              timeout: webhookConfig.REQUEST?.TIMEOUT_MS ?? 30000,
            });

            await this.retryWebhookRequest(
              httpService,
              webhookData,
              `${origin}.sendData-Webhook`,
              baseURL,
              serverUrl,
              undefined,
              undefined,
              instanceName,
              event,
            );
          }
        } catch (error) {
          this.logger.error({
            local: `${origin}.sendData-Webhook`,
            message: `Todas as tentativas falharam: ${error?.message}`,
            hostName: error?.hostname,
            syscall: error?.syscall,
            code: error?.code,
            error: error?.errno,
            stack: error?.stack,
            name: error?.name,
            url: baseURL,
            server_url: serverUrl,
          });
        }
      }
    }

    if (webhookConfig.GLOBAL?.ENABLED) {
      if (webhookConfig.EVENTS[we]) {
        let globalURL = webhookConfig.GLOBAL.URL;

        if (webhookConfig.GLOBAL.WEBHOOK_BY_EVENTS) {
          globalURL = `${globalURL}/${transformedWe}`;
        }

        if (enabledLog) {
          const logData = this.sanitizedLogPayload(`${origin}.sendData-Webhook-Global`, globalURL, webhookData);

          this.logger.log(logData);
        }

        try {
          if (regex.test(globalURL)) {
            const httpService = axios.create({
              baseURL: globalURL,
              timeout: webhookConfig.REQUEST?.TIMEOUT_MS ?? 30000,
            });

            await this.retryWebhookRequest(
              httpService,
              webhookData,
              `${origin}.sendData-Webhook-Global`,
              globalURL,
              serverUrl,
            );
          }
        } catch (error) {
          this.logger.error({
            local: `${origin}.sendData-Webhook-Global`,
            message: `Todas as tentativas falharam: ${error?.message}`,
            hostName: error?.hostname,
            syscall: error?.syscall,
            code: error?.code,
            error: error?.errno,
            stack: error?.stack,
            name: error?.name,
            url: globalURL,
            server_url: serverUrl,
          });
        }
      }
    }
  }

  private async retryWebhookRequest(
    httpService: AxiosInstance,
    webhookData: any,
    origin: string,
    baseURL: string,
    serverUrl: string,
    maxRetries?: number,
    delaySeconds?: number,
    instanceName?: string,
    event?: string,
  ): Promise<void> {
    const webhookConfig = configService.get<Webhook>('WEBHOOK');
    const maxRetryAttempts = maxRetries ?? webhookConfig.RETRY?.MAX_ATTEMPTS ?? 10;
    const initialDelay = delaySeconds ?? webhookConfig.RETRY?.INITIAL_DELAY_SECONDS ?? 5;
    const useExponentialBackoff = webhookConfig.RETRY?.USE_EXPONENTIAL_BACKOFF ?? true;
    const maxDelay = webhookConfig.RETRY?.MAX_DELAY_SECONDS ?? 300;
    const jitterFactor = webhookConfig.RETRY?.JITTER_FACTOR ?? 0.2;
    const nonRetryableStatusCodes = webhookConfig.RETRY?.NON_RETRYABLE_STATUS_CODES ?? [400, 401, 403, 404, 422];

    let attempts = 0;

    while (attempts < maxRetryAttempts) {
      if (instanceName && event) {
        const shouldContinue = await this.shouldContinueLocalWebhookRetry(instanceName, event, baseURL);

        if (!shouldContinue) {
          this.logger.log({
            local: `${origin}`,
            message: `Cancelando retentativas para o instance "${instanceName}" porque o webhook local nao esta mais ativo para este destino`,
            url: baseURL,
          });
          return;
        }
      }

      try {
        await httpService.post('', webhookData);
        if (attempts > 0) {
          this.logger.log({
            local: `${origin}`,
            message: `Sucesso no envio após ${attempts + 1} tentativas`,
            url: baseURL,
          });
        }
        return;
      } catch (error) {
        attempts++;

        const isTimeout = error.code === 'ECONNABORTED';

        if (error?.response?.status && nonRetryableStatusCodes.includes(error.response.status)) {
          this.logger.error({
            local: `${origin}`,
            message: `Erro não recuperável (${error.response.status}): ${error?.message}. Cancelando retentativas.`,
            statusCode: error?.response?.status,
            url: baseURL,
            server_url: serverUrl,
          });
          throw error;
        }

        this.logger.error({
          local: `${origin}`,
          message: `Tentativa ${attempts}/${maxRetryAttempts} falhou: ${isTimeout ? 'Timeout da requisição' : error?.message}`,
          hostName: error?.hostname,
          syscall: error?.syscall,
          code: error?.code,
          isTimeout,
          statusCode: error?.response?.status,
          error: error?.errno,
          stack: error?.stack,
          name: error?.name,
          url: baseURL,
          server_url: serverUrl,
        });

        if (attempts === maxRetryAttempts) {
          throw error;
        }

        if (instanceName && event) {
          const shouldContinue = await this.shouldContinueLocalWebhookRetry(instanceName, event, baseURL);

          if (!shouldContinue) {
            this.logger.log({
              local: `${origin}`,
              message: `Cancelando retentativas para o instance "${instanceName}" porque o webhook local nao esta mais ativo para este destino`,
              url: baseURL,
            });
            return;
          }
        }

        let nextDelay = initialDelay;
        if (useExponentialBackoff) {
          nextDelay = Math.min(initialDelay * Math.pow(2, attempts - 1), maxDelay);

          const jitter = nextDelay * jitterFactor * (Math.random() * 2 - 1);
          nextDelay = Math.max(initialDelay, nextDelay + jitter);
        }

        this.logger.log({
          local: `${origin}`,
          message: `Aguardando ${nextDelay.toFixed(1)} segundos antes da próxima tentativa`,
          url: baseURL,
        });

        await new Promise((resolve) => setTimeout(resolve, nextDelay * 1000));
      }
    }
  }

  private async shouldContinueLocalWebhookRetry(
    instanceName: string,
    event: string,
    baseURL: string,
  ): Promise<boolean> {
    const runtimeInstance = this.monitor.waInstances[instanceName];
    if (!runtimeInstance) {
      return false;
    }

    const webhookConfig = (await this.get(instanceName)) as wa.LocalWebHook | null;
    if (!webhookConfig?.enabled || !webhookConfig?.url) {
      return false;
    }

    if (!/^(https?:\/\/)/.test(webhookConfig.url)) {
      return false;
    }

    const transformedEvent = event.replace(/[.-]/gm, '_').toUpperCase().replace(/_/gm, '-').toLowerCase();
    const currentBaseURL = webhookConfig.webhookByEvents
      ? `${webhookConfig.url}/${transformedEvent}`
      : webhookConfig.url;

    return currentBaseURL === baseURL;
  }

  private sanitizedLogPayload(local: string, url: string, webhookData: Record<string, any>) {
    return {
      local,
      url,
      ...webhookData,
      data: this.sanitizeWebhookData(webhookData.event, webhookData.data),
    };
  }

  private sanitizeWebhookData(event: string, data: any): any {
    if (event === 'messages.set' && Array.isArray(data)) {
      return this.summarizeMessagesSet(data);
    }

    if (Array.isArray(data)) {
      const sample = data.slice(0, 2).map((entry) => this.redactSensitiveFields(entry));

      return data.length > 2 ? { count: data.length, sample, truncated: true } : sample;
    }

    return this.redactSensitiveFields(data);
  }

  private summarizeMessagesSet(data: any[]) {
    const timestamps = data
      .map((entry) => Number(entry?.messageTimestamp))
      .filter((timestamp) => Number.isFinite(timestamp));
    const messageTypes = [...new Set(data.map((entry) => entry?.messageType).filter(Boolean))].slice(0, 10);
    const remoteJids = [...new Set(data.map((entry) => entry?.key?.remoteJid).filter(Boolean))].slice(0, 10);

    return {
      count: data.length,
      messageTypes,
      remoteJids,
      firstTimestamp: timestamps.length > 0 ? Math.min(...timestamps) : undefined,
      lastTimestamp: timestamps.length > 0 ? Math.max(...timestamps) : undefined,
      truncated: true,
    };
  }

  private redactSensitiveFields(value: any): any {
    if (!value || typeof value !== 'object') {
      return value;
    }

    if (Array.isArray(value)) {
      return value.slice(0, 2).map((entry) => this.redactSensitiveFields(entry));
    }

    const sensitiveKeys = new Set(['mediaKey', 'fileSha256', 'fileEncSha256', 'directPath', 'encHandle', 'base64']);

    return Object.entries(value).reduce<Record<string, any>>((result, [key, entry]) => {
      if (sensitiveKeys.has(key)) {
        result[key] = '[REDACTED]';
      } else {
        result[key] = this.redactSensitiveFields(entry);
      }

      return result;
    }, {});
  }

  private generateJwtToken(authToken: string): string {
    try {
      const payload = {
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 600, // 10 min expiration
        app: 'evolution',
        action: 'webhook',
      };

      const token = jwt.sign(payload, authToken, { algorithm: 'HS256' });
      return token;
    } catch (error) {
      this.logger.error({
        local: 'WebhookController.generateJwtToken',
        message: `JWT generation failed: ${error?.message}`,
      });
      throw error;
    }
  }
}
