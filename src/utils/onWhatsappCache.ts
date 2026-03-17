import { prismaRepository } from '@api/server.module';
import { configService, Database } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { Prisma } from '@prisma/client';
import {
  getAvailableNumbers,
  getLookupCandidates,
  normalizeCacheJid,
  normalizeJidOptions,
} from '@utils/onWhatsappCache.helpers';
import dayjs from 'dayjs';

const logger = new Logger('OnWhatsappCache');

interface ISaveOnWhatsappCacheParams {
  remoteJid: string;
  remoteJidAlt?: string;
  lid?: 'lid' | undefined;
}

export async function saveOnWhatsappCache(data: ISaveOnWhatsappCacheParams[]) {
  if (!configService.get<Database>('DATABASE').SAVE_DATA.IS_ON_WHATSAPP) {
    return;
  }

  // Processa todos os itens em paralelo para melhor performance
  const processingPromises = data.map(async (item) => {
    try {
      const remoteJid = normalizeCacheJid(item.remoteJid);
      if (!remoteJid) {
        logger.warn('[saveOnWhatsappCache] Item skipped, missing remoteJid.');
        return;
      }

      const altJidNormalized = normalizeCacheJid(item.remoteJidAlt);
      const lidAltJid = altJidNormalized && altJidNormalized.includes('@lid') ? altJidNormalized : null;

      const baseJids = [remoteJid]; // Garante que o remoteJid esteja na lista inicial
      if (lidAltJid) {
        baseJids.push(lidAltJid);
      }

      const expandedJids = normalizeJidOptions(baseJids.flatMap((jid) => getAvailableNumbers(jid)));
      const lookupCandidates = getLookupCandidates(expandedJids);

      // 1. Busca entrada por jidOptions e também remoteJid
      // Às vezes acontece do remoteJid atual NÃO ESTAR no jidOptions ainda, ocasionando o erro:
      // 'Unique constraint failed on the fields: (`remoteJid`)'
      // Isso acontece principalmente em grupos que possuem o número do criador no ID (ex.: '559911223345-1234567890@g.us')
      const existingRecord = await prismaRepository.isOnWhatsapp.findFirst({
        where: {
          OR: [
            ...lookupCandidates.map((jid) => ({ jidOptions: { contains: jid } })),
            { remoteJid: remoteJid }, // TODO: Descobrir o motivo que causa o remoteJid não estar (às vezes) incluso na lista de jidOptions
          ],
        },
      });

      logger.verbose(
        `[saveOnWhatsappCache] Register exists for [${expandedJids.join(',')}]? => ${existingRecord ? existingRecord.remoteJid : 'Not found'}`,
      );

      // 2. Unifica todos os JIDs usando um Set para garantir valores únicos
      const finalJidOptions = new Set(expandedJids);

      if (lidAltJid) {
        finalJidOptions.add(lidAltJid);
      }

      if (existingRecord?.jidOptions) {
        normalizeJidOptions(existingRecord.jidOptions.split(',')).forEach((jid) => finalJidOptions.add(jid));
      }

      // 3. Prepara o payload final
      // Ordena os JIDs para garantir consistência na string final
      const sortedJidOptions = normalizeJidOptions(finalJidOptions);
      const newJidOptionsString = sortedJidOptions.join(',');
      const newLid = item.lid === 'lid' || item.remoteJid?.includes('@lid') ? 'lid' : null;

      const dataPayload = {
        remoteJid: remoteJid,
        jidOptions: newJidOptionsString,
        lid: newLid,
      };

      // 4. Decide entre Criar ou Atualizar
      if (existingRecord) {
        // Compara a string de JIDs ordenada existente com a nova
        const existingJidOptionsString = existingRecord.jidOptions
          ? existingRecord.jidOptions.split(',').sort().join(',')
          : '';

        const isDataSame =
          existingRecord.remoteJid === dataPayload.remoteJid &&
          existingJidOptionsString === dataPayload.jidOptions &&
          existingRecord.lid === dataPayload.lid;

        if (isDataSame) {
          logger.verbose(`[saveOnWhatsappCache] Data for ${remoteJid} is already up-to-date. Skipping update.`);
          return; // Pula para o próximo item
        }

        // Os dados são diferentes, então atualiza
        logger.verbose(
          `[saveOnWhatsappCache] Register exists, updating: remoteJid=${remoteJid}, jidOptions=${dataPayload.jidOptions}, lid=${dataPayload.lid}`,
        );
        await prismaRepository.isOnWhatsapp.update({
          where: { id: existingRecord.id },
          data: dataPayload,
        });
      } else {
        // Cria nova entrada
        logger.verbose(
          `[saveOnWhatsappCache] Register does not exist, creating: remoteJid=${remoteJid}, jidOptions=${dataPayload.jidOptions}, lid=${dataPayload.lid}`,
        );
        try {
          await prismaRepository.isOnWhatsapp.create({
            data: dataPayload,
          });
        } catch (error: any) {
          // Check for unique constraint violation (Prisma error code P2002)
          if (
            error instanceof Prisma.PrismaClientKnownRequestError &&
            error.code === 'P2002' &&
            (error.meta?.target as string[])?.includes('remoteJid')
          ) {
            logger.verbose(
              `[saveOnWhatsappCache] Race condition detected for ${remoteJid}, updating existing record instead.`,
            );
            await prismaRepository.isOnWhatsapp.update({
              where: { remoteJid: remoteJid },
              data: dataPayload,
            });
          } else {
            throw error;
          }
        }
      }
    } catch (e) {
      // Loga o erro mas não para a execução dos outros promises
      logger.error(`[saveOnWhatsappCache] Error processing item for ${item.remoteJid}: `);
      logger.error(e);
    }
  });

  // Espera todas as operações paralelas terminarem
  await Promise.allSettled(processingPromises);
}

export async function getOnWhatsappCache(remoteJids: string[]) {
  let results: {
    remoteJid: string;
    number: string;
    jidOptions: string[];
    lid?: string;
  }[] = [];

  if (configService.get<Database>('DATABASE').SAVE_DATA.IS_ON_WHATSAPP) {
    const remoteJidsWithoutPlus = remoteJids.flatMap((remoteJid) => getAvailableNumbers(remoteJid));
    const normalizedRemoteJids = normalizeJidOptions(remoteJids);
    const lookupCandidates = getLookupCandidates(remoteJidsWithoutPlus);

    const onWhatsappCache = await prismaRepository.isOnWhatsapp.findMany({
      where: {
        OR: [
          ...lookupCandidates.map((remoteJid) => ({ jidOptions: { contains: remoteJid } })),
          ...normalizedRemoteJids.map((remoteJid) => ({ remoteJid })),
        ],
        updatedAt: {
          gte: dayjs().subtract(configService.get<Database>('DATABASE').SAVE_DATA.IS_ON_WHATSAPP_DAYS, 'days').toDate(),
        },
      },
    });

    results = onWhatsappCache.map((item) => ({
      remoteJid: normalizeCacheJid(item.remoteJid) || item.remoteJid,
      number: item.remoteJid.split('@')[0],
      jidOptions: normalizeJidOptions(item.jidOptions.split(',')),
      lid: item.lid,
    }));
  }

  return results;
}
