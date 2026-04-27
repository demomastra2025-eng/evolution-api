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

const CACHE_WRITE_MAX_ATTEMPTS = 3;

function isRemoteJidUniqueViolation(error: unknown) {
  return (
    error instanceof Prisma.PrismaClientKnownRequestError &&
    error.code === 'P2002' &&
    (error.meta?.target as string[])?.includes('remoteJid')
  );
}

function toSortedJidOptionsString(jids: Iterable<string>) {
  return normalizeJidOptions(jids).join(',');
}

async function upsertOnWhatsappCacheRecord(
  remoteJid: string,
  lookupCandidates: string[],
  dataPayload: {
    remoteJid: string;
    jidOptions: string;
    lid: string | null;
  },
) {
  for (let attempt = 1; attempt <= CACHE_WRITE_MAX_ATTEMPTS; attempt++) {
    const matchingRecords = await prismaRepository.isOnWhatsapp.findMany({
      where: {
        OR: [...lookupCandidates.map((jid) => ({ jidOptions: { contains: jid } })), { remoteJid }],
      },
      orderBy: { updatedAt: 'desc' },
    });

    const canonicalRecord = matchingRecords.find((record) => record.remoteJid === remoteJid) ?? matchingRecords[0];
    const aliasRecordIds = canonicalRecord
      ? matchingRecords.filter((record) => record.id !== canonicalRecord.id).map((record) => record.id)
      : [];

    const mergedJidOptions = new Set(dataPayload.jidOptions.split(',').filter(Boolean));
    matchingRecords.forEach((record) => {
      normalizeJidOptions(record.jidOptions.split(',')).forEach((jid) => mergedJidOptions.add(jid));
    });

    const finalPayload = {
      ...dataPayload,
      jidOptions: toSortedJidOptionsString(mergedJidOptions),
      lid: dataPayload.lid ?? matchingRecords.find((record) => record.lid === 'lid')?.lid ?? null,
    };

    if (!canonicalRecord) {
      try {
        logger.verbose(
          `[saveOnWhatsappCache] Register does not exist, creating: remoteJid=${remoteJid}, jidOptions=${finalPayload.jidOptions}, lid=${finalPayload.lid}`,
        );
        await prismaRepository.isOnWhatsapp.create({
          data: finalPayload,
        });
        return;
      } catch (error) {
        if (isRemoteJidUniqueViolation(error) && attempt < CACHE_WRITE_MAX_ATTEMPTS) {
          logger.verbose(
            `[saveOnWhatsappCache] RemoteJid collision while creating ${remoteJid}, retrying merge (${attempt}/${CACHE_WRITE_MAX_ATTEMPTS}).`,
          );
          continue;
        }

        throw error;
      }
    }

    const existingJidOptionsString = canonicalRecord.jidOptions
      ? toSortedJidOptionsString(canonicalRecord.jidOptions.split(','))
      : '';
    const isDataSame =
      canonicalRecord.remoteJid === finalPayload.remoteJid &&
      existingJidOptionsString === finalPayload.jidOptions &&
      canonicalRecord.lid === finalPayload.lid &&
      aliasRecordIds.length === 0;

    if (isDataSame) {
      logger.verbose(`[saveOnWhatsappCache] Data for ${remoteJid} is already up-to-date. Skipping update.`);
      return;
    }

    try {
      logger.verbose(
        `[saveOnWhatsappCache] Upserting merged record: remoteJid=${remoteJid}, jidOptions=${finalPayload.jidOptions}, lid=${finalPayload.lid}, mergedAliases=${aliasRecordIds.length}`,
      );
      await prismaRepository.$transaction([
        prismaRepository.isOnWhatsapp.update({
          where: { id: canonicalRecord.id },
          data: finalPayload,
        }),
        ...(aliasRecordIds.length
          ? [
              prismaRepository.isOnWhatsapp.deleteMany({
                where: { id: { in: aliasRecordIds } },
              }),
            ]
          : []),
      ]);
      return;
    } catch (error) {
      const shouldRetry =
        (isRemoteJidUniqueViolation(error) ||
          (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2025')) &&
        attempt < CACHE_WRITE_MAX_ATTEMPTS;

      if (shouldRetry) {
        logger.verbose(
          `[saveOnWhatsappCache] Cache merge conflict for ${remoteJid}, retrying (${attempt}/${CACHE_WRITE_MAX_ATTEMPTS}).`,
        );
        continue;
      }

      throw error;
    }
  }
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

      // Merge alias and canonical cache rows into a single canonical record.
      // Historical sync can discover the canonical PN JID after an alias row already exists,
      // which would otherwise fail on the unique remoteJid constraint during update.
      const finalJidOptions = new Set(expandedJids);

      if (lidAltJid) {
        finalJidOptions.add(lidAltJid);
      }

      const newJidOptionsString = toSortedJidOptionsString(finalJidOptions);
      const newLid = item.lid === 'lid' || item.remoteJid?.includes('@lid') ? 'lid' : null;

      const dataPayload = {
        remoteJid: remoteJid,
        jidOptions: newJidOptionsString,
        lid: newLid,
      };

      await upsertOnWhatsappCacheRecord(remoteJid, lookupCandidates, dataPayload);
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
    const normalizedRemoteJids = normalizeJidOptions(remoteJids);
    if (!normalizedRemoteJids.length) {
      return results;
    }

    const remoteJidsWithoutPlus = normalizedRemoteJids.flatMap((remoteJid) => getAvailableNumbers(remoteJid));
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
