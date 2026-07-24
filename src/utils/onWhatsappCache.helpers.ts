const NORMAL_USER_DOMAIN = 's.whatsapp.net';

function unique(values: string[]) {
  return [...new Set(values)];
}

function hasDomain(jid: string) {
  return jid.includes('@');
}

function isNonPhoneDomain(domain: string) {
  return domain === 'lid' || domain === 'g.us';
}

export function normalizeCacheJid(jid: string | null | undefined): string | null {
  if (!jid) return null;

  const normalized = (jid.startsWith('+') ? jid.slice(1) : jid).trim();
  if (!normalized) return null;

  const parts = normalized.split('@');
  if (parts.length <= 2) {
    return normalized;
  }

  const [identifier, domain, ...restDomains] = parts;
  if (restDomains.length > 0 && restDomains.every((candidate) => candidate === domain)) {
    return `${identifier}@${domain}`;
  }

  return normalized;
}

export function getAvailableNumbers(remoteJid: string): string[] {
  const normalizedRemoteJid = normalizeCacheJid(remoteJid);
  if (!normalizedRemoteJid) {
    return [];
  }

  const [number, domain] = normalizedRemoteJid.split('@');
  if (!domain || isNonPhoneDomain(domain)) {
    return [normalizedRemoteJid];
  }

  const numbersAvailable: string[] = [];

  if (number.startsWith('55')) {
    const numberWithDigit =
      number.slice(4, 5) === '9' && number.length === 13 ? number : `${number.slice(0, 4)}9${number.slice(4)}`;
    const numberWithoutDigit = number.length === 12 ? number : number.slice(0, 4) + number.slice(5);

    numbersAvailable.push(numberWithDigit, numberWithoutDigit);
  } else if (number.startsWith('52') || number.startsWith('54')) {
    const prefix = number.startsWith('52') ? '1' : '9';
    const numberWithDigit =
      number.slice(2, 3) === prefix && number.length === 13
        ? number
        : `${number.slice(0, 2)}${prefix}${number.slice(2)}`;
    const numberWithoutDigit = number.length === 12 ? number : number.slice(0, 2) + number.slice(3);

    numbersAvailable.push(numberWithDigit, numberWithoutDigit);
  } else {
    numbersAvailable.push(number);
  }

  return unique(numbersAvailable.map((candidate) => `${candidate}@${domain}`));
}

export function getLookupCandidates(jids: Iterable<string>): string[] {
  const lookupCandidates = new Set<string>();

  for (const jid of jids) {
    const normalized = normalizeCacheJid(jid);
    if (!normalized) {
      continue;
    }

    lookupCandidates.add(normalized);

    if (!hasDomain(normalized)) {
      continue;
    }

    const [identifier, domain] = normalized.split('@');
    if (domain === NORMAL_USER_DOMAIN) {
      lookupCandidates.add(`${identifier}@${domain}@${domain}`);
    }
  }

  return [...lookupCandidates];
}

export function normalizeJidOptions(jids: Iterable<string>): string[] {
  const normalizedOptions = new Set<string>();

  for (const jid of jids) {
    const normalized = normalizeCacheJid(jid);
    if (normalized) {
      normalizedOptions.add(normalized);
    }
  }

  return [...normalizedOptions].sort();
}
