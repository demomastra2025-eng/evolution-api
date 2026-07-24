const MAX_DEPTH = 5;
const MAX_ARRAY_SAMPLE = 5;
const MAX_OBJECT_KEYS = 25;
const MAX_STRING_LENGTH = 500;

const REDACTED_KEYS = new Set([
  'accessToken',
  'advSecretKey',
  'apiKey',
  'authorization',
  'base64',
  'cookie',
  'directPath',
  'encHandle',
  'fileEncSha256',
  'fileSha256',
  'identityKey',
  'jwt',
  'keyData',
  'mediaKey',
  'messageSecret',
  'noiseKey',
  'pairingEphemeralKeyPair',
  'password',
  'privKey',
  'privateKey',
  'refreshToken',
  'remoteIdentityKey',
  'secret',
  'secretKey',
  'senderKey',
  'signalIdentities',
  'signedIdentityKey',
  'signedPreKey',
  'token',
]);

const SUMMARIZED_KEYS = new Set([
  'args',
  'contact',
  'contacts',
  'message',
  'messages',
  'packet',
  'payload',
  'received',
  'request',
  'response',
  'stanza',
]);

const SENSITIVE_KEY_PATTERN =
  /(authorization|cookie|password|secret|token|api[-_]?key|jwt|priv(?:ate)?key|identitykey|ciphertext|base64)/i;

function truncateString(value: string) {
  if (value.length <= MAX_STRING_LENGTH) {
    return value;
  }

  return `${value.slice(0, MAX_STRING_LENGTH)}...[truncated ${value.length - MAX_STRING_LENGTH} chars]`;
}

function isPlainObject(value: unknown): value is Record<string, any> {
  return Object.prototype.toString.call(value) === '[object Object]';
}

function shouldRedact(key: string) {
  return REDACTED_KEYS.has(key) || SENSITIVE_KEY_PATTERN.test(key);
}

function summarizeValue(value: any, seen: WeakSet<object>) {
  if (value instanceof Error) {
    return {
      name: value.name,
      message: truncateString(value.message || ''),
      stack: truncateString(value.stack || ''),
    };
  }

  if (Buffer.isBuffer(value)) {
    return `[Buffer length=${value.length}]`;
  }

  if (value instanceof Uint8Array) {
    return `[Uint8Array length=${value.byteLength}]`;
  }

  if (Array.isArray(value)) {
    return {
      kind: 'array',
      count: value.length,
      sample: value.slice(0, 2).map((entry) => sanitizeInternal(entry, MAX_DEPTH, undefined, seen)),
      truncated: value.length > 2,
    };
  }

  if (isPlainObject(value)) {
    const keys = Object.keys(value);

    return {
      kind: 'object',
      keys: keys.slice(0, 10),
      truncated: keys.length > 10,
    };
  }

  if (typeof value === 'string') {
    return truncateString(value);
  }

  return value;
}

function sanitizeInternal(value: any, depth: number, parentKey?: string, seen = new WeakSet<object>()): any {
  if (value === null || value === undefined) {
    return value;
  }

  if (value instanceof Error) {
    return summarizeValue(value, seen);
  }

  if (typeof value === 'string') {
    return truncateString(value);
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return summarizeValue(value, seen);
  }

  if (depth >= MAX_DEPTH) {
    return summarizeValue(value, seen);
  }

  if (Array.isArray(value)) {
    if (seen.has(value)) {
      return '[Circular]';
    }

    seen.add(value);
    const sample = value.slice(0, MAX_ARRAY_SAMPLE).map((entry) => sanitizeInternal(entry, depth + 1, parentKey, seen));

    if (value.length > MAX_ARRAY_SAMPLE) {
      return { count: value.length, sample, truncated: true };
    }

    return sample;
  }

  if (!isPlainObject(value)) {
    return String(value);
  }

  if (parentKey && SUMMARIZED_KEYS.has(parentKey)) {
    return summarizeValue(value, seen);
  }

  if (seen.has(value)) {
    return '[Circular]';
  }

  seen.add(value);
  const entries = Object.entries(value);
  const sanitized = entries.slice(0, MAX_OBJECT_KEYS).reduce<Record<string, any>>((result, [key, entry]) => {
    if (shouldRedact(key)) {
      result[key] = '[REDACTED]';
    } else {
      result[key] = sanitizeInternal(entry, depth + 1, key, seen);
    }

    return result;
  }, {});

  if (entries.length > MAX_OBJECT_KEYS) {
    sanitized.__truncatedKeys = entries.length - MAX_OBJECT_KEYS;
  }

  return sanitized;
}

export function sanitizeLogValue(value: any) {
  return sanitizeInternal(value, 0, undefined, new WeakSet<object>());
}

export function formatLogValue(value: any) {
  if (value === undefined) {
    return 'undefined';
  }

  if (typeof value === 'string') {
    return value;
  }

  try {
    return JSON.stringify(value);
  } catch {
    return '[Unserializable value]';
  }
}
