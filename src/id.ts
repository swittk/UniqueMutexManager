function toUuid(bytes: ArrayLike<number>): string {
  const arr = Array.from({ length: bytes.length }, (_, index) => bytes[index]);
  const segments = [
    arr.slice(0, 4),
    arr.slice(4, 6),
    arr.slice(6, 8),
    arr.slice(8, 10),
    arr.slice(10, 16),
  ];

  return segments
    .map((segment) => segment.map((byte) => byte.toString(16).padStart(2, '0')).join(''))
    .join('-');
}

function fallbackUuid(): string {
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Math.floor(Math.random() * 256);
  }

  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  return toUuid(bytes);
}

export function generateInstanceId(): string {
  const cryptoObj = (globalThis as typeof globalThis & {
    crypto?: { randomUUID?: () => string; getRandomValues?: (buffer: Uint8Array) => Uint8Array };
  }).crypto;

  if (cryptoObj?.randomUUID) {
    return cryptoObj.randomUUID();
  }

  if (cryptoObj?.getRandomValues) {
    const buffer = cryptoObj.getRandomValues(new Uint8Array(16));
    buffer[6] = (buffer[6] & 0x0f) | 0x40;
    buffer[8] = (buffer[8] & 0x3f) | 0x80;
    return toUuid(buffer);
  }

  if (typeof require === 'function') {
    try {
      const module = require('node:crypto') as { randomUUID?: () => string };
      if (typeof module?.randomUUID === 'function') {
        return module.randomUUID();
      }
    } catch {
      // ignore
    }
  }

  return fallbackUuid();
}
