import { describe, expect, it } from 'vitest';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

describe('ESM bundle', () => {
  it('can be imported and used', async () => {
    const modulePath = path.resolve(__dirname, '../dist/esm/index.js');
    const moduleUrl = pathToFileURL(modulePath).href;

    const esmModule = await import(moduleUrl);

    expect(typeof esmModule.UniqueMutexManager).toBe('function');

    const manager = new esmModule.UniqueMutexManager();
    const result = await manager.runOperation('esm-smoke-test', async () => 'ok');
    expect(result).toBe('ok');
  });
});
