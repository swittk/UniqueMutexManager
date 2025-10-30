import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    threads: false,
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
