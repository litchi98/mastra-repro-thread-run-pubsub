import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const dir = join(import.meta.dirname, '.tmp');
mkdirSync(dir, { recursive: true });

export const TMP_DIR = dir;
export const DB_URL = `file:${join(dir, 'repro.db')}`;
export const SOCKET = join(dir, 'pubsub.sock');
export const RESOURCE_ID = 'user-1';
export const THREAD_ID = 'thread-1';
