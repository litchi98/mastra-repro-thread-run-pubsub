/**
 * Issue 2: when `mastra.shutdown()` hands back a running thread run, subscribers get
 * `thread-run-status: failed` — the same final status as a real failure — although durable recovery
 * then continues the same runId on another process.
 *
 * Three processes share one libsql file and one UnixSocketPubSub (cross-process, no Redis needed):
 *   observer (this process) : subscribeToThread({ withRunStatus: true }) and prints what arrives
 *   worker "start"          : sendMessage; the model call hangs; shutdown({ drainTimeout: 500 })
 *   worker "recover"        : boots with recovery on and finishes the run
 *
 * Run: npm run repro:shutdown
 */
import { spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { Mastra } from '@mastra/core/mastra';
import { UnixSocketPubSub } from '@mastra/core/events';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { durableAgent, mockModel, observeThread, sleep } from './shared.ts';
import { DB_URL, RESOURCE_ID, SOCKET, THREAD_ID, TMP_DIR } from './shutdown-config.ts';

rmSync(TMP_DIR, { recursive: true, force: true });
mkdirSync(TMP_DIR, { recursive: true });

function worker(mode: 'start' | 'recover') {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(process.execPath, ['--import', 'tsx', 'shutdown-worker.ts', mode], {
      cwd: import.meta.dirname,
      stdio: 'inherit',
    });
    child.on('exit', code => (code === 0 ? resolve() : reject(new Error(`worker ${mode} exited with ${code}`))));
  });
}

console.log(`@mastra/core ${corePkg.version}`);
const storage = new LibSQLStore({ id: 'store', url: DB_URL });
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model: mockModel(() => ({ text: 'unused' })), memory }) as any },
  storage,
  pubsub: new UnixSocketPubSub(SOCKET),
  logger: false,
});
const agent: any = mastra.getAgent('agent');
const obs = await observeThread(agent, RESOURCE_ID, THREAD_ID);

await worker('start');
console.log(`  [observer] after the hand-back: ${JSON.stringify(obs.log)}`);
await worker('recover');
await sleep(500);
console.log(`  [observer] after recovery:      ${JSON.stringify(obs.log)}`);

const { messages } = await memory.recall({ threadId: THREAD_ID, resourceId: RESOURCE_ID });
console.log(`  stored messages: ${JSON.stringify(messages.map((m: any) => `${m.role}`))}`);
obs.close();
await mastra.shutdown();
process.exit(0);
