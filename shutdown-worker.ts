/**
 * Worker process for shutdown.ts.
 *   `tsx shutdown-worker.ts start`   : sends a message; step 1 calls a tool, the step-2 model call never
 *                                     returns; after 1.5 s it calls
 *                                     `mastra.shutdown({ drainTimeout: 500 })` (a rolling restart).
 *   `tsx shutdown-worker.ts recover` : calls `mastra.recoverAllDurableAgents()`; the model answers right away.
 */
import { Mastra } from '@mastra/core/mastra';
import { UnixSocketPubSub } from '@mastra/core/events';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { durableAgent, mockModel, sleep } from './shared.ts';
import { DB_URL, RESOURCE_ID, SOCKET, THREAD_ID } from './shutdown-config.ts';

const mode = process.argv[2];
const log = (m: string) => console.log(`  [worker:${mode}] ${m}`);

// Step 1 calls `lookup` (a step boundary, so a checkpoint exists); step 2 is where the run is when shutdown starts.
const model = mockModel(
  prompt =>
    prompt.at(-1)?.role === 'tool'
      ? { text: 'Answer produced after recovery.' }
      : { toolCall: { id: 'call-lookup', name: 'lookup', args: { q: 'x' } } },
  mode === 'start'
    ? (signal, prompt) =>
        prompt.at(-1)?.role === 'tool'
          ? new Promise<void>((_, reject) => signal?.addEventListener('abort', () => reject(new Error('aborted'))))
          : Promise.resolve()
    : undefined,
);

const storage = new LibSQLStore({ id: 'store', url: DB_URL });
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory }) as any },
  storage,
  pubsub: new UnixSocketPubSub(SOCKET),
  // Every process runs the same config: 'auto' also makes running checkpoints persist at step boundaries.
  recovery: { durableAgents: 'auto' },
  logger: false,
});
const agent: any = mastra.getAgent('agent');

if (mode === 'start') {
  const accepted: any = await agent.sendMessage('hello', { resourceId: RESOURCE_ID, threadId: THREAD_ID }).accepted;
  log(`sendMessage accepted, runId=${accepted.runId}; step-2 model call is hanging`);
  await sleep(1500);
  log('mastra.shutdown({ drainTimeout: 500 })  <- e.g. SIGTERM during a rolling restart');
  await mastra.shutdown({ drainTimeout: 500 });
  log('shutdown done');
} else {
  log(`recoverAllDurableAgents() -> ${JSON.stringify(await (mastra as any).recoverAllDurableAgents())}`);
  await sleep(3000);
  await mastra.shutdown();
}
process.exit(0);
