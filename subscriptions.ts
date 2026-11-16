/**
 * Issue 1: a thread-API run that went through a tool suspension keeps its `agent.stream.<runId>` /
 * `agent.control.<runId>` PubSub subscriptions until `cleanupTimeoutMs`; a plain run releases them
 * as soon as it finishes.
 *
 * Setup: Mastra with `backgroundTasks` enabled (so `sendMessage` goes through the idle wrapper),
 * DurableAgent with `cleanupTimeoutMs: 10_000`, in-memory libsql, an EventEmitterPubSub subclass that
 * counts live subscriptions. With Redis Streams each such subscription is a blocking XREADGROUP connection.
 *
 * Run: npm run repro:subscriptions
 */
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import corePkg from '@mastra/core/package.json' with { type: 'json' };
import { CountingPubSub, durableAgent, mockModel, observeThread, sleep } from './shared.ts';

const CLEANUP_MS = 10_000;
const resourceId = 'user-1';

const model = mockModel(prompt => {
  const last = prompt.at(-1);
  if (last?.role === 'tool') return { text: 'Thanks for answering.' };
  const userText = JSON.stringify(last?.content ?? '');
  if (userText.includes('ask me')) {
    return { toolCall: { id: `call-${Date.now()}`, name: 'ask_user', args: { question: 'Which one?' } } };
  }
  return { text: 'Plain reply.' };
});

const storage = new LibSQLStore({ id: 'store', url: ':memory:' });
const pubsub = new CountingPubSub();
const memory = new Memory({ storage });
const mastra = new Mastra({
  agents: { agent: durableAgent({ model, memory, cleanupTimeoutMs: CLEANUP_MS }) as any },
  storage,
  pubsub,
  // Without backgroundTasks, sendMessage does not use the idle wrapper and both runs wait for the timer.
  ...(process.env.NO_BACKGROUND_TASKS ? {} : { backgroundTasks: { enabled: true } }),
  logger: false,
});
const agent: any = mastra.getAgent('agent');

async function report(runId: string, finishedAt: number) {
  console.log(`  topics subscribed during the run: [${pubsub.everForRun(runId).join(', ')}]`);
  for (const at of [500, 3_000, CLEANUP_MS + 1_000]) {
    await sleep(at - (Date.now() - finishedAt));
    console.log(`  +${String(at).padStart(5)} ms after the run settled: live subscriptions = [${pubsub.forRun(runId).join(', ')}]`);
  }
}

console.log(`@mastra/core ${corePkg.version}, cleanupTimeoutMs = ${CLEANUP_MS}`);

// --- A. plain run ------------------------------------------------------------
{
  const threadId = 'thread-plain';
  const obs = await observeThread(agent, resourceId, threadId);
  const accepted: any = await agent.sendMessage('hello', { resourceId, threadId }).accepted;
  console.log(`\nA. plain run (${accepted.runId})`);
  console.log(`  status: ${await obs.until(['success', 'failed'])}`);
  obs.close();
  await report(accepted.runId, Date.now());
}

// --- B. run that suspends on a tool, then is resumed with sendStreamResume ----
{
  const threadId = 'thread-suspend';
  const obs = await observeThread(agent, resourceId, threadId);
  const accepted: any = await agent.sendMessage('please ask me something', { resourceId, threadId }).accepted;
  const runId = accepted.runId;
  console.log(`\nB. run that suspends and is resumed (${runId})`);
  console.log(`  status: ${await obs.until(['suspended', 'tool-call-suspended'])}`);
  await sleep(300);
  console.log(`  while suspended: live subscriptions = [${pubsub.forRun(runId).join(', ')}]`);

  const { runs } = await agent.listSuspendedRuns({ threadId, resourceId });
  const toolCallId = JSON.stringify(runs[0]).match(/call-\d+/)?.[0];
  const resumed = await agent.sendStreamResume({ resourceId, threadId, runId, toolCallId, resumeData: { answer: 'this one' } });
  console.log(`  sendStreamResume() returned: ${JSON.stringify(resumed)}  (no cleanup handle)`);
  console.log(`  status: ${await obs.until(['success', 'failed'])}`);
  obs.close();
  await report(runId, Date.now());
}

await mastra.shutdown();
process.exit(0);
