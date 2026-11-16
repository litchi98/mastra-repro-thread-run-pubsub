/** Shared setup: mock model, a suspending tool, a subscription-counting PubSub, and an observer helper. */
import { Agent } from '@mastra/core/agent';
import { createDurableAgent } from '@mastra/core/agent/durable';
import { EventEmitterPubSub } from '@mastra/core/events';
import { createTool } from '@mastra/core/tools';
import { MockLanguageModelV2, convertArrayToReadableStream } from 'ai/test';
import { z } from 'zod';

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const usage = { inputTokens: 10, outputTokens: 5, totalTokens: 15 };

type Reply = { text: string } | { toolCall: { id: string; name: string; args: object } };

/**
 * Mock model. `decide` gets the prompt and returns a reply; `hold` (optional) is awaited
 * before the stream starts so a test can keep a run "in the model call".
 */
export function mockModel(
  decide: (prompt: any[]) => Reply,
  hold?: (signal: AbortSignal | undefined, prompt: any[]) => Promise<void>,
) {
  return new MockLanguageModelV2({
    doStream: async ({ prompt, abortSignal }) => {
      if (hold) await hold(abortSignal, prompt as any[]);
      const reply = decide(prompt as any[]);
      const parts: any[] = [
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: `r-${Math.random()}`, modelId: 'mock', timestamp: new Date(0) },
      ];
      if ('text' in reply) {
        parts.push(
          { type: 'text-start', id: 't1' },
          { type: 'text-delta', id: 't1', delta: reply.text },
          { type: 'text-end', id: 't1' },
          { type: 'finish', finishReason: 'stop', usage },
        );
      } else {
        parts.push(
          {
            type: 'tool-call',
            toolCallId: reply.toolCall.id,
            toolName: reply.toolCall.name,
            input: JSON.stringify(reply.toolCall.args),
          },
          { type: 'finish', finishReason: 'tool-calls', usage },
        );
      }
      return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: convertArrayToReadableStream(parts) };
    },
  });
}

/** A tool that suspends on first call and returns the resume data afterwards. */
export const askUser = createTool({
  id: 'ask_user',
  description: 'Ask the user a question',
  inputSchema: z.object({ question: z.string() }),
  suspendSchema: z.object({ question: z.string() }),
  resumeSchema: z.object({ answer: z.string() }),
  execute: async (input, ctx) => {
    const resumeData = ctx.agent?.resumeData as { answer: string } | undefined;
    if (!resumeData) {
      await ctx.agent?.suspend({ question: input.question });
      return undefined;
    }
    return { answer: resumeData.answer };
  },
});

/** A plain tool that returns immediately. */
export const lookup = createTool({
  id: 'lookup',
  description: 'Look something up',
  inputSchema: z.object({ q: z.string() }),
  execute: async input => ({ result: `result for ${input.q}` }),
});

export function durableAgent(opts: { model: any; memory: any; cleanupTimeoutMs?: number }) {
  return createDurableAgent({
    agent: new Agent({
      id: 'agent',
      name: 'agent',
      instructions: 'You are a test agent.',
      model: opts.model,
      tools: { ask_user: askUser, lookup },
      memory: opts.memory,
    }),
    cleanupTimeoutMs: opts.cleanupTimeoutMs,
  });
}

/** EventEmitterPubSub that tracks live subscriptions per topic. */
export class CountingPubSub extends EventEmitterPubSub {
  live = new Map<string, number>();
  everSubscribed = new Set<string>();
  override async subscribe(topic: string, cb: any, options?: any) {
    this.everSubscribed.add(topic);
    this.live.set(topic, (this.live.get(topic) ?? 0) + 1);
    return super.subscribe(topic, cb, options);
  }
  override async unsubscribe(topic: string, cb: any) {
    const n = (this.live.get(topic) ?? 0) - 1;
    if (n > 0) this.live.set(topic, n);
    else this.live.delete(topic);
    return super.unsubscribe(topic, cb);
  }
  /** Topics containing `runId` that were subscribed at some point. */
  everForRun(runId: string) {
    return [...this.everSubscribed].filter(t => t.includes(runId)).map(t => t.replace(runId, '<runId>'));
  }
  /** Live subscriptions on topics that contain `runId` (agent.stream.<runId>, agent.control.<runId>, ...). */
  forRun(runId: string) {
    return [...this.live.entries()]
      .filter(([topic]) => topic.includes(runId))
      .map(([topic, n]) => `${topic.replace(runId, '<runId>')} x${n}`);
  }
}

/** Subscribe to a thread with run statuses and keep a compact log of what arrived. */
export async function observeThread(agent: any, resourceId: string, threadId: string) {
  const sub = await agent.subscribeToThread({ resourceId, threadId, withRunStatus: true });
  const log: string[] = [];
  const statuses: string[] = [];
  void (async () => {
    try {
      for await (const c of sub.stream as AsyncIterable<any>) {
        if (c.type === 'thread-run-status') {
          statuses.push(c.payload?.status);
          log.push(`thread-run-status:${c.payload?.status}`);
        } else if (['start', 'step-start', 'text-delta', 'tool-call-suspended', 'finish', 'error', 'abort'].includes(c.type)) {
          log.push(c.type === 'text-delta' ? `text-delta(${JSON.stringify(c.payload?.text)})` : c.type);
        }
      }
    } catch (e) {
      log.push(`stream error: ${(e as Error).message}`);
    }
  })();
  return {
    log,
    statuses,
    /** Wait until a status (or a chunk type) in `want` arrives. */
    async until(want: string[], ms = 20_000) {
      const hit = () => statuses.find(s => want.includes(s)) ?? log.find(l => want.includes(l));
      const deadline = Date.now() + ms;
      while (Date.now() < deadline && !hit()) await sleep(25);
      return hit();
    },
    close: () => sub.unsubscribe?.(),
  };
}
