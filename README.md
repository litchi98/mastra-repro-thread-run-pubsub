# Repro: thread-API runs — PubSub release after a suspension, and `failed` on shutdown hand-back

Minimal reproductions for two Mastra bug reports about `DurableAgent` runs started through the thread API (`sendMessage` / `sendStreamResume` / `subscribeToThread`).

- No API keys, no network: the model is a `MockLanguageModelV2` from `ai/test`.
- Storage: libsql (in-memory, or a file under `.tmp/`). PubSub: `EventEmitterPubSub` / `UnixSocketPubSub` from `@mastra/core/events`, so no Redis is needed.
- Versions: `@mastra/core@1.83.0`, `@mastra/memory@1.41.1`, `@mastra/libsql@1.30.0`. Same results with `@mastra/core@1.84.0-alpha.2`.

Environment: Node 26.13.0, npm 11.20.0, macOS 27 (arm64). Runtime only: no `mastra build` / `mastra dev` involved.

```bash
npm install
npm run repro:subscriptions   # issue 1
npm run repro:shutdown        # issue 2
```

`shared.ts` holds the mock model, the tools (`ask_user` suspends, `lookup` returns right away), the agent factory, a subscription-counting `EventEmitterPubSub` subclass, and a `subscribeToThread` logger.

---

## 1. `subscriptions.ts`: a run that went through a suspension holds its PubSub subscriptions until `cleanupTimeoutMs`

Setup: `new Mastra({ backgroundTasks: { enabled: true } })`, so `sendMessage` runs through the idle wrapper, and a `DurableAgent` with `cleanupTimeoutMs: 10_000`.

- **A.** `sendMessage('hello')` → plain run → `success`.
- **B.** `sendMessage('please ask me something')` → the run suspends in `ask_user` → `sendStreamResume(...)` → `success`.

The script prints the live subscriptions on topics that contain the run id after each run settles.

### Actual output

```
@mastra/core 1.83.0, cleanupTimeoutMs = 10000

A. plain run (437a35fa-...)
  status: success
  topics subscribed during the run: [agent.stream.<runId>, agent.control.<runId>]
  +  500 ms after the run settled: live subscriptions = []
  + 3000 ms after the run settled: live subscriptions = []
  +11000 ms after the run settled: live subscriptions = []

B. run that suspends and is resumed (fdf12a2b-...)
  status: tool-call-suspended
  while suspended: live subscriptions = [agent.stream.<runId> x1, agent.control.<runId> x1]
  sendStreamResume() returned: {"accepted":true,"runId":"fdf12a2b-...","toolCallId":"call-..."}  (no cleanup handle)
  status: success
  topics subscribed during the run: [agent.stream.<runId>, agent.control.<runId>]
  +  500 ms after the run settled: live subscriptions = [agent.stream.<runId> x1, agent.control.<runId> x1]
  + 3000 ms after the run settled: live subscriptions = [agent.stream.<runId> x1, agent.control.<runId> x1]
  +11000 ms after the run settled: live subscriptions = []
```

With `NO_BACKGROUND_TASKS=1`, `sendMessage` skips the idle wrapper and both runs wait for the timer. The immediate release in A comes from the idle wrapper's `onForceClose → cleanup()`.

### Expected

Once the resumed run settles, its subscriptions are released right away, as in A. Alternatively, the caller gets a way to release them (`sendStreamResume` returns no cleanup handle).

With `@mastra/redis-streams`, each of those subscriptions is a blocking `XREADGROUP` connection. A larger `cleanupTimeoutMs` (for example, a longer `observe()` replay window) multiplies the connection count for every run that went through a suspension.

---

## 2. `shutdown.ts`: `mastra.shutdown()` hand-back is reported as `thread-run-status: failed`, then the same run continues

Three processes share one libsql file and one `UnixSocketPubSub`. All of them use `recovery: { durableAgents: 'auto' }`.

1. **observer** (`shutdown.ts`): `subscribeToThread({ withRunStatus: true })` on `thread-1` and logs what arrives.
2. **worker start**: `sendMessage('hello')`. Step 1 calls `lookup`. The step-2 model call hangs. After 1.5 s it calls `mastra.shutdown({ drainTimeout: 500 })`, as in a rolling restart.
3. **worker recover**: `mastra.recoverAllDurableAgents()`. The model answers right away.

### Actual output

```
@mastra/core 1.83.0
  [worker:start] sendMessage accepted, runId=01b7390b-...; step-2 model call is hanging
  [worker:start] mastra.shutdown({ drainTimeout: 500 })  <- e.g. SIGTERM during a rolling restart
  [worker:start] shutdown done
  [observer] after the hand-back: ["start","step-start","thread-run-status:failed"]
  [worker:recover] recoverAllDurableAgents() -> {"agents":1,"recovered":1,"succeeded":1,"failed":0,"skipped":0}
  [observer] after recovery:      ["start","step-start","thread-run-status:failed","step-start","text-delta(\"Answer produced after recovery.\")","finish","thread-run-status:success"]
  stored messages: ["signal","assistant"]
```

The observer gets `failed`, then more output and `success` for the **same runId** in the **same subscription**. The ChunkType reference says "`success`, `failed`, and `aborted` are final outcomes" and "Final outcomes are deduplicated within a subscription".

### Expected

The hand-back isn't reported as the final `failed` outcome. For example, it could be reported as a distinct, non-final status, or the subscriber could get no outcome until the run really ends. Then a client can show "reconnecting/recovering" instead of an error, and a real failure stays distinguishable.
