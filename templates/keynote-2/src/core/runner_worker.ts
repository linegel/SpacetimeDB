/**
 * Worker thread entry point for multi-threaded benchmarking.
 * Each thread gets its own event loop, HTTP connections, HDR histogram,
 * and collision tracker — eliminating the single-event-loop bottleneck.
 */
import { parentPort, workerData } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import hdr from 'hdr-histogram-js';
import { CONNECTORS } from '../connectors/index.ts';
import { makeCollisionTracker } from './collision_tracker.ts';

if (!parentPort) {
  throw new Error('runner_worker.ts must be run as a worker thread, not as a main script');
}
const port = parentPort;

export interface WorkerInput {
  connectorSystem: string;
  seconds: number;
  concurrency: number;
  workerThreadIndex: number;
  totalWorkerThreads: number;
  accounts: number;
  alpha: number;
  pairCount: number;
  fromBuffer: SharedArrayBuffer;
  toBuffer: SharedArrayBuffer;
  pairStartOffset: number;
  pairsForThisThread: number;
  pipelined: boolean;
  maxInflightPerWorker: number;
  opTimeoutMs: number;
  minOpTimeoutMs: number;
  tailSlackMs: number;
}

export interface WorkerResult {
  completedWithinWindow: number;
  completedTotal: number;
  histogramBase64: string;
  collisionTotal: number;
  collisionCount: number;
}

async function withOpTimeout<T>(
  promise: Promise<T>,
  label: string,
  timeoutMs: number,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`[worker] ${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return (await Promise.race([promise, timeoutPromise])) as T;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function run() {
  const input = workerData as WorkerInput;
  const {
    connectorSystem,
    seconds,
    concurrency,
    pairCount,
    fromBuffer,
    toBuffer,
    pairStartOffset,
    pairsForThisThread,
    pipelined,
    maxInflightPerWorker,
    opTimeoutMs,
    minOpTimeoutMs,
    tailSlackMs,
  } = input;

  // Reconstruct connector from system name
  const connectorFactory = (CONNECTORS as any)[connectorSystem];
  if (!connectorFactory) {
    throw new Error(`Unknown connector system: ${connectorSystem}`);
  }

  // Wrap SharedArrayBuffers as typed arrays
  const fromArr = new Uint32Array(fromBuffer);
  const toArr = new Uint32Array(toBuffer);

  // Load the test module to get the scenario function
  const testMod = await import(`../tests/test-1/${connectorSystem}.ts`);
  const scenario = testMod.default.run as (
    conn: unknown,
    from: number,
    to: number,
    amount: number,
  ) => Promise<void>;

  // Build per-thread histogram and collision tracker
  const hist = hdr.build({
    lowestDiscernibleValue: 1,
    highestTrackableValue: 10_000_000_000,
    numberOfSignificantValueDigits: 3,
  });
  const collisionTracker = makeCollisionTracker();

  // Create worker connections (one per async concurrency slot)
  const rootConnector = connectorFactory();
  await rootConnector.open(concurrency);

  const hasWorkerFactory = typeof rootConnector.createWorker === 'function';
  const workers: unknown[] = [];

  if (hasWorkerFactory) {
    for (let i = 0; i < concurrency; i++) {
      const w = await rootConnector.createWorker({ index: i, total: concurrency });
      workers.push(w);
    }
  } else {
    for (let i = 0; i < concurrency; i++) {
      workers.push(rootConnector);
    }
  }

  const start = performance.now();
  const endAt = start + seconds * 1000;

  let completedWithinWindow = 0;
  let completedTotal = 0;

  // Each async worker runs within this thread
  async function asyncWorker(workerIndex: number) {
    const conn = workers[workerIndex];
    const pairsPerWorker = Math.max(1, Math.floor(pairsForThisThread / concurrency));
    let pairIdx = pairStartOffset + workerIndex * pairsPerWorker;

    const nextPair = (): [number, number] => {
      // Wrap around within the full precomputed array
      if (pairIdx >= pairCount) pairIdx = 0;
      const f = fromArr[pairIdx]!;
      const t = toArr[pairIdx]!;
      pairIdx++;
      return [f, t];
    };

    if (!pipelined) {
      // Non-pipelined: one op at a time
      while (true) {
        const now = performance.now();
        if (now >= endAt) break;

        const timeLeft = endAt - now;
        const dynamicTimeout = Math.max(
          minOpTimeoutMs,
          Math.min(opTimeoutMs, timeLeft + tailSlackMs),
        );

        const [from, to] = nextPair();
        collisionTracker.begin(from);
        collisionTracker.begin(to);

        const t0 = performance.now();
        let ok = false;
        try {
          await withOpTimeout(
            scenario(conn as unknown, from, to, 1),
            `${connectorSystem} ${from}->${to}`,
            dynamicTimeout,
          );
          ok = true;
        } catch (err) {
          if (process.env.LOG_ERRORS === '1') {
            const msg = err instanceof Error ? err.message : String(err);
            console.warn(`[worker-${input.workerThreadIndex}] ${from}->${to}: ${msg}`);
          }
        } finally {
          collisionTracker.end(from);
          collisionTracker.end(to);
        }

        const t1 = performance.now();
        if (ok) {
          completedTotal++;
          if (t1 <= endAt) {
            completedWithinWindow++;
            hist.recordValue(Math.max(1, Math.round((t1 - t0) * 1e3)));
          }
        }
      }
      return;
    }

    // Pipelined mode
    const inflight = new Set<Promise<void>>();
    const unlimitedInflight = !Number.isFinite(maxInflightPerWorker);

    const launchOp = (dynamicTimeout: number) => {
      const [from, to] = nextPair();
      collisionTracker.begin(from);
      collisionTracker.begin(to);

      const p = (async () => {
        const t0 = performance.now();
        try {
          await withOpTimeout(
            scenario(conn as unknown, from, to, 1),
            `${connectorSystem} ${from}->${to}`,
            dynamicTimeout,
          );
          const t1 = performance.now();
          completedTotal++;
          if (t1 <= endAt) {
            completedWithinWindow++;
            hist.recordValue(Math.max(1, Math.round((t1 - t0) * 1e3)));
          }
        } catch (err) {
          if (process.env.LOG_ERRORS === '1') {
            const msg = err instanceof Error ? err.message : String(err);
            console.warn(`[worker-${input.workerThreadIndex}] ${from}->${to}: ${msg}`);
          }
        } finally {
          collisionTracker.end(from);
          collisionTracker.end(to);
        }
      })();

      inflight.add(p);
      p.finally(() => inflight.delete(p));
    };

    while (true) {
      const now = performance.now();
      if (now >= endAt) break;

      const timeLeft = endAt - now;
      const dynamicTimeout = Math.max(
        minOpTimeoutMs,
        Math.min(opTimeoutMs, timeLeft + tailSlackMs),
      );

      if (unlimitedInflight || inflight.size < maxInflightPerWorker) {
        launchOp(dynamicTimeout);
      } else {
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    }

    await Promise.all(inflight);
  }

  console.log(
    `[thread-${input.workerThreadIndex}] Starting ${concurrency} async workers for ${seconds}s...`,
  );

  // Run all async workers in this thread
  await Promise.all(
    Array.from({ length: concurrency }, (_, i) => asyncWorker(i)),
  );

  // Clean up connections
  if (hasWorkerFactory) {
    for (const w of workers) {
      const c = w as { close?: () => Promise<void> };
      if (typeof c.close === 'function') {
        try { await c.close(); } catch {}
      }
    }
  }
  await rootConnector.close();

  // Encode histogram and send results
  const histogramBase64 = hdr.encodeIntoCompressedBase64(hist);
  const cStats = collisionTracker.stats();

  const result: WorkerResult = {
    completedWithinWindow,
    completedTotal,
    histogramBase64,
    collisionTotal: cStats.total,
    collisionCount: cStats.collisions,
  };

  port.postMessage(result);
}

run().catch((err) => {
  console.error(`[worker-${(workerData as WorkerInput).workerThreadIndex}] Fatal:`, err);
  process.exit(1);
});
