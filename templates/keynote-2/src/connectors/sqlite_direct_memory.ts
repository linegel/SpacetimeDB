import sqlite_direct from './sqlite_direct.ts';
import type { RpcConnector } from '../core/connectors.ts';

/**
 * In-memory variant of sqlite_direct. Each instance gets its own independent
 * :memory: database, auto-seeded on open(). No cross-thread state — measures
 * pure SQLite throughput as an upper-bound benchmark.
 */
export default function sqlite_direct_memory(): RpcConnector {
  const connector = sqlite_direct(':memory:');
  const origOpen = connector.open.bind(connector);

  connector.name = 'sqlite_direct_memory';

  connector.open = async () => {
    await origOpen();
    // Auto-seed since each :memory: DB starts empty
    const accounts = Number(process.env.SEED_ACCOUNTS ?? 100_000);
    const initialBalance = Number(process.env.SEED_INITIAL_BALANCE ?? 10_000_000);
    await connector.call('seed', { accounts, initialBalance });
  };

  connector.createWorker = async () => {
    const worker = sqlite_direct_memory();
    await worker.open();
    return worker;
  };

  return connector;
}
