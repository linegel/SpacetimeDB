import Database from 'better-sqlite3';
import { applySqlitePragmas, getSqliteMode, ensureSqliteDirExistsSync } from './sqlite_common.ts';
import type { RpcConnector } from '../core/connectors.ts';

/**
 * Direct better-sqlite3 connector — bypasses HTTP/JSON RPC server entirely.
 * Each instance opens its own DB connection with prepared statements compiled once.
 * Implements RpcConnector so rpc_single_call scenario works unchanged.
 */
export default function sqlite_direct(
  dbPath: string = process.env.SQLITE_FILE ?? './.data/accounts.sqlite',
): RpcConnector {
  let db: Database.Database | null = null;

  // Prepared statements — compiled once at open(), reused for every call
  let selectTwo: Database.Statement | null = null;
  let updateBal: Database.Statement | null = null;
  let doTransfer: Database.Transaction<(fromId: number, toId: number, amount: number) => void> | null = null;
  let selectOne: Database.Statement | null = null;
  let sumBalance: Database.Statement | null = null;
  let countAccounts: Database.Statement | null = null;

  function ensureDb(): Database.Database {
    if (!db) throw new Error('[sqlite_direct] not open');
    return db;
  }

  function execTransfer(args: Record<string, any>): void {
    const { from_id, to_id, amount } = args;
    doTransfer!(from_id, to_id, amount);
  }

  function execSeed(args: Record<string, any>): void {
    const d = ensureDb();
    const { accounts, initialBalance } = args;

    // Drop and recreate for clean seed
    d.prepare('DROP TABLE IF EXISTS accounts').run();
    d.prepare(`CREATE TABLE IF NOT EXISTS accounts (
      id INTEGER PRIMARY KEY, balance INTEGER NOT NULL
    )`).run();

    const insert = d.prepare('INSERT INTO accounts (id, balance) VALUES (?, ?)');
    const batchInsert = d.transaction((start: number, end: number) => {
      for (let i = start; i < end; i++) {
        insert.run(i, initialBalance);
      }
    });

    // Insert in batches of 10k for efficiency
    const BATCH = 10_000;
    for (let i = 0; i < accounts; i += BATCH) {
      batchInsert(i, Math.min(i + BATCH, accounts));
    }

    console.log(`[sqlite_direct] seeded ${accounts} accounts with balance=${initialBalance}`);
  }

  function execVerify(): { totalBalance: number; accountCount: number } {
    ensureDb();
    const total = (sumBalance!.get() as any)?.total ?? 0;
    const count = (countAccounts!.get() as any)?.count ?? 0;
    return { totalBalance: Number(total), accountCount: Number(count) };
  }

  function execGetAccount(args: Record<string, any>): { id: number; balance: bigint } | null {
    const row = selectOne!.get(args.id) as { id: number; balance: number } | undefined;
    if (!row) return null;
    return { id: row.id, balance: BigInt(row.balance) };
  }

  const connector: RpcConnector = {
    name: 'sqlite_direct',

    async open() {
      const mode = getSqliteMode();
      const isMemory = dbPath === ':memory:' || mode === 'fastest';
      const actualPath = isMemory ? ':memory:' : dbPath;

      if (!isMemory) {
        ensureSqliteDirExistsSync(actualPath, mode);
      }

      db = new Database(actualPath);
      applySqlitePragmas(db, mode);

      // Ensure schema exists
      db.prepare(`CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY, balance INTEGER NOT NULL
      )`).run();

      // Compile prepared statements once
      selectTwo = db.prepare('SELECT id, balance FROM accounts WHERE id IN (?, ?)');
      updateBal = db.prepare('UPDATE accounts SET balance = ? WHERE id = ?');
      selectOne = db.prepare('SELECT id, balance FROM accounts WHERE id = ?');
      sumBalance = db.prepare('SELECT SUM(balance) as total FROM accounts');
      countAccounts = db.prepare('SELECT COUNT(*) as count FROM accounts');

      // Wrap transfer as a compiled transaction for max throughput
      doTransfer = db.transaction((fromId: number, toId: number, amount: number) => {
        const rows = selectTwo!.all(fromId, toId) as { id: number; balance: number }[];
        if (rows.length !== 2) return; // one or both accounts missing
        const fromRow = rows[0].id === fromId ? rows[0] : rows[1];
        const toRow = rows[0].id === fromId ? rows[1] : rows[0];
        if (fromRow.balance < amount) return; // insufficient funds
        updateBal!.run(fromRow.balance - amount, fromId);
        updateBal!.run(toRow.balance + amount, toId);
      });
    },

    async close() {
      if (db) {
        db.close();
        db = null;
        selectTwo = null;
        updateBal = null;
        doTransfer = null;
        selectOne = null;
        sumBalance = null;
        countAccounts = null;
      }
    },

    async call(name: string, args?: Record<string, any>) {
      switch (name) {
        case 'transfer':
          execTransfer(args!);
          return;
        case 'seed':
          execSeed(args!);
          return;
        case 'health':
          return { status: 'ok' };
        case 'verify':
          return execVerify();
        case 'getAccount':
          return execGetAccount(args!);
        default:
          throw new Error(`[sqlite_direct] unknown method: ${name}`);
      }
    },

    async getAccount(id: number) {
      return execGetAccount({ id });
    },

    async verify() {
      execVerify();
    },

    async createWorker() {
      // Each worker gets its own connection, already opened
      const worker = sqlite_direct(dbPath);
      await worker.open();
      return worker;
    },
  };

  return connector;
}
