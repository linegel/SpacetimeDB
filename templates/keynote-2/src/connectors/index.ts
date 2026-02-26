import convex from './convex.ts';
import bun from './bun.ts';
import postgres_rpc from './rpc/postgres_rpc.ts';
import cockroach_rpc from './rpc/cockroach_rpc.ts';
import sqlite_rpc from './rpc/sqlite_rpc.ts';
import supabase_rpc from './rpc/supabase_rpc.ts';
import planetscale_pg_rpc from './rpc/planetscale_pg_rpc.ts';
import sqlite_direct from './sqlite_direct.ts';
import sqlite_direct_memory from './sqlite_direct_memory.ts';

// spacetimedb imports module_bindings which may not exist in all Docker images
let spacetimedb: any = undefined;
try {
  const mod = await import('./spacetimedb.ts');
  spacetimedb = mod.spacetimedb;
} catch {
  // module_bindings not available — spacetimedb connector disabled
}

export const CONNECTORS: Record<string, any> = {
  convex,
  ...(spacetimedb ? { spacetimedb } : {}),
  bun,
  postgres_rpc,
  cockroach_rpc,
  sqlite_rpc,
  supabase_rpc,
  planetscale_pg_rpc,
  sqlite_direct,
  sqlite_direct_memory,
};
export type ConnectorKey = keyof typeof CONNECTORS;
