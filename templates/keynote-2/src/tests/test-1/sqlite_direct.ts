import { rpc_single_call } from '../../scenario_recipes/rpc_single_call.ts';

export default {
  system: 'sqlite_direct' as const,
  label: 'sqlite_direct:rpc_single_call',
  run: rpc_single_call,
};
