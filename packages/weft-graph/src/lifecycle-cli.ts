// The `weft` command a lifecycle verb from the graph runs. The webview
// says what the person picked (`LifecycleAction`); every host spells it
// as the CLI's verb and flags through here, so the flags exist once.

import type { InstanceTarget, LifecycleAction, TakeDownChoice } from './protocol';

/// The CLI verb, then its arguments: `{ verb: 'infra', args: ['stop',
/// '--infra', 'db', ...] }` runs `weft infra stop --infra db ...`.
// SYNC: lifecycleCommand <-> crates/weft-cli/src/commands/activate.rs, deactivate.rs, resync.rs, infra.rs (their flags)
export function lifecycleCommand(action: LifecycleAction): { verb: string; args: string[] } {
  switch (action.verb) {
    case 'activate':
      return {
        verb: 'activate',
        args: [
          ...repeated('--trigger', action.triggers),
          ...instanceFlags(action.instances),
          ...(action.held ? ['--held', action.held] : []),
        ],
      };
    case 'deactivate':
    case 'resync':
      return {
        verb: action.verb,
        args: [
          ...repeated('--trigger', action.triggers),
          ...instanceFlags(action.instances),
          ...takeDownFlags(action.takeDown),
        ],
      };
    case 'infra_start':
      return { verb: 'infra', args: ['start', ...repeated('--infra', action.infra), ...instanceFlags(action.instances)] };
    case 'infra_stop':
    case 'infra_terminate':
    case 'infra_upgrade':
      return {
        verb: 'infra',
        args: [
          action.verb.slice('infra_'.length),
          ...repeated('--infra', action.infra),
          ...instanceFlags(action.instances),
          ...takeDownFlags(action.takeDown),
        ],
      };
  }
}

function repeated(flag: string, values: string[]): string[] {
  return values.flatMap((value) => [flag, value]);
}

/// No flag for the shared ones, `--instance` per instance named,
/// `--all-instances` for every instance, `--all` for the shared ones and
/// every instance.
// SYNC: instanceFlags <-> crates/weft-core/src/instance.rs InstanceTarget::from_flags
export function instanceFlags(instances: InstanceTarget | undefined): string[] {
  if (instances === undefined) return [];
  if (instances === 'all_instances') return ['--all-instances'];
  if (instances === 'all') return ['--all'];
  return repeated('--instance', instances);
}

/// Only the answers given: one left out is the command's default.
function takeDownFlags(choice: TakeDownChoice): string[] {
  return [
    ...(choice.door ? ['--door', choice.door] : []),
    ...(choice.doorForMinutes !== undefined ? ['--door-for', String(choice.doorForMinutes)] : []),
    ...(choice.held ? ['--held', choice.held] : []),
    ...(choice.running ? ['--running', choice.running] : []),
  ];
}
