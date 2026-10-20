import { describe, it, expect } from 'vitest';
import { instanceFlags, lifecycleCommand } from './lifecycle-cli';

describe('the weft command a lifecycle verb runs', () => {
  it('names each trigger, and only the answers given', () => {
    expect(lifecycleCommand({ verb: 'deactivate', triggers: ['door', 'feed'], takeDown: { running: 'cancel' } })).toEqual({
      verb: 'deactivate',
      args: ['--trigger', 'door', '--trigger', 'feed', '--running', 'cancel'],
    });
    expect(lifecycleCommand({
      verb: 'resync',
      triggers: ['door'],
      instances: 'all_instances',
      takeDown: { door: 'hide', doorForMinutes: 30, held: 'drop' },
    })).toEqual({
      verb: 'resync',
      args: ['--trigger', 'door', '--all-instances', '--door', 'hide', '--door-for', '30', '--held', 'drop'],
    });
    expect(lifecycleCommand({ verb: 'resync', triggers: ['door'], takeDown: { running: 'wait' } }).args)
      .toEqual(['--trigger', 'door', '--running', 'wait']);
  });

  it('says what activate does with the held work only when asked', () => {
    expect(lifecycleCommand({ verb: 'activate', triggers: ['door'] }).args).toEqual(['--trigger', 'door']);
    expect(lifecycleCommand({ verb: 'activate', triggers: ['door'], held: 'drop' }).args).toEqual(['--trigger', 'door', '--held', 'drop']);
  });

  it('spells the infra verbs as `weft infra <verb> --infra`', () => {
    expect(lifecycleCommand({ verb: 'infra_start', infra: ['db'], instances: ['ada'] })).toEqual({
      verb: 'infra',
      args: ['start', '--infra', 'db', '--instance', 'ada'],
    });
    expect(lifecycleCommand({ verb: 'infra_terminate', infra: ['db'], takeDown: { running: 'cancel' } })).toEqual({
      verb: 'infra',
      args: ['terminate', '--infra', 'db', '--running', 'cancel'],
    });
  });

  it('names whose copies: none for the shared ones, each instance, every instance, or everything', () => {
    expect(instanceFlags(undefined)).toEqual([]);
    expect(instanceFlags(['ada', 'bob'])).toEqual(['--instance', 'ada', '--instance', 'bob']);
    expect(instanceFlags('all_instances')).toEqual(['--all-instances']);
    expect(instanceFlags('all')).toEqual(['--all']);
    expect(lifecycleCommand({ verb: 'infra_start', infra: ['db'], instances: 'all' }).args).toEqual(['start', '--infra', 'db', '--all']);
  });
});
