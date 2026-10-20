import { describe, expect, it } from 'vitest';

import { isScopeKey } from './storage-key';

// Mirrors crates/weft-core/src/storage/key.rs is_scope_key_accepts_tenant_less_keys_only.
describe('isScopeKey', () => {
  const sha = 'a'.repeat(64);

  it.each(['run/c1/f1', 'project/p1/f2', 'shared/team/f3', `asset/${sha}`, 'instance/p1/ada/f5'])(
    'accepts %s',
    (key) => expect(isScopeKey(key)).toBe(true),
  );

  it.each([
    'asset/logo.png', // a project folder called `asset`
    'asset/p1/f4', // the old per-project asset shape
    `asset/${'A'.repeat(64)}`, // a hash is lowercase hex
    't1/project/p1/f2', // tenant-anchored (4 segments)
    'instance/p1/f5', // instance scope needs its instance segment
    'run/a/b/c', // 4 segments under a 3-segment scope
    'assets/pic.png', // ordinary project path
    'project/p1', // missing id
    'project//f', // empty owner
    'project/../f', // traversal segment
    'banana/p1/f', // unknown scope tag
    'https://ex.com/a', // URL
  ])('rejects %s', (key) => expect(isScopeKey(key)).toBe(false));
});
