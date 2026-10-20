import { INSTANCE_ID_PATTERN } from '../../../run-spec';

/** A content hash: 64 lowercase hex digits (sha256). */
const CONTENT_HASH_PATTERN = /^[0-9a-f]{64}$/;

/// Is `path` a tenant-less storage key, a stored runtime file rather than a
/// path in the project folder? The grammar, segment by segment:
///   `run|project|shared/<owner>/<id>`
///   `instance/<project>/<instance>/<id>`
///   `asset/<sha256>`
/// Every owner and id segment has the storage segment grammar (the same one
/// an instance id has), so a project folder named `asset` or `run` is never
/// mistaken for a key.
// SYNC: isScopeKey <-> crates/weft-core/src/storage/key.rs is_scope_key
export function isScopeKey(path: string): boolean {
  const parts = path.split('/');
  const segment = (s: string) => INSTANCE_ID_PATTERN.test(s);
  if (parts.length === 2) return parts[0] === 'asset' && CONTENT_HASH_PATTERN.test(parts[1]);
  if (parts.length === 4) return parts[0] === 'instance' && parts.slice(1).every(segment);
  if (parts.length === 3) {
    return ['run', 'project', 'shared'].includes(parts[0]) && segment(parts[1]) && segment(parts[2]);
  }
  return false;
}
