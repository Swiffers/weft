import * as nodePath from 'path';

/// The first segment of an `@include` path read from the project's
/// `nodes/` folder, wherever the include is written:
/// `@include("__catalog/base_catalog/computer/agent.weft")`.
// SYNC: CATALOG_PREFIX <-> crates/weft-compiler/src/project.rs CATALOG_PREFIX
export const CATALOG_PREFIX = '__catalog';
// SYNC: NODES_DIR <-> crates/weft-compiler/src/project.rs NODES_DIR
const NODES_DIR = 'nodes';

/// An `@include` path as the file that writes it would resolve it: the
/// path itself for an ordinary one (it is relative to that file), and
/// for a `__catalog/` one the same file under `<root>/nodes/`, spelled
/// relative to the including file's directory, so a caller joins either
/// onto that directory the same way (and a remote URI keeps its scheme).
/// `undefined` for a `__catalog/` path whose rest climbs out of `nodes/`,
/// which the compiler refuses too.
// SYNC: includeRelativeToFile <-> crates/weft-compiler/src/file_reader.rs catalog_path
export function includeRelativeToFile(written: string, includingFile: string, root: string): string | undefined {
  const segments = written.split('/');
  if (segments[0] !== CATALOG_PREFIX) return written;
  // The rest is normalized on its own, so a `..` that leaves `nodes/`
  // is refused even when a later segment comes back in.
  const rest: string[] = [];
  for (const segment of segments.slice(1)) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') {
      if (rest.length === 0) return undefined;
      rest.pop();
      continue;
    }
    rest.push(segment);
  }
  return nodePath.relative(nodePath.dirname(includingFile), nodePath.join(root, NODES_DIR, ...rest));
}
