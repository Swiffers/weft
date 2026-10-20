// includeRelativeToFile turns an `@include` path into the path the editor
// joins onto the including file's directory, the way the compiler reads
// it (`weft_compiler::file_reader::catalog_path`): a `__catalog/` path
// names a file under the project's `nodes/` from wherever it is written.

import { describe, it, expect } from 'vitest';
import { includeRelativeToFile } from './includePath';

const root = '/p';

describe('includeRelativeToFile', () => {
  it('keeps an ordinary include as written', () => {
    expect(includeRelativeToFile('lib/clean.weft', '/p/src/main.weft', root)).toBe('lib/clean.weft');
    expect(includeRelativeToFile('../shared/x.weft', '/p/src/a/b.weft', root)).toBe('../shared/x.weft');
  });

  it('reads a __catalog/ include from nodes/ wherever it is written', () => {
    const path = '__catalog/base_catalog/computer/agent.weft';
    expect(includeRelativeToFile(path, '/p/src/main.weft', root)).toBe('../nodes/base_catalog/computer/agent.weft');
    expect(includeRelativeToFile(path, '/p/src/deep/er/x.weft', root)).toBe('../../../nodes/base_catalog/computer/agent.weft');
    expect(includeRelativeToFile('__catalog/my_pkg/x.weft', '/p/nodes/my_pkg/y.weft', root)).toBe('x.weft');
  });

  it('refuses a __catalog/ include that climbs out of nodes/, even one that comes back', () => {
    expect(includeRelativeToFile('__catalog/../src/x.weft', '/p/src/main.weft', root)).toBeUndefined();
    expect(includeRelativeToFile('__catalog/a/../../nodes/x.weft', '/p/src/main.weft', root)).toBeUndefined();
    expect(includeRelativeToFile('__catalog/a/../x.weft', '/p/src/main.weft', root)).toBe('../nodes/x.weft');
  });
});
