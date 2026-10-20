import { describe, expect, it } from 'vitest';
import { storedFilePickerRows, type StoredFileRow } from './storedFilePicker';

const PID = '11111111-1111-1111-1111-111111111111';
const OTHER = '22222222-2222-2222-2222-222222222222';
const HASH = 'a'.repeat(64);

function row(key: string): StoredFileRow {
  return { key, filename: 'f.png', mimeType: 'image/png', sizeBytes: 3 };
}

function keys(listing: string[], projectId = PID): string[] {
  return storedFilePickerRows(listing.map(row), projectId).map((r) => r.key);
}

describe('storedFilePickerRows', () => {
  it('offers this project\'s own, its instances\' and the shared files, tenant-less', () => {
    expect(keys([
      `local/project/${PID}/report`,
      `local/instance/${PID}/alice/avatar`,
      'local/shared/brand/logo',
    ])).toEqual([`project/${PID}/report`, `instance/${PID}/alice/avatar`, 'shared/brand/logo']);
  });

  it('leaves out what this project\'s runs cannot read', () => {
    expect(keys([
      `local/project/${OTHER}/report`,
      `local/instance/${OTHER}/alice/avatar`,
      'local/run/0190f0f0-0000-7000-8000-000000000000/scratch',
    ])).toEqual([]);
  });

  it('leaves out the tenant\'s assets, the copies of files sources name by path', () => {
    expect(keys([`local/asset/${HASH}`])).toEqual([]);
  });

  it('refuses a key whose shape is not its scope\'s', () => {
    expect(keys([
      `local/project/${PID}`,
      `local/project/${PID}/a/b`,
      `local/instance/${PID}/alice`,
      'local/shared/brand',
      'no-tenant',
    ])).toEqual([]);
  });

  it('keeps the row\'s metadata', () => {
    expect(storedFilePickerRows([row(`local/project/${PID}/report`)], PID)).toEqual([
      { key: `project/${PID}/report`, filename: 'f.png', mimeType: 'image/png', sizeBytes: 3 },
    ]);
  });
});
