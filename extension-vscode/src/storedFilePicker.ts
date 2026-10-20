// Which stored runtime files the "pick a stored file" picker offers for one
// project. A picked file is written into source as its tenant-less key
// (`@asset("<scope-key>", <Type>)`), the build looks it up in the tenant's
// listing, and at run time the project's worker reads it through the
// storage wall. So the picker lists exactly the keys that wall lets this
// project's runs read and that outlive a run:
//   `project/<project>/<id>`                this project's files
//   `instance/<project>/<instance>/<id>`    any instance's files in this project
//   `shared/<name>/<id>`                    the tenant's files shared by name
// Left out: `run/...` (walled to the one run that made it, so no later run
// can read it), another project's `project/` or `instance/` files (the wall
// denies them), and `asset/<sha256>`. An asset is the store's copy of a file
// some source names by its path (every file of every version snapshot, of
// every project of the tenant, plus each media `@asset`), so listing them
// floods the picker with source files, and a field reaches one through the
// path it already has on disk.
// SYNC: storedFilePickerRows <-> crates/weft-core/src/storage/key.rs check_key_access

export type StoredFileRow = { key: string; filename: string; mimeType: string; sizeBytes: number };

/** The rows of `files` (the dispatcher's tenant listing, keys
 *  `<tenant>/<scope-key>`) that project `projectId` can use as a field
 *  value, each with its key made tenant-less. */
export function storedFilePickerRows(files: StoredFileRow[], projectId: string): StoredFileRow[] {
  return files.flatMap((f) => {
    const slash = f.key.indexOf('/');
    if (slash < 0) return [];
    const scopeKey = f.key.slice(slash + 1);
    if (!readableByProject(scopeKey, projectId)) return [];
    return [{ key: scopeKey, filename: f.filename, mimeType: f.mimeType, sizeBytes: f.sizeBytes }];
  });
}

function readableByProject(scopeKey: string, projectId: string): boolean {
  const parts = scopeKey.split('/');
  switch (parts[0]) {
    case 'project':
      return parts.length === 3 && parts[1] === projectId;
    case 'instance':
      return parts.length === 4 && parts[1] === projectId;
    case 'shared':
      return parts.length === 3;
    default:
      return false;
  }
}
