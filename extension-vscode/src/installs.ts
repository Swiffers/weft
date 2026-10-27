// The installs a project can be looked at on: the local one, and each
// target its `weft.toml` names (`[targets.<name>]`). The editor's install
// switch reads them here, and everything it needs from an install (its
// address, the key a request there carries, the program it holds) comes
// through the CLI, which owns `weft.toml`, `~/.config/weft/credentials.toml`
// and every local install's `ports.json` and `secrets.env`, so the editor
// never reads any of them on its own.

import * as fs from 'node:fs/promises';
import * as path from 'node:path';

import { LOCAL_INSTALL } from '../../packages/weft-graph/src/protocol';
import { runWeftJson } from './cli';
import { dataDir } from './localInstall';

/// One row of `weft target list --json`.
// SYNC: InstallTarget <-> crates/weft-cli/src/commands/target.rs (TargetAction::List)
export interface InstallTarget {
  name: string;
  url: string;
  loggedIn: boolean;
}

/// What `weft target key` answers (with `--on <name>`, `--dispatcher
/// <address>` or neither): where requests go and the key they carry.
// SYNC: InstallAccess <-> crates/weft-cli/src/commands/target.rs key
export interface InstallAccess {
  url: string;
  operatorKey: string | null;
}

/// The program an install holds, written into a folder by
/// `weft running-source`.
// SYNC: RunningSource <-> crates/weft-cli/src/commands/running_source.rs run
export interface RunningSource {
  version: string;
  dir: string;
}

/// The `--on` a command for `install` carries: none for the local one.
export function onArgs(install: string): string[] {
  return install === LOCAL_INSTALL ? [] : ['--on', install];
}

/// The project's installs, the local one first.
export function listTargets(projectRoot: string): Promise<InstallTarget[]> {
  return runWeftJson<InstallTarget[]>(['target', 'list', '--json'], projectRoot);
}

/// Where the editor reaches `install` and the key it carries there.
export function installAccess(projectRoot: string, install: string): Promise<InstallAccess> {
  return runWeftJson<InstallAccess>([...onArgs(install), 'target', 'key'], projectRoot);
}

/// Where this window's local install answers and the key a request there
/// carries, or why there is none. `address` is the `weft.dispatcherUrl`
/// setting when somebody set one: the key is then that of whichever
/// install on this machine answers there, none for an address elsewhere.
/// Asked from weft's data folder, outside every project and checkout, so
/// the install is the one the window's environment names (`installDir`).
export async function localAccess(
  address: string | undefined,
  env: NodeJS.ProcessEnv,
): Promise<{ url: string; operatorKey: string | null } | { error: string }> {
  // A machine with no weft folder has no install yet, and a CLI asked
  // from a folder that is not there fails as if it were not installed.
  const from = dataDir(env);
  try {
    await fs.access(from);
  } catch {
    return { error: `no weft install on this machine yet (${from} does not exist); run ./setup.sh from a weft checkout` };
  }
  try {
    const args = [...(address ? ['--dispatcher', address] : []), 'target', 'key'];
    return await runWeftJson<InstallAccess>(args, from);
  } catch (e) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

/// Write the program `install` holds into a fresh folder under `cacheRoot`
/// (one per project and install), with the project's own canvas layouts
/// beside it so the graph keeps the positions the person gave it. The
/// previous copies of that install go once the new one is whole.
export async function fetchRunningSource(
  projectRoot: string,
  projectId: string,
  install: string,
  cacheRoot: string,
): Promise<RunningSource> {
  const parent = path.join(cacheRoot, 'installs', projectId, install);
  await fs.mkdir(parent, { recursive: true });
  const dir = path.join(parent, String(Date.now()));
  const running = await runWeftJson<RunningSource>(
    [...onArgs(install), 'running-source', dir, '--json'],
    projectRoot,
  );
  await copyLayouts(projectRoot, running.dir);
  // The copy is what the install runs, never something to edit: a text
  // tab of it cannot be saved over.
  await setFilesWritable(running.dir, false);
  for (const old of await fs.readdir(parent)) {
    const oldDir = path.join(parent, old);
    if (oldDir !== running.dir) {
      // Some platforms refuse to delete a read-only file.
      await setFilesWritable(oldDir, true);
      await fs.rm(oldDir, { recursive: true, force: true });
    }
  }
  return running;
}

/// Make every file under `dir` writable by its owner, or read-only for
/// everyone. Folders keep their mode, so files can still be added and
/// removed.
async function setFilesWritable(dir: string, writable: boolean): Promise<void> {
  for (const entry of await fs.readdir(dir, { withFileTypes: true, recursive: true })) {
    if (!entry.isFile()) continue;
    await fs.chmod(path.join(entry.parentPath, entry.name), writable ? 0o644 : 0o444);
  }
}

/// `layouts/` is not part of a version (a canvas drag is not a program
/// change), so the copy has none; the local ones are the person's.
async function copyLayouts(projectRoot: string, dir: string): Promise<void> {
  const from = path.join(projectRoot, 'layouts');
  try {
    await fs.cp(from, path.join(dir, 'layouts'), { recursive: true });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
  }
}
