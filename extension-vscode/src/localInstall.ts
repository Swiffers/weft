// Which install this window's `weft` acts on, and the folder that install
// keeps its files in, where the editor watches its `ports.json` to follow
// a start on another port. Its address and its key come from the CLI
// (`localAccess` in installs.ts), which owns how both are read.

import * as fs from 'node:fs';
import * as path from 'node:path';

/// weft's data folder on this machine: the default install's folder, with
/// every named install under its `installs/`.
// SYNC: dataDir <-> crates/weft-core/src/infra/install.rs (data_dir), scripts/lib/data-dir.sh (weft_state_dir)
export function dataDir(env: NodeJS.ProcessEnv): string {
  const root = env.HOME;
  if (!root) throw new Error("HOME is not set: weft keeps every install's files under $HOME/.local/share/weft");
  return path.join(root, '.local', 'share', 'weft');
}

/// What `WEFT_INSTALL` holds to name the default install explicitly.
// SYNC: DEFAULT_INSTALL_ENV <-> crates/weft-core/src/infra/install.rs (DEFAULT_LABEL, Install::from_env_value)
const DEFAULT_INSTALL_ENV = 'default';

/// The named install `WEFT_INSTALL` picks, `undefined` for the default
/// one (unset, empty, or `default`).
// SYNC: WEFT_INSTALL <-> crates/weft-core/src/infra/install.rs (INSTALL_ENV)
function installName(env: NodeJS.ProcessEnv): string | undefined {
  const value = env.WEFT_INSTALL?.trim();
  return value && value !== DEFAULT_INSTALL_ENV ? value : undefined;
}

/// The folder an install keeps its files in: weft's data folder for the
/// default install, `installs/<name>` under it for the one `WEFT_INSTALL`
/// names.
// SYNC: installDir <-> crates/weft-core/src/infra/install.rs (Install::dir, Install::from_env_value)
export function installDir(env: NodeJS.ProcessEnv = process.env): string {
  const root = dataDir(env);
  const name = installName(env);
  return name ? path.join(root, 'installs', name) : root;
}

// What `weft.dispatcherUrl` defaulted to before the extension read
// `ports.json`. VS Code wrote it into the settings of anybody who edited
// the field, and there it would beat the saved port while pointing at an
// address no install answers on, so it counts as unset.
export const RETIRED_DEFAULT_URL = 'http://localhost:9999';

/// The `weft.dispatcherUrl` setting as the address to ask for: `undefined`
/// when it is unset or holds the retired default.
export function settingAddress(setting: string | undefined): string | undefined {
  return setting && setting !== RETIRED_DEFAULT_URL ? setting : undefined;
}

/// What to check when the local install does not answer: the two places
/// its address comes from.
export function unreachableHint(setting: string | undefined, env: NodeJS.ProcessEnv = process.env): string {
  const file = path.join(installDir(env), 'ports.json');
  return settingAddress(setting)
    ? `the address comes from the \`weft.dispatcherUrl\` setting (${setting}); clear it to use ${file}`
    : `the address comes from ${file}; is the install started (\`weft daemon start\`), or should \`weft.dispatcherUrl\` point elsewhere?`;
}

/// The file a weft checkout records its install in once `./setup.sh`
/// installed it: one line, the install's absolute folder.
// SYNC: CHECKOUT_INSTALL_FILE <-> scripts/lib/checkout-install.sh (weft_install_record, weft_record_checkout_install:
//       setup.sh writes it through them), scripts/dev-editor.sh (reads it), .gitignore
export const CHECKOUT_INSTALL_FILE = '.weft-install';

/// The `weft` this window runs and the environment it runs it in. The
/// environment is also where the window reads which install it talks to
/// (`installDir`, `localAccess`), so the CLI and the address always name
/// the same install.
export interface WeftCli {
  command: string;
  env: NodeJS.ProcessEnv;
}

/// Which `weft` this window runs, or why it cannot run one.
export type WhichCli = WeftCli | { error: string };

/// The installed extension runs `weft` from PATH, in the editor's own
/// environment. An extension loaded from a checkout's source
/// (`development`, through `scripts/dev-editor.sh`) whose checkout
/// recorded its install runs that install's own `weft`, with
/// `WEFT_INSTALL` naming it: the CLI otherwise picks its install from the
/// folder it runs in, which for a project outside the checkout is the
/// default install. A recorded install with no `weft` in it is an error,
/// never a quiet fall back to the one on PATH, which would be the default
/// install's.
export function whichCli(
  development: boolean,
  extensionPath: string,
  env: NodeJS.ProcessEnv = process.env,
): WhichCli {
  const onPath: WeftCli = { command: 'weft', env };
  if (!development) return onPath;
  const checkout = path.dirname(extensionPath);
  const file = path.join(checkout, CHECKOUT_INSTALL_FILE);
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return onPath;
    return { error: `cannot read ${file}: ${e}` };
  }
  const setup = `run ./setup.sh in ${checkout}`;
  const dir = raw.trim();
  if (!path.isAbsolute(dir)) return { error: `${file} does not hold an install's absolute folder; ${setup}` };
  let root: string;
  try {
    root = dataDir(env);
  } catch (e) {
    return { error: (e as Error).message };
  }
  const resolved = path.resolve(dir);
  let install: string;
  if (resolved === root) install = DEFAULT_INSTALL_ENV;
  else if (path.dirname(resolved) === path.join(root, 'installs')) install = path.basename(resolved);
  else return { error: `${file} names ${dir}, which is not a weft install's folder (those are ${root} and ${path.join(root, 'installs')}/<name>); ${setup}` };
  const command = path.join(resolved, 'bin', 'weft');
  if (!fs.existsSync(command)) return { error: `${file} names the install at ${dir}, which has no ${command}; ${setup}` };
  return { command, env: { ...env, WEFT_INSTALL: install } };
}
