// Where the local install answers on this machine when `weft.dispatcherUrl`
// is unset: the public port it saved in its `ports.json` when it started,
// read the way the CLI reads it, so a port moved with `WEFT_PUBLIC_PORT`
// is found here too.

import * as fs from 'node:fs';
import * as path from 'node:path';

// The default install's address before its first start.
// SYNC: 14111 <-> crates/weft-core/src/ports.rs (PUBLIC, LOCAL_PUBLIC_URL),
// extension-browser/src/entrypoints/popup/App.svelte
export const DEFAULT_LOCAL_URL = 'http://127.0.0.1:14111';

/// weft's data folder on this machine: the default install's folder, with
/// every named install under its `installs/`.
// SYNC: dataDir <-> crates/weft-core/src/infra/install.rs (data_dir), scripts/lib/data-dir.sh (weft_state_dir)
function dataDir(env: NodeJS.ProcessEnv): string {
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

/// The local install's address. A default install never started takes
/// the default port on its first start; a named one never started has no
/// address, and pointing at the default install instead would talk to the
/// wrong one, so that throws.
// SYNC: <-> crates/weft-core/src/ports.rs (InstallPorts, local_public_url_in)
export function localInstallUrl(env: NodeJS.ProcessEnv = process.env): string {
  const dir = installDir(env);
  const file = path.join(dir, 'ports.json');
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error(`cannot read ${file}: ${e}`);
    const name = installName(env);
    if (!name) return DEFAULT_LOCAL_URL;
    throw new Error(`install '${name}' has no ports yet (${file} does not exist); start it with \`weft daemon start\``);
  }
  let port: unknown;
  try {
    port = (JSON.parse(raw) as { public?: unknown } | null)?.public;
  } catch (e) {
    throw new Error(`${file} is not valid: ${e}`);
  }
  if (typeof port !== 'number' || !Number.isInteger(port)) throw new Error(`${file} is not valid: no numeric \`public\` port`);
  return `http://127.0.0.1:${port}`;
}

// What `weft.dispatcherUrl` defaulted to before the extension read
// `ports.json`. VS Code wrote it into the settings of anybody who edited
// the field, and there it would beat the saved port while pointing at an
// address no install answers on, so it counts as unset.
export const RETIRED_DEFAULT_URL = 'http://localhost:9999';

/// Where the local install is, from the `weft.dispatcherUrl` setting and
/// the install's `ports.json`. `ignoredSetting` says the setting held the
/// retired default and was passed over.
export type LocalAddress =
  | { url: string; ignoredSetting: boolean }
  | { error: string; ignoredSetting: boolean };

export function localAddress(
  setting: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): LocalAddress {
  const ignoredSetting = setting === RETIRED_DEFAULT_URL;
  if (setting && !ignoredSetting) return { url: setting, ignoredSetting };
  try {
    return { url: localInstallUrl(env), ignoredSetting };
  } catch (e) {
    return { error: e instanceof Error ? e.message : String(e), ignoredSetting };
  }
}

/// What to check when the local install does not answer: the two places
/// its address comes from.
export function unreachableHint(setting: string | undefined, env: NodeJS.ProcessEnv = process.env): string {
  const file = path.join(installDir(env), 'ports.json');
  return setting && setting !== RETIRED_DEFAULT_URL
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
/// (`installDir`, `localAddress`), so the CLI and the address always name
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
