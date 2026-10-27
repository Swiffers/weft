import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { CHECKOUT_INSTALL_FILE, RETIRED_DEFAULT_URL, installDir, settingAddress, whichCli } from './localInstall';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'weft-local-install-'));
const root = path.join(home, '.local', 'share', 'weft');

afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

describe('installDir', () => {
  it("is weft's data folder for the default install and installs/<name> for a named one", () => {
    expect(installDir({ HOME: home })).toBe(root);
    expect(installDir({ HOME: home, WEFT_INSTALL: 'default' })).toBe(root);
    expect(installDir({ HOME: home, WEFT_INSTALL: 'cell1' })).toBe(path.join(root, 'installs', 'cell1'));
  });
});

describe('settingAddress', () => {
  it('passes over an unset setting and the retired default', () => {
    expect(settingAddress(undefined)).toBeUndefined();
    expect(settingAddress('')).toBeUndefined();
    expect(settingAddress(RETIRED_DEFAULT_URL)).toBeUndefined();
    expect(settingAddress('http://10.0.0.2:9000')).toBe('http://10.0.0.2:9000');
  });
});

// A checkout with the extension's folder in it, which recorded `install`
// (an absolute folder) when given one.
function checkout(install?: string): string {
  const dir = fs.mkdtempSync(path.join(home, 'checkout-'));
  fs.mkdirSync(path.join(dir, 'extension-vscode'));
  if (install !== undefined) fs.writeFileSync(path.join(dir, CHECKOUT_INSTALL_FILE), `${install}\n`);
  return path.join(dir, 'extension-vscode');
}

// An install folder with its own `bin/weft`.
function installed(dir: string): string {
  fs.mkdirSync(path.join(dir, 'bin'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'bin', 'weft'), '');
  return dir;
}

describe('whichCli', () => {
  const env = { HOME: home };

  it('runs weft from PATH in the installed extension, even in a checkout that recorded an install', () => {
    const ext = checkout(installed(path.join(root, 'installs', 'deploy')));
    expect(whichCli(false, ext, env)).toEqual({ command: 'weft', env });
  });

  it('runs weft from PATH in a development window whose checkout recorded no install', () => {
    expect(whichCli(true, checkout(), env)).toEqual({ command: 'weft', env });
  });

  it("runs the recorded install's own weft in a development window, naming the install", () => {
    const dir = installed(path.join(root, 'installs', 'deploy'));
    expect(whichCli(true, checkout(dir), env)).toEqual({
      command: path.join(dir, 'bin', 'weft'),
      env: { HOME: home, WEFT_INSTALL: 'deploy' },
    });
    installed(root);
    expect(whichCli(true, checkout(root), env)).toEqual({
      command: path.join(root, 'bin', 'weft'),
      env: { HOME: home, WEFT_INSTALL: 'default' },
    });
  });

  it('refuses a recorded install with no weft in it, naming ./setup.sh', () => {
    const ext = checkout(path.join(root, 'installs', 'deploy'));
    const got = whichCli(true, ext, env);
    expect('error' in got && got.error).toContain(`run ./setup.sh in ${path.dirname(ext)}`);
  });

  it('refuses a recorded folder that is not an install folder', () => {
    const ext = checkout(installed(path.join(home, 'elsewhere')));
    const got = whichCli(true, ext, env);
    expect('error' in got && got.error).toContain('is not a weft install');
    expect('error' in whichCli(true, checkout('installs/deploy'), env)).toBe(true);
  });

  it("watches the recorded install's folder", () => {
    const dir = installed(path.join(root, 'installs', 'deploy'));
    const got = whichCli(true, checkout(dir), env);
    if ('error' in got) throw new Error(got.error);
    expect(installDir(got.env)).toBe(dir);
  });
});
