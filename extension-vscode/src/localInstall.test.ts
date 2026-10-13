import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import {
  CHECKOUT_INSTALL_FILE,
  DEFAULT_LOCAL_URL,
  RETIRED_DEFAULT_URL,
  installDir,
  localAddress,
  localInstallUrl,
  whichCli,
} from './localInstall';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'weft-local-install-'));
const root = path.join(home, '.local', 'share', 'weft');

afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

function save(dir: string, contents: string) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'ports.json'), contents);
}

describe('localInstallUrl', () => {
  it('is the default port until the default install saved one', () => {
    expect(localInstallUrl({ HOME: home })).toBe(DEFAULT_LOCAL_URL);
    save(root, '{"public":15000,"internal":14113,"outside":14112,"postgres":14114}');
    expect(localInstallUrl({ HOME: home })).toBe('http://127.0.0.1:15000');
  });

  it('reads a named install from its own folder and refuses one never started', () => {
    const env = { HOME: home, WEFT_INSTALL: 'cell1' };
    expect(installDir(env)).toBe(path.join(root, 'installs', 'cell1'));
    expect(() => localInstallUrl(env)).toThrow('weft daemon start');
    save(path.join(root, 'installs', 'cell1'), '{"public":15100}');
    expect(localInstallUrl(env)).toBe('http://127.0.0.1:15100');
  });

  it('takes WEFT_INSTALL=default as the default install', () => {
    const env = { HOME: home, WEFT_INSTALL: 'default' };
    expect(installDir(env)).toBe(root);
    expect(localInstallUrl(env)).toBe(DEFAULT_LOCAL_URL);
    save(root, '{"public":15000}');
    expect(localInstallUrl(env)).toBe('http://127.0.0.1:15000');
  });

  it('fails loudly on a file it cannot read', () => {
    save(root, '{"public":"x"}');
    expect(() => localInstallUrl({ HOME: home })).toThrow('is not valid');
  });
});

describe('localAddress', () => {
  it('takes a set address over the saved port', () => {
    save(root, '{"public":15000}');
    expect(localAddress('http://10.0.0.2:9000', { HOME: home })).toEqual({ url: 'http://10.0.0.2:9000', ignoredSetting: false });
  });

  it('passes over the retired default and says so', () => {
    save(root, '{"public":15000}');
    expect(localAddress(RETIRED_DEFAULT_URL, { HOME: home })).toEqual({ url: 'http://127.0.0.1:15000', ignoredSetting: true });
  });

  it('returns the reason instead of throwing when there is no address', () => {
    const got = localAddress(undefined, { HOME: home, WEFT_INSTALL: 'cell1' });
    expect('error' in got && got.error).toContain('weft daemon start');
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

  it('points the address at the recorded install', () => {
    const dir = installed(path.join(root, 'installs', 'deploy'));
    save(root, '{"public":15000}');
    save(dir, '{"public":15200}');
    const got = whichCli(true, checkout(dir), env);
    if ('error' in got) throw new Error(got.error);
    expect(installDir(got.env)).toBe(dir);
    expect(localAddress(undefined, got.env)).toEqual({ url: 'http://127.0.0.1:15200', ignoredSetting: false });
  });
});
