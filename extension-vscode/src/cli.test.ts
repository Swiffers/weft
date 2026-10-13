import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { describe, expect, it } from 'vitest';

import { chooseCli, runWeftJson } from './cli';
import { CHECKOUT_INSTALL_FILE, whichCli } from './localInstall';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'weft-cli-'));

// A `weft` that answers with which file ran and the install it was told.
function fakeWeft(bin: string): void {
  fs.mkdirSync(bin, { recursive: true });
  const script = '#!/bin/sh\nprintf \'{"weft":"%s","install":"%s"}\' "$0" "$WEFT_INSTALL"\n';
  fs.writeFileSync(path.join(bin, 'weft'), script, { mode: 0o755 });
}

// The extension folder of a checkout that recorded `install`.
function checkout(install: string): string {
  const dir = fs.mkdtempSync(path.join(home, 'checkout-'));
  fs.mkdirSync(path.join(dir, 'extension-vscode'));
  fs.writeFileSync(path.join(dir, CHECKOUT_INSTALL_FILE), install);
  return path.join(dir, 'extension-vscode');
}

describe('runWeftJson', () => {
  const onPath = path.join(home, 'path');
  fakeWeft(onPath);
  const install = path.join(home, '.local', 'share', 'weft', 'installs', 'deploy');
  fakeWeft(path.join(install, 'bin'));
  const env = { HOME: home, PATH: `${onPath}:/usr/bin:/bin` };

  it("runs the recorded install's own weft in a development window, telling it its install", async () => {
    chooseCli(whichCli(true, checkout(install), env));
    expect(await runWeftJson(['tree'], home)).toEqual({ weft: path.join(install, 'bin', 'weft'), install: 'deploy' });
  });

  it('runs weft from PATH in the installed extension', async () => {
    chooseCli(whichCli(false, checkout(install), env));
    expect(await runWeftJson(['tree'], home)).toEqual({ weft: path.join(onPath, 'weft'), install: '' });
  });

  it('fails every verb with the reason when the recorded install has no weft', async () => {
    chooseCli(whichCli(true, checkout(path.join(home, '.local', 'share', 'weft', 'installs', 'gone')), env));
    await expect(runWeftJson(['tree'], home)).rejects.toThrow('run ./setup.sh');
  });
});
