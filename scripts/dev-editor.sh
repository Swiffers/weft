#!/usr/bin/env bash
# Try this checkout's VS Code extension without installing anything.
#
# Builds the extension from this checkout (the same build ./setup.sh runs
# before packaging it), then opens one extra VS Code window running it
# from source. That window drives this checkout's own weft install (the
# one ./setup.sh set up here, named in .weft-install); every other window
# keeps the installed extension and main's weft, untouched. On a branch
# this is how you use the editor: ./setup.sh installs no extension there.
#
# Usage:
#   scripts/dev-editor.sh [folder]   # folder: what the window opens (a weft project)
set -euo pipefail

checkout="$(cd "$(dirname "$0")/.." && pwd)"
ext_dir="${checkout}/extension-vscode"

# shellcheck source=scripts/lib/checkout-install.sh
. "${checkout}/scripts/lib/checkout-install.sh"
# shellcheck source=scripts/lib/vscode-build.sh
. "${checkout}/scripts/lib/vscode-build.sh"

if [ $# -gt 1 ]; then
  echo "usage: scripts/dev-editor.sh [folder]" >&2
  exit 1
fi
# SYNC: .weft-install <-> extension-vscode/src/localInstall.ts (CHECKOUT_INSTALL_FILE),
#       scripts/lib/checkout-install.sh (weft_install_record, weft_checkout_install), .gitignore
# The window reads this checkout's .weft-install to find its weft, so
# that has to name the install this checkout is on now (the check says
# to run ./setup.sh when it does not).
weft_checkout_install "${checkout}" || exit 1
for tool in pnpm code; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "${tool} is not on PATH; the development window needs it" >&2
    exit 1
  fi
done

step() { # label cmd...
  local label="$1"
  shift
  echo "==> ${label}"
  "$@"
}
weft_vscode_link_borrowers "${ext_dir}"
weft_vscode_compile "${ext_dir}" step

# In a remote session (SSH, WSL) `code` is VS Code's remote command, which
# drops --extensionDevelopmentPath and opens a plain window running the
# installed extension. VS Code opens a remote development window only
# from a window, so point at the launch config that does. That window
# starts with its extension host paused for a debugger that does not
# attach through the remote (Ctrl+F5 too), so nothing in it loads until it
# reloads; a reload starts the host without the debugger.
if [[ "$(command -v code)" == */remote-cli/* ]]; then
  echo "==> built. VS Code is remote here, and its \`code\` command cannot open a development window."
  echo "    In a VS Code window on ${checkout}, press F5 (\"weft: this checkout's extension\")."
  echo "    The window that opens hangs until it reloads: Ctrl+Shift+P, \"Developer: Reload Window\"."
  echo "    Then open the project in it${1:+: $1}."
  exit 0
fi

echo "==> opening a development window (install ${WHICH_ENV})"
code --extensionDevelopmentPath="${ext_dir}" ${1:+"$1"}
