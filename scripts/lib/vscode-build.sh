# Building the VS Code extension from a checkout, shared by setup.sh
# (which then packages and installs it) and scripts/dev-editor.sh (which
# runs it from source in a development window), so the two never build
# it differently.
#
# Sourced by bash 3.2 (macOS's setup.sh) as well as bash 5.

# The shared graph webview lives in the sibling `packages/weft-graph`
# package, consumed FROM SOURCE (one renderer for the extension and the
# website). It has no install of its own; the extension bundles it
# through vite's `resolve.dedupe` (its bare deps resolve to the
# extension's node_modules). svelte-check (check:webview) needs the same
# redirect, so the package's node_modules points at the extension's
# install. A real directory there (a `pnpm install` run inside the
# package) is replaced: `ln -sfn` onto a directory would nest the link
# INSIDE it and leave the package resolving against the wrong install.
# Same borrow for the grammars package (its only dependency is the
# highlight.js the extension already installs, for its own tests) and
# the connect library the graph's connection picker is built on.
weft_vscode_link_borrowers() { # ext_dir
  local borrower
  for borrower in "$1/../packages/weft-graph" "$1/../packages/weft-syntax" "$1/../packages/weft-connect"; do
    if [ -d "${borrower}/node_modules" ] && [ ! -L "${borrower}/node_modules" ]; then
      rm -rf "${borrower}/node_modules"
    fi
    ln -sfn ../../extension-vscode/node_modules "${borrower}/node_modules"
  done
}

# Install the extension's dependencies and compile everything it runs:
# the extension host code, the graph webview and the markdown preview.
# `run` is how each step is shown: called as `run <label> <cmd...>`.
weft_vscode_compile() { # ext_dir run
  local ext_dir="$1" run="$2"
  "${run}" "pnpm install" pnpm -C "${ext_dir}" install --prefer-offline
  "${run}" "tsc compile" pnpm -C "${ext_dir}" run compile
  "${run}" "vite bundle webview" pnpm -C "${ext_dir}" run bundle:webview
  "${run}" "vite bundle markdown preview" pnpm -C "${ext_dir}" run bundle:markdown-preview
}
