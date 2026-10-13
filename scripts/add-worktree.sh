#!/usr/bin/env bash
# Create a new git worktree for parallel feature work.
#
# Branches off whatever branch you're currently on, into a new branch with
# the name you give. The worktree lives as a sibling of the main checkout at
# ../weft-trees/<branch>, wherever you run this from.
#
# The gitignored local-config files (.env, .env.extension, access-apps.json)
# are SYMLINKED to the main worktree's copies rather than duplicated: setup.sh
# reads them but never creates them, and one set of credentials editable from
# any worktree beats N copies that drift. A setting that must differ per
# checkout (a public tunnel's token, say) goes in the worktree's own
# .env.local instead, which wins over .env and is never linked or created
# here. Everything else (node deps, builds, the branch's own weft install) is
# handled by running ./setup.sh inside the worktree afterwards.
#
# Usage:
#   scripts/add-worktree.sh <new-branch-name>
#
# Example:
#   scripts/add-worktree.sh feat/storage-plane
#     -> branch feat/storage-plane off the current branch
#     -> worktree at ../weft-trees/feat-storage-plane
#     -> then: cd ../weft-trees/feat-storage-plane && ./setup.sh
#        (sets up that branch's own install; main's is never touched)
#     -> when done: scripts/remove-worktree.sh feat/storage-plane
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: scripts/add-worktree.sh <new-branch-name>" >&2
  exit 1
fi

new_branch="$1"

# Resolve repo root regardless of where the script is invoked from.
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# The MAIN worktree, which is where the shared local-config files live and
# where the symlinks below must point. `--show-toplevel` above is the CURRENT
# worktree, so running this from inside one would otherwise chain the new
# worktree's links to that worktree instead of to the canonical copies.
# `--git-common-dir` is the main `.git` in both cases; its parent is the main
# worktree.
main_root="$(cd "$(git rev-parse --git-common-dir)" && cd .. && pwd)"

base_branch="$(git rev-parse --abbrev-ref HEAD)"

# Slashes are legal in branch names but make nested dirs; flatten for the
# folder name while keeping the real branch name intact in git.
dir_name="${new_branch//\//-}"
worktree_path="$main_root/../weft-trees/$dir_name"

if git show-ref --verify --quiet "refs/heads/$new_branch"; then
  echo "error: branch '$new_branch' already exists" >&2
  exit 1
fi
if [ -e "$worktree_path" ]; then
  echo "error: worktree path already exists: $worktree_path" >&2
  exit 1
fi

echo "==> branching '$new_branch' off '$base_branch'"
git worktree add -b "$new_branch" "$worktree_path" "$base_branch"

# Resolve the real path now that the dir exists (collapses the ../).
worktree_path="$(cd "$worktree_path" && pwd)"

# Link the gitignored local-config the worktree needs to run. Symlinks, not
# copies: editing any one of these from any worktree edits the single real
# file, so the credentials in them never drift between trees. Tunnel
# settings are per checkout, in the unlinked .env.local.
for cfg_file in .env .env.extension access-apps.json; do
  if [ -f "$main_root/$cfg_file" ]; then
    ln -sfn "$main_root/$cfg_file" "$worktree_path/$cfg_file"
    echo "==> linked $cfg_file -> $main_root/$cfg_file"
  else
    echo "==> skipped $cfg_file (not present in $main_root)"
  fi
done

echo ""
echo "worktree ready:"
echo "  $worktree_path"
echo "  branch: $new_branch (off $base_branch)"
echo ""
echo "next:   cd $worktree_path && ./setup.sh   # sets up this branch's own install"
echo "editor: scripts/dev-editor.sh                # this branch's VS Code extension, from source"
echo "remove: scripts/remove-worktree.sh $new_branch   # its install, then the worktree (the branch stays)"
