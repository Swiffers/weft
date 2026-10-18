#!/usr/bin/env bash
# Take a worktree off this machine: its branch's weft install first, then
# the worktree itself. The counterpart of scripts/add-worktree.sh.
#
# The install goes whole (`weft daemon remove`): its runtime, its projects,
# workers and infra, its database, its buckets, and the build caches its
# runtime and builder base compiled weft's crates in. Running this is the
# confirmation, so it asks nothing. The git branch stays: delete it
# yourself once it is merged (git branch -d <branch>).
#
# Refuses the main checkout, and any checkout whose install is the default
# one (a worktree on `main`): that install is the one `weft` on your PATH
# drives, and ./setup.sh --uninstall is how it goes. A worktree with
# uncommitted changes is refused before anything is removed.
#
# Usage:
#   scripts/remove-worktree.sh <path-or-branch>
#
# Example:
#   scripts/remove-worktree.sh feat/storage-plane
#   scripts/remove-worktree.sh ../weft-trees/feat-storage-plane
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: scripts/remove-worktree.sh <path-or-branch>" >&2
  exit 1
fi
target="$1"

script_dir="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/lib/checkout-install.sh
. "${script_dir}/lib/checkout-install.sh"

# The MAIN worktree, the same way scripts/add-worktree.sh finds it:
# `--git-common-dir` is the main `.git` from any worktree.
# (It may print a path relative to where it runs, hence the cd first.)
main_root="$(cd "${script_dir}" && cd "$(git rev-parse --git-common-dir)" && cd .. && pwd -P)"

# The worktree to remove: a folder, or the worktree a branch is checked
# out in.
if [ -d "${target}" ]; then
  worktree="$(cd "${target}" && pwd -P)"
else
  worktree="$(git -C "${main_root}" worktree list --porcelain | awk -v want="refs/heads/${target}" '
    /^worktree / { path = substr($0, 10) }
    /^branch / && substr($0, 8) == want { print path; exit }')"
  if [ -z "${worktree}" ]; then
    echo "no worktree has branch '${target}' checked out, and there is no folder by that name" >&2
    exit 1
  fi
  worktree="$(cd "${worktree}" && pwd -P)"
fi
if ! git -C "${main_root}" worktree list --porcelain | grep -qxF "worktree ${worktree}"; then
  echo "${worktree} is not a worktree of ${main_root}" >&2
  exit 1
fi
if [ "${worktree}" = "${main_root}" ]; then
  echo "${worktree} is the main checkout; this script only removes the worktrees made beside it" >&2
  exit 1
fi
# git would refuse a dirty worktree anyway, but only after its install was
# gone: check first, so a refusal removes nothing.
if [ -n "$(git -C "${worktree}" status --porcelain)" ]; then
  echo "${worktree} has uncommitted changes; commit or discard them first (nothing was removed)" >&2
  git -C "${worktree}" status --short >&2
  exit 1
fi

# Which install that checkout uses, asked from inside it so its branch
# decides (never an inherited WEFT_INSTALL). Its own weft when ./setup.sh
# recorded one, else the weft on PATH, whose front door answers for any
# checkout.
# SYNC: WEFT_INSTALL <-> crates/weft-core/src/infra/install.rs (INSTALL_ENV)
recorded=""
[ -f "${worktree}/${weft_install_record}" ] && recorded="$(head -n1 "${worktree}/${weft_install_record}" | tr -d '\r')"
if [ -n "${recorded}" ] && [ -x "${recorded}/bin/weft" ]; then
  weft="${recorded}/bin/weft"
elif command -v weft >/dev/null 2>&1; then
  weft="$(command -v weft)"
else
  echo "no weft to ask which install ${worktree} uses: none on PATH and none recorded there; install main first (./setup.sh in ${main_root})" >&2
  exit 1
fi
unset WEFT_INSTALL
which_rc=0
weft_which "${worktree}" "${weft}" || which_rc=$?
if [ "${which_rc}" -eq 2 ]; then
  echo "run a full ./setup.sh in ${main_root} to install a weft that can, then run this again (nothing was removed)" >&2
  exit 1
elif [ "${which_rc}" -ne 0 ]; then
  echo "could not ask ${weft} which install ${worktree} uses (its answer is above; nothing was removed)" >&2
  exit 1
fi

if [ "${WHICH_ENV}" = "default" ]; then
  echo "${worktree} uses the default install (it is on branch ${WHICH_BRANCH:-(none)}); that one goes with ./setup.sh --uninstall, never from here" >&2
  exit 1
fi
# The install this checkout set up before its branch changed, if any,
# by the name weft knows it under, for the note at the end: the default
# install, or the named one its folder is called after, whichever weft
# confirms lives in that folder. Asked in a subshell, so the WHICH_* of
# the install being removed stay as they are.
other_env=""
if [ -n "${recorded}" ] && [ "${recorded}" != "${WHICH_DIR}" ]; then
  for candidate in default "$(basename "${recorded}")"; do
    if [ "$(weft_which "${worktree}" "${weft}" "${candidate}" 2>/dev/null && printf '%s' "${WHICH_DIR}")" = "${recorded}" ]; then
      other_env="${candidate}"
      break
    fi
  done
fi

# Its e2e runs' bucket first, which the runner names after it, in the
# machine's shared object store; there even when the install never was,
# since an install set up and removed before leaves it behind. First, so
# a bucket that cannot go stops this before anything else is removed and
# a rerun finds everything as it was. weed shell's exit
# code and chatter are both unreliable (it prints success for things it
# did not do), so the LIST afterwards is the one source of truth.
# SYNC: the bucket, the create-then-list check <-> scripts/run-e2e.sh (WEFT_E2E_S3_BUCKET)
bucket="$(weft_e2e_bucket "${WHICH_NAME}")"
bucket_delete="docker exec weft-object-store sh -c 'echo \"s3.bucket.delete -name ${bucket}\" | weed shell'"
if ! command -v docker >/dev/null 2>&1 || [ -z "$(docker ps -q -f name='^weft-object-store$' 2>/dev/null)" ]; then
  echo "note: the machine's object store (weft-object-store) is not running, so the e2e bucket ${bucket} stays if it exists. Once any install is up, delete it with: ${bucket_delete}" >&2
else
  docker exec weft-object-store sh -c "echo \"s3.bucket.delete -name ${bucket}\" | weed shell" >/dev/null 2>&1 || true
  bucket_list="$(docker exec weft-object-store sh -c 'echo "s3.bucket.list" | weed shell' 2>&1)" || {
    echo "could not list the object store's buckets to check ${bucket} is gone. weed shell said:" >&2
    printf '%s\n' "${bucket_list}" >&2
    echo "nothing was removed; delete the bucket with: ${bucket_delete}, then run this again" >&2
    exit 1
  }
  if printf '%s\n' "${bucket_list}" | grep -Eq "(^|[[:space:]])${bucket}([[:space:]]|\$)"; then
    echo "could not delete the e2e bucket ${bucket}. weed shell lists:" >&2
    printf '%s\n' "${bucket_list}" >&2
    echo "nothing was removed; delete the bucket with: ${bucket_delete}, then run this again" >&2
    exit 1
  fi
fi

if [ "${WHICH_INSTALLED}" -eq 1 ]; then
  echo "==> removing install ${WHICH_ENV} (${WHICH_DIR})"
  (cd "${worktree}" && "${weft}" daemon remove)
else
  echo "==> install ${WHICH_ENV} was never set up; nothing to remove there"
fi

if [ -n "${recorded}" ] && [ "${recorded}" != "${WHICH_DIR}" ]; then
  if [ "${other_env}" = "default" ]; then
    echo "note: ${worktree} was set up before on the default install (${recorded}), which stays; it is the one \`weft\` on your PATH drives" >&2
  elif [ -n "${other_env}" ]; then
    echo "note: ${worktree} once set up another install, ${other_env} (${recorded}), before its branch changed; it is still there. Remove it with: WEFT_INSTALL=${other_env} weft daemon remove" >&2
  else
    echo "note: ${worktree} once set up another install, in ${recorded}, before its branch changed, and its weft could not say which install that is; it is still there" >&2
  fi
fi

echo "==> removing worktree ${worktree}"
git -C "${main_root}" worktree remove "${worktree}"
echo ""
echo "removed. The branch ${WHICH_BRANCH:-(none)} is kept; delete it once merged: git branch -d ${WHICH_BRANCH:-<branch>}"
