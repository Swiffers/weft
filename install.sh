#!/usr/bin/env bash
# install.sh: install weft with one line.
#
#   curl -fsSL https://weft.weavemind.ai/install.sh | bash
#
# Clones weft into ~/weft, or brings an existing clone there up to date,
# then runs its ./setup.sh, which installs everything and says how to
# start a first project. Running the line again updates weft.
#
#   WEFT_DIR=<folder>   clone somewhere else than ~/weft
#   bash -s -- <flags>  pass flags to setup.sh, e.g.
#                       curl -fsSL https://weft.weavemind.ai/install.sh | bash -s -- --verbose
#
# The clone stays where it is: weft reads its standard library and the
# assistant templates from it, so moving or deleting the folder breaks
# the install. To remove weft, run ./setup.sh --uninstall --purge in it.
set -euo pipefail

repo_url="https://github.com/WeaveMindAI/weft.git"
dir="${WEFT_DIR:-${HOME}/weft}"

say()  { printf '  %s\n' "$*"; }
fail() { printf '  ✗ %s\n' "$*" >&2; exit 1; }

command -v git >/dev/null 2>&1 \
  || fail "git is required. Install it with your system's package manager, then run this line again."

# The same test weft itself uses for "this folder is a weft checkout":
# the workspace files plus the catalog crate and folder.
# SYNC: is_weft_checkout <-> crates/weft-core/src/checkout.rs (is_weft_checkout)
is_weft_checkout() {
  [[ -f "$1/Cargo.toml" && -f "$1/Cargo.lock" && -d "$1/crates/weft-catalog" && -d "$1/catalog" ]]
}

if [[ -d "${dir}/.git" ]] && is_weft_checkout "${dir}"; then
  branch="$(git -C "${dir}" rev-parse --abbrev-ref HEAD)"
  [[ "${branch}" == "main" ]] \
    || fail "${dir} is on branch ${branch}; this installer updates main only. Run ./setup.sh in that folder yourself, or switch it back to main."
  say "updating weft in ${dir}"
  git -C "${dir}" pull --ff-only --quiet \
    || fail "could not update ${dir} (local changes or a diverged history). Sort it out with git in that folder, then run this line again."
elif [[ -e "${dir}" ]]; then
  fail "${dir} already exists and is not a weft clone. Move it, or pick another folder: WEFT_DIR=<folder> before bash."
else
  say "downloading weft into ${dir}"
  git clone --quiet --filter=blob:none "${repo_url}" "${dir}"
fi

cd "${dir}"
# Piped through curl, this script's input is the pipe, so setup.sh's
# questions read from the terminal instead when there is one.
if [[ -r /dev/tty ]] && { : </dev/tty; } 2>/dev/null; then
  exec ./setup.sh "$@" </dev/tty
fi
exec ./setup.sh "$@"
