# Which weft install a checkout uses, for the scripts.
#
# The CLI is the one authority on it (`weft daemon which`: WEFT_INSTALL
# if set, else the branch of the checkout it runs in, `main` meaning the
# default install), so nothing here works it out from git. setup.sh asks
# the binary it just built and records the answer in the checkout's
# `.weft-install`; every other script reads that record and has the
# install's own `weft` confirm it still holds.
#
# Sourced by bash 3.2 (macOS's setup.sh) as well as bash 5. Safe under
# `set -euo pipefail`: every function reports on stderr and returns
# non-zero instead of exiting.

# The record setup.sh leaves in a checkout: one line, the install's
# folder. The VS Code extension, run from source, reads it too.
# SYNC: .weft-install <-> extension-vscode/src/localInstall.ts (CHECKOUT_INSTALL_FILE, whichCli),
#       scripts/dev-editor.sh (has the extension read it), .gitignore
weft_install_record=".weft-install"

# One string field of the object `weft daemon which --json` printed;
# empty for a null. The value runs to the first quote no backslash
# escapes, and its `\"` and `\\` escapes are decoded (a path may hold
# either; the JSON writer escapes nothing else a path holds).
_weft_which_string() { # key json
  # A `\\` is parked as a control character while `\"` is decoded, so
  # the backslash it ends in never pairs with a quote after it. (BSD
  # sed has no \x escapes, hence printf.)
  local parked
  parked="$(printf '\001')"
  printf '%s\n' "$2" \
    | sed -nE 's/.*"'"$1"'" *: *"(([^"\\]|\\.)*)".*/\1/p' \
    | sed -e "s/\\\\\\\\/${parked}/g" -e 's/\\"/"/g' -e "s/${parked}/\\\\/g"
}

# Ask `weft` which install it would act on from `checkout`. `install`,
# when given, is a WEFT_INSTALL value to ask about instead (a test
# cell's name, say). Returns 2, having said so, when `weft` is older
# than installs per branch and has no `weft daemon which` (the caller
# names the fix that fits it), 1 on any other failure. Sets:
#   WHICH_NAME       the install's name, empty for the default install
#   WHICH_ENV        its WEFT_INSTALL value (`default` or the name)
#   WHICH_DIR        its folder
#   WHICH_BIN        its bin folder, where its weft and weft-runtime live
#   WHICH_BRANCH     the checkout's branch when that is what picked it
#   WHICH_INSTALLED  1 when its bin folder holds a weft, else 0
# SYNC: the JSON fields <-> crates/weft-cli/src/commands/daemon.rs (`weft daemon which --json`)
# SYNC: WEFT_INSTALL <-> crates/weft-core/src/infra/install.rs (INSTALL_ENV)
weft_which() { # checkout weft [install]
  local checkout="$1" weft="$2" json installed err
  # Asked from inside the checkout, so a relative path to it is made
  # whole first (a bare name stays a PATH lookup).
  case "${weft}" in
    /*) ;;
    */*) weft="${PWD}/${weft}" ;;
  esac
  err="$(mktemp -t weft-which.XXXXXX)" || return 1
  if [ $# -ge 3 ]; then
    json="$(cd "${checkout}" && WEFT_INSTALL="$3" "${weft}" daemon which --json 2>"${err}")" || { _weft_which_failed "${weft}" "${err}"; return $?; }
  else
    json="$(cd "${checkout}" && "${weft}" daemon which --json 2>"${err}")" || { _weft_which_failed "${weft}" "${err}"; return $?; }
  fi
  cat "${err}" >&2
  rm -f "${err}"
  WHICH_NAME="$(_weft_which_string name "${json}")"
  WHICH_ENV="$(_weft_which_string env "${json}")"
  WHICH_DIR="$(_weft_which_string dir "${json}")"
  WHICH_BIN="$(_weft_which_string bin "${json}")"
  WHICH_BRANCH="$(_weft_which_string branch "${json}")"
  installed="$(printf '%s\n' "${json}" | sed -nE 's/.*"installed" *: *(true|false).*/\1/p')"
  if [ -z "${WHICH_ENV}" ] || [ -z "${WHICH_DIR}" ] || [ -z "${WHICH_BIN}" ] || [ -z "${installed}" ]; then
    echo "${weft} daemon which --json answered something this script cannot read:" >&2
    printf '%s\n' "${json}" >&2
    return 1
  fi
  WHICH_INSTALLED=0
  [ "${installed}" = "true" ] && WHICH_INSTALLED=1
  return 0
}

# Why `weft daemon which` failed, from what it printed (in the file
# `err`, removed here): 2 for a weft that has no such verb (clap's
# "unrecognized subcommand", explained instead of shown), 1 for anything
# else, its own message shown as it is (a weft that cannot run at all,
# a checkout on no branch).
_weft_which_failed() { # weft err
  if grep -q "unrecognized subcommand 'which'" "$2"; then
    rm -f "$2"
    echo "$1 is older than installs per branch: it has no \`weft daemon which\` to say which install a checkout uses" >&2
    return 2
  fi
  cat "$2" >&2
  rm -f "$2"
  return 1
}

# Record that `checkout` set up the install in `dir`. Written through a
# temp and a rename, so a reader never sees half a line.
weft_record_checkout_install() { # checkout dir
  local record="$1/${weft_install_record}"
  printf '%s\n' "$2" > "${record}.$$" && mv -f "${record}.$$" "${record}"
}

# The install `checkout` set up, for a script that drives it: the
# folder its `.weft-install` names, confirmed by that install's own
# weft from inside the checkout. Sets the WHICH_* variables. Fails
# naming the fix when the checkout was never set up, its install is
# gone, or it has moved to another branch since.
weft_checkout_install() { # checkout
  local checkout="$1" record recorded
  record="${checkout}/${weft_install_record}"
  if [ ! -f "${record}" ]; then
    echo "${checkout} has no install set up yet (no ${weft_install_record}); run ./setup.sh there first" >&2
    return 1
  fi
  recorded="$(head -n1 "${record}" | tr -d '\r')"
  if [ ! -x "${recorded}/bin/weft" ]; then
    echo "${checkout} was set up for the install in ${recorded}, which has no weft any more; run ./setup.sh there" >&2
    return 1
  fi
  if ! weft_which "${checkout}" "${recorded}/bin/weft"; then
    echo "run ./setup.sh in ${checkout} to install its own weft again" >&2
    return 1
  fi
  if [ "${WHICH_DIR}" != "${recorded}" ]; then
    if [ -n "${WEFT_INSTALL:-}" ]; then
      echo "WEFT_INSTALL=${WEFT_INSTALL} picks the install in ${WHICH_DIR}, but ${checkout} was set up for ${recorded}; unset WEFT_INSTALL, or run ./setup.sh with it set" >&2
    else
      echo "${checkout} is now on branch ${WHICH_BRANCH:-(none)}, whose install is ${WHICH_DIR}, but it was set up for ${recorded}; run ./setup.sh there" >&2
    fi
    return 1
  fi
  return 0
}

# The bucket an install's e2e runs keep their S3 test objects in, in the
# machine's shared object store: one per install, so two installs' runs
# never share one. `name` is empty for the default install.
weft_e2e_bucket() { # name
  printf 'weft-e2e%s' "${1:+-$1}"
}
