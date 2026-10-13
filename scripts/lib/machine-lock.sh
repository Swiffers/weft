# Machine-wide locks for the scripts: one holder at a time on this
# machine, whichever checkout it runs from, and everyone else waits.
#
# The kernel holds each lock. Taking one opens its file on a fixed file
# descriptor and flock()s it there; the lock belongs to that open file
# and is released when the last descriptor on it closes, which the
# kernel does for a holder that dies however it dies (SIGKILL, a
# reboot). So nothing ever has to decide a holder is gone: no pids are
# trusted, nothing is taken over. Linux's util-linux `flock <fd>` does
# the locking; macOS has no flock binary, so there perl flocks the same
# inherited descriptor, which locks the same open file.
#
# A waiter first tries without blocking; when the lock is held it prints
# once who holds it (the holder writes its pid and command line into
# `<lock>.holder` after taking it), then blocks until it is free.
#
# A child inherits the descriptor, and a child that outlives its parent
# (a daemon it starts) would keep the lock. So every command run under a
# lock runs with the lock descriptors closed (`without_machine_locks`),
# and a script holding a lock across many commands runs anything that
# may start a long-lived process through it too. A background job is a
# subshell that would keep the descriptors while its command runs, so it
# closes them first thing (`close_machine_locks_here`). No script runs
# another script that asks for a lock it holds itself: that would wait
# forever.
#
# Sourced by bash 3.2 (macOS's setup.sh) as well as bash 5: no
# associative arrays, no namerefs, no `{var}>` redirections. Safe under
# `set -euo pipefail`.

# weft_state_dir: the data directory every install lives under.
# shellcheck source=scripts/lib/data-dir.sh
. "$(dirname "${BASH_SOURCE[0]}")/data-dir.sh"
# One e2e run at a time on the machine: tests crash under load, and
# several worktrees may each want to run theirs.
weft_e2e_run_lock="${weft_state_dir}/e2e-run.lock"
# One heavy cargo build at a time on the machine: overlapping builds
# from parallel worktrees once ran WSL out of memory.
weft_build_slot_lock="${weft_state_dir}/build-slot.lock"
_weft_build_slot_what="the build slot (one heavy cargo build at a time on this machine)"

# The descriptor each lock is held on. Fixed numbers, because bash 3.2
# cannot pick a free one; `without_machine_locks` closes exactly these.
_machine_lock_fd() { # lock
  case "$1" in
    "${weft_e2e_run_lock}") echo 8 ;;
    "${weft_build_slot_lock}") echo 9 ;;
    *) echo "machine-lock.sh: no descriptor assigned to the lock $1" >&2; return 1 ;;
  esac
}

# Run a command with every lock descriptor closed, so nothing it starts
# can keep a lock after this process lets go.
without_machine_locks() { # cmd...
  "$@" 8>&- 9>&-
}

# Close every lock descriptor in this shell for good: the first line of a
# background subshell, `( close_machine_locks_here; ... ) &`, which takes
# no lock itself. Never in the shell that holds them.
close_machine_locks_here() {
  exec 8>&- 9>&-
}

# The locks this process holds, colon separated, so an exit can hand
# them all back.
_machine_locks_mine=""

_machine_lock_held() { # lock
  case ":${_machine_locks_mine}:" in
    *":$1:"*) return 0 ;;
  esac
  return 1
}

# flock() the open descriptor `fd`: `nb` tries once (non-zero when held),
# `wait` blocks until it is free, `un` unlocks.
_machine_lock_flock() { # fd nb|wait|un
  if command -v flock >/dev/null 2>&1; then
    case "$2" in
      nb) flock -n "$1" ;;
      wait) flock "$1" ;;
      un) flock -u "$1" ;;
    esac
  else
    perl -MFcntl=:flock -e '
      open(my $fh, ">&=", $ARGV[0]) or die "machine-lock.sh: descriptor $ARGV[0]: $!\n";
      my %op = (nb => LOCK_EX | LOCK_NB, wait => LOCK_EX, un => LOCK_UN);
      flock($fh, $op{$ARGV[1]}) or exit 1;' "$1" "$2"
  fi
}

# Take `lock`, waiting for its holder to let go. `what` names it in the
# one line a waiter prints. Returns non-zero when the lock file cannot be
# opened or the wait was cut short (Ctrl-C).
machine_lock_take() { # lock what
  local lock="$1" what="$2" fd holder
  _machine_lock_held "${lock}" && return 0
  fd="$(_machine_lock_fd "${lock}")" || return 1
  mkdir -p "$(dirname "${lock}")" || return 1
  eval "exec ${fd}>>\"\${lock}\"" || return 1
  if ! _machine_lock_flock "${fd}" nb; then
    holder="$(cat "${lock}.holder" 2>/dev/null)"
    # A holder killed outright leaves its line behind until the next one
    # writes its own: a line naming a process that is gone says nothing.
    kill -0 "${holder%% *}" 2>/dev/null || holder=""
    echo "waiting for ${what}: held by ${holder:-a process that has not said who it is yet}" >&2
    if ! _machine_lock_flock "${fd}" wait; then
      eval "exec ${fd}>&-"
      return 1
    fi
  fi
  printf '%s %s\n' "$$" "$(ps -p "$$" -o command= 2>/dev/null | cut -c1-160)" > "${lock}.holder"
  _machine_locks_mine="${_machine_locks_mine:+${_machine_locks_mine}:}${lock}"
}

# Hand `lock` back if this process holds it. The explicit unlock frees it
# even if a child still has the descriptor open.
machine_lock_release() { # lock
  local fd out="" item IFS=:
  _machine_lock_held "$1" || return 0
  fd="$(_machine_lock_fd "$1")" || return 1
  rm -f "$1.holder"
  # An exit trap can fire inside `without_machine_locks`, where the
  # descriptor is closed for the moment: there the unlock has nothing to
  # act on, and the exit that follows closes the saved copy, which frees
  # the lock.
  _machine_lock_flock "${fd}" un 2>/dev/null || true
  eval "exec ${fd}>&-"
  for item in ${_machine_locks_mine}; do
    [ -n "${item}" ] && [ "${item}" != "$1" ] && out="${out:+${out}:}${item}"
  done
  _machine_locks_mine="${out}"
  return 0
}

# Hand back every lock this process took. For an exit trap: the kernel
# frees them at exit anyway, but the holder file should go with them.
machine_lock_release_mine() {
  local lock IFS=:
  for lock in ${_machine_locks_mine}; do
    [ -n "${lock}" ] && machine_lock_release "${lock}"
  done
  return 0
}

# Run a command holding `lock` (with the lock descriptors closed for it),
# and hand the lock back as soon as the command ends, whatever it
# returned. Under a lock this process already holds, it just runs the
# command. Returns the command's status.
with_machine_lock() { # lock what cmd...
  local lock="$1" what="$2" rc=0
  shift 2
  if _machine_lock_held "${lock}"; then
    without_machine_locks "$@" || rc=$?
    return "${rc}"
  fi
  machine_lock_take "${lock}" "${what}" || return 1
  without_machine_locks "$@" || rc=$?
  machine_lock_release "${lock}"
  return "${rc}"
}

# The build slot, for a script whose heavy step spans more than one
# command (a batch of builds running side by side): take it, then hand it
# back when the last of them is done.
build_slot_take() {
  machine_lock_take "${weft_build_slot_lock}" "${_weft_build_slot_what}"
}
build_slot_release() {
  machine_lock_release "${weft_build_slot_lock}"
}

# Run one heavy cargo build in the machine's build slot.
with_build_slot() { # cmd...
  with_machine_lock "${weft_build_slot_lock}" "${_weft_build_slot_what}" "$@"
}
