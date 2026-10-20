#!/usr/bin/env bash
# Run the suites that need a real Postgres, against a throwaway one.
#
# These tests compile in every test build, and nextest's `db` profile is what
# picks them (`.config/nextest.toml`): each crate's `db` test binary, and any
# `db_tests` module inside a library. A plain `cargo nextest run` leaves them
# out. They read $DATABASE_URL and create their own randomly named database
# per test, dropping it afterwards.
#
# Why a container rather than the Postgres weft runs: these tests create and
# drop databases on whatever server they are pointed at, and the one your local
# weft uses is holding your projects and your run history.
#
# Usage:
#   scripts/run-db-tests.sh                 # every crate that has them
#   scripts/run-db-tests.sh weft-broker     # just this crate
#   scripts/run-db-tests.sh weft-dispatcher an_upgraded_database
#                                           # and just tests matching a name
#                                           # (a plain substring of the test's
#                                           # name, `<file>::<test>`; matching
#                                           # none fails loudly)
#   scripts/run-db-tests.sh weft-dispatcher test_name db_lifecycle
#                                           # and only the tests in this file
#
# Set DATABASE_URL yourself and the script uses that server instead of
# starting a container, which is what CI does.
#
# The nextest run (it builds every crate below, then runs) takes its turn
# in the machine's build slot, one heavy cargo build at a time shared with
# setup.sh and the e2e and node-test runners: a run that finds another
# build going says which, and waits.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1
# shellcheck source=lib/machine-lock.sh
. "$SCRIPT_DIR/lib/machine-lock.sh"
trap machine_lock_release_mine EXIT

# Every crate whose tests need a database. A new one goes here.
ALL_CRATES=(weft-dispatcher weft-broker weft-access-store weft-task-store weft-platform-local weft-runtime)
CRATES=("${ALL_CRATES[@]}")

only_crate="${1:-}"
filter="${2:-}"
test_target="${3:-}"
if [ "$#" -gt 3 ] || { [ -n "$test_target" ] && { [ -z "$only_crate" ] || [ -z "$filter" ]; }; }; then
  echo "usage: scripts/run-db-tests.sh [crate [test-name-filter [test-file]]]" >&2
  exit 1
fi
if [ -n "$only_crate" ]; then
  found=0
  for c in "${ALL_CRATES[@]}"; do [ "$c" = "$only_crate" ] && found=1; done
  if [ "$found" -eq 0 ]; then
    echo "no db tests in '$only_crate'; it is one of: ${ALL_CRATES[*]}" >&2
    exit 1
  fi
  CRATES=("$only_crate")
fi

command -v cargo-nextest >/dev/null 2>&1 || {
  echo "cargo-nextest is missing; install it with: cargo install cargo-nextest --locked" >&2
  exit 1
}

if [ -z "${DATABASE_URL:-}" ]; then
  # shellcheck source=lib/throwaway-postgres.sh
  . "$SCRIPT_DIR/lib/throwaway-postgres.sh"
  echo "waiting for postgres..."
  # Trap installed BEFORE the start: the helper exports the container
  # name before running it, so a Ctrl-C during the pull or the
  # readiness wait still removes it.
  trap 'docker rm -f -v "${THROWAWAY_PG_CONTAINER:-}" >/dev/null 2>&1 || true; machine_lock_release_mine' EXIT
  start_throwaway_postgres weft-db-tests || exit 1
  export DATABASE_URL="$THROWAWAY_DATABASE_URL"
fi

echo "running against $DATABASE_URL"
# Every crate in ONE nextest run: cargo builds them all at once, and
# nextest runs every test of every test binary side by side (cargo test
# runs the binaries one after another). Each test makes its own database,
# so nothing they share needs them in order.
# The whole workspace is BUILT every time, and the crate, file and name
# narrow only what RUNS: a build of some crates alone turns on different
# features of the crates under them (a `test-helpers` one crate's tests
# ask for, `serve` on the task store), so cargo would compile a second
# copy of most of the workspace instead of reusing the one every other
# test run builds. The `db` profile adds the rest of the selection: only
# the database tests.
nextest_args=(--profile db --workspace)
selection=""
for crate in "${CRATES[@]}"; do
  selection="${selection:+$selection | }package($crate)"
done
selection="($selection)"
# Each file is a module of its crate's `db` binary, so its tests are the
# ones whose name starts with `<file>::`.
[ -n "$test_target" ] && selection="$selection & test(/^${test_target}::/)"
# A filter is a plain substring of test names (`~`, nextest's contains
# matcher), not a regex; a name is `<file>::<test>`. nextest refuses a run in
# which no test matched, so a filter that selects nothing fails loudly
# instead of reporting green on a suite where nothing executed.
[ -n "$filter" ] && selection="$selection & test(~$filter)"
nextest_args+=(-E "$selection")
with_build_slot cargo nextest run --no-fail-fast "${nextest_args[@]}"
