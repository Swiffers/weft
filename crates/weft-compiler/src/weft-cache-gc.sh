#!/bin/sh
# Bounds a cargo compile cache by what builds use. Run after a `cargo
# build` whose JSON messages (`--message-format=json...`) went to a file:
# cargo reports there every unit the build used, the ones it reused
# untouched included, and that report is how this knows what was used.
#
# usage: weft-cache-gc.sh this-build <target/release> <cargo messages>
#        weft-cache-gc.sh aged <target/release> <cargo messages> <worker crate> <retention days> <versions kept>
#
# `this-build`, for a cache one build owns (the runtime image's, the
# builder base's: one install, one top binary): everything the build did
# not use goes, so the cache is exactly one build's worth (superseded
# dependency versions, engine artifacts of an older lock, package slots
# the stdlib moved past).
#
# `aged`, for the cache every worker build on a host shares: what one
# project's build did not use may be another project's, so only the
# per-project units (the `pkg_` package crates, the worker crate, the
# `weft-isolate` helper) are bounded, by when a build last used them and
# by how many versions of each stay. The engine and the shared
# dependencies are never touched.
#
# A unit is known by the hash cargo names all its files with:
# `deps/lib<crate>-<hash>.rlib`, `deps/<crate>-<hash>.d`,
# `.fingerprint/<package>-<hash>/`, `build/<package>-<hash>/`. Removing a
# unit's files and its fingerprint together leaves cargo nothing stale: it
# compiles the unit again if a later build wants it.
#
# The source is crates/weft-compiler/src/weft-cache-gc.sh: codegen writes a
# copy into every crate it generates (`worker_image::CACHE_GC_SCRIPT`), and
# the runtime image runs it from the source tree it compiles.
# It never fails a build: every caller reports a failure and carries on.
set -eu
mode="$1"; release="$2"; messages="$3"
deps="$release/deps"; fingerprints="$release/.fingerprint"; scripts="$release/build"
[ -d "$deps" ] || exit 0
if ! [ -s "$messages" ]; then
  echo "weft: no cargo messages at $messages; the compile cache is left as it is" >&2
  exit 0
fi

used="$(mktemp)"
trap 'rm -f "$used"' EXIT
# Used: every unit cargo reported, by its hash.
grep -oE "$release/(deps|build)/[^\"/]+-[0-9a-f]{16}" "$messages" | sed 's/.*-//' > "$used" || true
# The binary the build asked for is reported where cargo linked it, at the
# top of `release/`, which is a hard link of its unit's file in `deps/`.
untraced=""
for top in $(grep -oE "\"$release/[^\"/]+\"" "$messages" | tr -d '"' | sort -u); do
  [ -f "$top" ] || continue
  traced=""
  for file in "$deps"/*; do
    if [ "$file" -ef "$top" ]; then
      unit="${file##*/}"
      echo "${unit##*-}" >> "$used"
      traced=1
      break
    fi
  done
  [ -n "$traced" ] || untraced="$untraced ${top##*/}"
done

# The names in `$1` (a directory) whose unit hash this build did not use.
unused_in() {
  ls -A "$1" | awk -v used="$used" '
    BEGIN { while ((getline h < used) > 0) keep[h] = 1 }
    {
      unit = $0; sub(/\..*/, "", unit)
      n = split(unit, part, "-"); hash = part[n]
      if (n > 1 && length(hash) == 16 && hash ~ /^[0-9a-f]+$/ && !(hash in keep)) print
    }'
}

if [ "$mode" = this-build ]; then
  if [ -n "$untraced" ]; then
    echo "weft: cannot tell which compiled unit is$untraced (cargo copied it instead of linking it); the compile cache is left as it is" >&2
    exit 0
  fi
  for dir in "$deps" "$fingerprints" "$scripts"; do
    [ -d "$dir" ] || continue
    for name in $(unused_in "$dir"); do
      rm -rf "${dir:?}/$name"
    done
  done
  # Incremental state is kept per crate in a directory cargo's hash does
  # not name (`incremental/<crate>-<rustc's own id>`), so it is bounded by
  # count: a crate keeps as many directories as this build used units of
  # it, the most recently written first (rustc writes into a crate's
  # directory each time it compiles it). A unit's dep-info names its
  # crate: in `deps/`, or for a build script in its own `build/` dir.
  incremental="$release/incremental"
  if [ -d "$incremental" ]; then
    crates="$(mktemp)"
    { ls -A "$deps"; for dir in "$scripts"/*/; do [ -d "$dir" ] && ls -A "$dir"; done; } | awk -v used="$used" '
      BEGIN { while ((getline h < used) > 0) keep[h] = 1 }
      /\.d$/ {
        unit = $0; sub(/\.d$/, "", unit)
        n = split(unit, part, "-")
        if (part[n] in keep) { sub(/-[^-]*$/, "", unit); print unit }
      }' > "$crates"
    for name in $(ls -t "$incremental" | awk -v crates="$crates" '
      BEGIN { while ((getline c < crates) > 0) wanted[c]++ }
      { crate = $0; sub(/-[^-]*$/, "", crate); if (++seen[crate] > wanted[crate]) print }'); do
      rm -rf "${incremental:?}/$name"
    done
    rm -f "$crates"
  fi
  exit 0
fi

[ "$mode" = aged ] || { echo "weft: unknown sweep '$mode' (this-build or aged)" >&2; exit 1; }
worker="$4"; days="$5"; versions="$6"
# Mark: every per-project unit this build used is touched, so its date is
# when a build last used it rather than when one compiled it. Only those:
# a shared unit dated after its dependents would make cargo rebuild them.
for dir in "$deps" "$fingerprints"; do
  [ -d "$dir" ] || continue
  for name in $(ls -A "$dir" | awk -v used="$used" -v worker="$worker" '
    BEGIN { while ((getline h < used) > 0) keep[h] = 1 }
    {
      unit = $0; sub(/\..*/, "", unit); sub(/^lib/, "", unit)
      n = split(unit, part, "-")
      if (!(part[n] in keep)) next
      if (unit ~ /^pkg_/ || unit ~ /^weft[-_]isolate-/ || index(unit, worker "-") == 1) print $0
    }'); do
    touch -c "$dir/$name"
  done
done
# Bound: of each package's compiled versions (every stem of one crate
# name), of the worker crate's and of the weft-isolate helper's, only the
# newest few stay.
drop_stem() {
  rm -f "$deps/$1" "$deps/$1".* "$deps/lib$1".*
  rm -rf "$fingerprints"/*-"${1##*-}"
}
for name in $(for d in "$deps"/pkg_*.d "$deps"/"$worker"-*.d "$deps"/weft_isolate-*.d; do [ -e "$d" ] && basename "$d" .d; done | sed 's/-[^-]*$//' | sort -u); do
  n=0
  for d in $(ls -t "$deps"/"$name"-*.d 2>/dev/null); do
    n=$((n + 1))
    [ "$n" -le "$versions" ] && continue
    drop_stem "$(basename "$d" .d)"
  done
done
# Expire: a per-project crate any of whose files is older than the
# retention period (a used crate had every file touched above; an orphan
# missing its dep-info still ages by its rlib or fingerprint).
keep=$((days - 1))
for file in "$deps"/pkg_*.d "$deps"/libpkg_*.rlib "$deps"/libpkg_*.rmeta "$deps"/"$worker"-*.d \
            "$fingerprints"/pkg_* "$fingerprints"/"$worker"-*; do
  [ -e "$file" ] || continue
  [ -n "$(find "$file" -maxdepth 0 -mtime +"$keep")" ] || continue
  stem="$(basename "$file")"
  case "$stem" in *.d|*.rlib|*.rmeta) stem="${stem%.*}";; esac
  stem="${stem#lib}"
  drop_stem "$stem"
done
# The worker crate's build-script units carry their own hashes; they age
# on their own and are cheap to run again.
for dir in "$scripts"/"$worker"-*/; do
  [ -d "$dir" ] || continue
  [ -n "$(find "$dir" -maxdepth 0 -mtime +"$keep")" ] && rm -rf "$dir"
done
exit 0
