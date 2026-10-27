#!/usr/bin/env bash
# Lists the version pins Dependabot cannot see (image tags inside Rust and
# shell, tool versions inside commands, the Rust toolchain) and fails when
# any of them has a newer release. Run weekly by
# .github/workflows/security.yml; runs the same from a checkout.
#
# Each line below reads one pin out of the file that holds it and asks
# that tool's registry for its newest release. A pattern names the line
# that DEFINES the pin (the constant, the variable), never only the image
# name, which a SYNC comment beside it also carries. A pin on a major line on
# purpose (pnpm 10 in the extensions, pnpm 11 in the bridge) is compared
# with the newest release of that major only.
#
# Left out on purpose: Postgres majors (moving one is a data directory
# migration, never a routine bump) and floating tags such as `alpine:3`.
#
# Needs curl and jq. Set GITHUB_TOKEN to lift GitHub's anonymous rate
# limit.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

curl_json() {
  local -a auth=()
  if [[ -n "${GITHUB_TOKEN:-}" && "$1" == https://api.github.com/* ]]; then
    auth=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
  fi
  curl -fsSL --retry 3 -A "weft-check-pins" ${auth[@]+"${auth[@]}"} "$1"
}

# The one capture group of an extended regex, read from a file.
pin_in() { # file regex
  local found
  found="$(sed -nE "s|.*$2.*|\1|p" "$1" | head -n1)"
  if [[ -z "$found" ]]; then
    echo "check-pins: no pin matching /$2/ in $1 (the line moved: update this script)" >&2
    exit 2
  fi
  printf '%s' "$found"
}

github_release() { curl_json "https://api.github.com/repos/$1/releases/latest" | jq -r '.tag_name'; }
# The newest release whose tag starts with a prefix, the prefix dropped: for
# a repository that releases several things (nextest tags its library too).
github_release_prefixed() { # repo prefix
  curl_json "https://api.github.com/repos/$1/releases?per_page=50" \
    | jq -r --arg p "$2" '[.[] | select(.prerelease | not) | .tag_name | select(startswith($p))][0] // "" | ltrimstr($p)'
}
npm_tag() { curl_json "https://registry.npmjs.org/$1" | jq -r --arg t "$2" '."dist-tags"[$t]'; }
crate_latest() { curl_json "https://crates.io/api/v1/crates/$1" | jq -r '.crate.max_stable_version'; }
docker_static_latest() {
  curl -fsSL --retry 3 https://download.docker.com/linux/static/stable/x86_64/ \
    | grep -oE 'docker-[0-9]+\.[0-9]+\.[0-9]+\.tgz' | sed -E 's/docker-(.*)\.tgz/\1/' | sort -V | tail -n1
}
debian_stable_codename() { curl -fsSL --retry 3 https://deb.debian.org/debian/dists/stable/Release | sed -nE 's/^Codename: //p'; }

behind=()
check() { # what where pinned latest
  local what="$1" where="$2" pinned="$3" latest="$4"
  if [[ -z "$latest" || "$latest" == "null" ]]; then
    echo "check-pins: could not read the newest release of ${what}" >&2
    exit 2
  fi
  if [[ "$pinned" == "$latest" ]]; then
    printf '  ok      %-28s %s\n' "$what" "$pinned"
  else
    printf '  BEHIND  %-28s %s -> %s  (%s)\n' "$what" "$pinned" "$latest" "$where"
    behind+=("$what")
  fi
}

daemon=crates/weft-cli/src/commands/daemon.rs
worker_image=crates/weft-compiler/src/worker_image.rs
runtime_dockerfile=deploy/docker/runtime.Dockerfile
bridge_dockerfile=catalog/bailey/bridge/images/bridge/Dockerfile

echo "Version pins Dependabot cannot see:"

check "seaweedfs image" "$daemon, setup.sh" \
  "$(pin_in "$daemon" 'OBJECT_STORE_IMAGE: &str = "chrislusf/seaweedfs:([0-9.]+)"')" "$(github_release seaweedfs/seaweedfs)"
check "cloudflared image" "$daemon" \
  "$(pin_in "$daemon" 'TUNNEL_IMAGE: &str = "cloudflare/cloudflared:([0-9.]+)"')" "$(github_release cloudflare/cloudflared)"
check "worker base (Debian)" "$worker_image, deploy/docker/*.Dockerfile" \
  "$(pin_in "$worker_image" 'DEFAULT_BASE_IMAGE: &str = "debian:([a-z]+)-slim"')" "$(debian_stable_codename)"
check "Docker CLI (runtime image)" "$runtime_dockerfile" \
  "$(pin_in "$runtime_dockerfile" 'DOCKER_VERSION=([0-9.]+)')" "$(docker_static_latest)"

bridge_pnpm="$(pin_in "$bridge_dockerfile" 'pnpm@([0-9.]+)')"
check "pnpm (bridge image)" "$bridge_dockerfile" "$bridge_pnpm" "$(npm_tag pnpm "latest-${bridge_pnpm%%.*}")"
for pkg in extension-vscode/package.json extension-browser/package.json; do
  ext_pnpm="$(pin_in "$pkg" '"packageManager": "pnpm@([0-9.]+)"')"
  check "pnpm (${pkg%%/*})" "$pkg" "$ext_pnpm" "$(npm_tag pnpm "latest-${ext_pnpm%%.*}")"
done

check "@vscode/vsce" ".github/workflows/release.yml" \
  "$(pin_in .github/workflows/release.yml '@vscode/vsce@([0-9.]+)')" "$(npm_tag @vscode/vsce latest)"
check "ovsx" ".github/workflows/release.yml" \
  "$(pin_in .github/workflows/release.yml 'ovsx@([0-9.]+)')" "$(npm_tag ovsx latest)"
check "cargo-hakari" ".github/workflows/ci.yml" \
  "$(pin_in .github/workflows/ci.yml 'cargo-hakari --version ([0-9.]+)')" "$(crate_latest cargo-hakari)"
check "cargo-nextest" ".github/workflows/ci.yml" \
  "$(pin_in .github/workflows/ci.yml 'NEXTEST_VERSION: ([0-9.]+)')" "$(github_release_prefixed nextest-rs/nextest cargo-nextest-)"
check "mdbook" ".github/workflows/docs.yml" \
  "$(pin_in .github/workflows/docs.yml 'MDBOOK_VERSION: ([0-9.]+)')" "$(github_release_prefixed rust-lang/mdBook v)"
check "mdbook-mermaid" ".github/workflows/docs.yml" \
  "$(pin_in .github/workflows/docs.yml 'MDBOOK_MERMAID_VERSION: ([0-9.]+)')" "$(github_release_prefixed badboy/mdbook-mermaid v)"
check "Rust toolchain" "rust-toolchain.toml" \
  "$(pin_in rust-toolchain.toml 'channel = "([0-9.]+)"')" "$(github_release rust-lang/rust)"

if ((${#behind[@]})); then
  echo
  echo "${#behind[@]} pin(s) behind their newest release: ${behind[*]}"
  exit 1
fi
echo
echo "Every pin is on its newest release."
