#!/bin/sh
# Runs as root, once per start: installs the extra packages the node asks
# for, hands the home folder (a disk kept across stop) to the user,
# prepares SSH when the node was given keys, then runs the computer as that
# user (which starts the SSH server and watches it).
# Any failure ends the container with its cause in `weft infra logs`.
set -eu

# SYNC: WEFT_COMPUTER_PACKAGES, WEFT_COMPUTER_SSH_KEYS <-> catalog/computer/computer/mod.rs, images/computer/computer.py (WEFT_COMPUTER_SSH_KEYS)
# SYNC: OUTPUT_DIR <-> catalog/computer/computer/images/computer/computer.py
OUTPUT_DIR=/var/tmp/weft-computer

if [ -n "${WEFT_COMPUTER_PACKAGES:-}" ]; then
  echo "computer: installing ${WEFT_COMPUTER_PACKAGES}" >&2
  apt-get update -qq >&2
  # shellcheck disable=SC2086 # one word per package, split on purpose
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ${WEFT_COMPUTER_PACKAGES} >&2
fi

mkdir -p "$HOME"
chown user:user "$HOME"
install -d -o user -g user -m 700 "$OUTPUT_DIR"

if [ -n "${WEFT_COMPUTER_SSH_KEYS:-}" ]; then
  # The host key lives on the home disk, so the computer keeps the same
  # fingerprint across stops and a client's known_hosts stays right.
  # SYNC: host_keys <-> catalog/computer/computer/images/computer/computer.py (SSH_HOST_KEY)
  host_keys="$HOME/.weft/ssh"
  install -d -o user -g user -m 755 "$HOME/.weft"
  install -d -m 700 "$host_keys"
  [ -f "$host_keys/ssh_host_ed25519_key" ] || ssh-keygen -q -t ed25519 -N "" -f "$host_keys/ssh_host_ed25519_key"
  # The node's keys are the ones that get in: written at every start.
  install -d -o user -g user -m 700 "$HOME/.ssh"
  printf '%s\n' "$WEFT_COMPUTER_SSH_KEYS" > "$HOME/.ssh/authorized_keys"
  chown user:user "$HOME/.ssh/authorized_keys"
  chmod 600 "$HOME/.ssh/authorized_keys"
  mkdir -p /run/sshd
fi

exec runuser -u user -- python3 /opt/computer/computer.py
