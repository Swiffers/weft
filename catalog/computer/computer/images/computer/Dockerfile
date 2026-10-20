# A desktop and a shell for a program to work on: a virtual screen with a
# window manager, a terminal and a browser, xdotool for the mouse and the
# keyboard, ImageMagick for screenshots, and the HTTP service the computer
# nodes talk to (Python's standard library only).
FROM debian:trixie-slim

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      xvfb openbox xterm x11-xserver-utils xdotool imagemagick \
      firefox-esr fonts-dejavu-core \
      bash procps sudo git curl ca-certificates python3 tini openssh-server \
 && rm -rf /var/lib/apt/lists/*

# The user everything on the screen and in the shell runs as. It may use
# sudo, so a program can install what its work needs: the container is
# the boundary, and nothing inside it holds a credential.
# SYNC: the home folder <-> catalog/computer/computer/mod.rs (HOME), images/computer/computer.py (HOME)
ENV HOME=/home/user
RUN useradd --create-home --home-dir "$HOME" --shell /bin/bash user \
 && echo "user ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/user \
 && chmod 0440 /etc/sudoers.d/user \
 && mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

COPY computer.py start.sh /opt/computer/
RUN chmod 0755 /opt/computer/start.sh

# WEFT_COMPUTER_PORT, WEFT_COMPUTER_SCREEN, WEFT_COMPUTER_PACKAGES and
# WEFT_COMPUTER_SSH_KEYS are set by the Computer node's spec whenever weft
# runs the image, so the spec stays the one source of truth.
WORKDIR $HOME

# tini as the first process, so a program a command left behind is
# reaped when it ends instead of staying a zombie for the container's life.
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/computer/start.sh"]
