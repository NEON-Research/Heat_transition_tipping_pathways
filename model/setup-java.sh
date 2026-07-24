#!/usr/bin/env bash
# setup-java.sh -- install a working JDK 17 into the Cowork Linux sandbox WITHOUT root.
#
# Why this exists: the sandbox has no Java and blocks `apt-get install` (no sudo) and external
# JDK downloads (github/adoptium/gradle are 403). But `apt-get download` from the Ubuntu mirror
# IS reachable, so we fetch the .deb packages, extract them into a user dir with `dpkg-deb -x`,
# and repair the config symlinks (Debian's JDK keeps conf/security in /etc via absolute links
# that break when extracted). Result: a usable javac + java.
#
# The sandbox is wiped between sessions, so re-run this each session (~1 min). Idempotent.
#
# Usage:
#   source model/setup-java.sh      # sets JAVA_HOME + PATH in the current shell
#   # or:
#   bash model/setup-java.sh && export PATH="$(cat /tmp/jdkroot/.javahome)/bin:$PATH"
set -euo pipefail

JDK_ROOT="${JDK_ROOT:-/tmp/jdkroot}"
JAVA_HOME_DIR="$JDK_ROOT/usr/lib/jvm/java-17-openjdk-amd64"
WORK="$(mktemp -d)"

# Fast path: already installed and working.
if [ -x "$JAVA_HOME_DIR/bin/javac" ] && "$JAVA_HOME_DIR/bin/javac" -version >/dev/null 2>&1; then
    echo "JDK already present at $JAVA_HOME_DIR"
else
    echo "Downloading JDK 17 .debs from the Ubuntu mirror..."
    cd "$WORK"
    # jre-headless carries libjvm.so; jdk-headless carries javac; ca-certificates-java is a dep.
    for pkg in openjdk-17-jre-headless openjdk-17-jdk-headless; do
        # retry once -- apt-get download occasionally truncates large files
        apt-get download "$pkg" >/dev/null 2>&1 || apt-get download "$pkg" >/dev/null 2>&1
    done

    echo "Extracting..."
    mkdir -p "$JDK_ROOT"
    for deb in openjdk-17-jre-headless*.deb openjdk-17-jdk-headless*.deb; do
        # verify the archive before extracting; re-download if corrupt
        if ! dpkg-deb --info "$deb" >/dev/null 2>&1; then
            echo "  $deb looked corrupt, re-downloading..."
            rm -f "$deb"; apt-get download "${deb%%_*}" >/dev/null 2>&1
            deb="$(ls ${deb%%_*}*.deb)"
        fi
        dpkg-deb -x "$deb" "$JDK_ROOT"
    done

    echo "Repairing config symlinks (Debian keeps conf/ in /etc)..."
    # conf/security/* are absolute symlinks into /etc/java-17-openjdk that don't exist here;
    # rebuild conf/ from the extracted /etc tree, dereferencing the links.
    if [ -d "$JDK_ROOT/etc/java-17-openjdk" ]; then
        rm -rf "$JAVA_HOME_DIR/conf"
        cp -rL "$JDK_ROOT/etc/java-17-openjdk" "$JAVA_HOME_DIR/conf"
    fi
fi

# Sanity check.
if ! "$JAVA_HOME_DIR/bin/javac" -version >/dev/null 2>&1; then
    echo "ERROR: javac still not working. See $JAVA_HOME_DIR" >&2
    exit 1
fi

# Record the path so callers can pick it up, and export for `source` usage.
echo "$JAVA_HOME_DIR" > "$JDK_ROOT/.javahome"
export JAVA_HOME="$JAVA_HOME_DIR"
export PATH="$JAVA_HOME/bin:$PATH"

echo "OK -> $("$JAVA_HOME/bin/javac" -version 2>&1) / $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
echo "JAVA_HOME=$JAVA_HOME"
echo "If you ran this with 'bash' (not 'source'), add to your shell:"
echo "  export JAVA_HOME=$JAVA_HOME && export PATH=\"\$JAVA_HOME/bin:\$PATH\""

rm -rf "$WORK"
