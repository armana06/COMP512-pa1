#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT HUP INT TERM
JAVAC=${JAVAC:-javac}

compile() {
	if [ -n "${JAVAC_JAR:-}" ]; then
		java -jar "$JAVAC_JAR" "$@"
	else
		"$JAVAC" "$@"
	fi
}

cd "$SCRIPT_DIR"
compile -d "$BUILD_DIR" \
	Server/Server/Interface/*.java \
	Server/Server/Common/*.java \
	Server/Server/TCP/*.java \
	Client/Client/*.java \
	tests/Server/TCP/*.java

if ! java -cp "$BUILD_DIR" Server.TCP.TCPIntegrationTest >"$BUILD_DIR/test.log" 2>&1; then
	cat "$BUILD_DIR/test.log"
	exit 1
fi
tail -n 1 "$BUILD_DIR/test.log"
