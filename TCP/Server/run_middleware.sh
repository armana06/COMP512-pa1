#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TCP_DIR=$(dirname "$SCRIPT_DIR")

make -C "$SCRIPT_DIR" compile-server-tcp
exec java -cp "$SCRIPT_DIR:$TCP_DIR" \
	Server.TCP.TCPMiddlewareServer "$@"
