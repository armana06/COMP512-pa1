#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TCP_DIR=$(dirname "$SCRIPT_DIR")
SERVER_DIR="$TCP_DIR/Server"

make -C "$SCRIPT_DIR" compile-client
exec java -cp "$SCRIPT_DIR:$SERVER_DIR/RMIInterface.jar:$TCP_DIR" \
	Client.TCPClient "$@"
