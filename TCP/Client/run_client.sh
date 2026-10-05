#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

make -C "$PROJECT_DIR" all
exec java -cp "$PROJECT_DIR/build" Client.TCPClient "$@"
