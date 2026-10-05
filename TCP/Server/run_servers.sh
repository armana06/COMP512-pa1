#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
SESSION=pa1-tcp

if ! command -v tmux >/dev/null 2>&1; then
	echo "tmux is required for this convenience launcher." >&2
	exit 1
fi
if tmux has-session -t "$SESSION" 2>/dev/null; then
	echo "tmux session '$SESSION' already exists." >&2
	exit 1
fi

make -C "$PROJECT_DIR" all
tmux new-session -d -s "$SESSION" -n flights \
	"cd \"$SCRIPT_DIR\" && java -cp \"$PROJECT_DIR/build\" Server.TCP.TCPResourceManagerServer Flights"
tmux new-window -t "$SESSION" -n cars \
	"cd \"$SCRIPT_DIR\" && java -cp \"$PROJECT_DIR/build\" Server.TCP.TCPResourceManagerServer Cars"
tmux new-window -t "$SESSION" -n rooms \
	"cd \"$SCRIPT_DIR\" && java -cp \"$PROJECT_DIR/build\" Server.TCP.TCPResourceManagerServer Rooms"
tmux new-window -t "$SESSION" -n middleware \
	"sleep 2; cd \"$SCRIPT_DIR\" && java -cp \"$PROJECT_DIR/build\" Server.TCP.TCPMiddlewareServer"
tmux select-window -t "$SESSION:middleware"
exec tmux attach-session -t "$SESSION"
