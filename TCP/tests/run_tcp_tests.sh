#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TCP_DIR=$(dirname "$SCRIPT_DIR")
SERVER_DIR="$TCP_DIR/Server"
TEST_CLASSES=$(mktemp -d "${TMPDIR:-/tmp}/tcp-pa1-tests.XXXXXX")

cleanup()
{
	rm -rf -- "$TEST_CLASSES"
}
trap cleanup EXIT HUP INT TERM

javac -d "$TEST_CLASSES" \
	"$SERVER_DIR"/Server/Interface/IResourceManager.java \
	"$SERVER_DIR"/Server/Common/*.java \
	"$SERVER_DIR"/Server/TCP/*.java \
	"$TCP_DIR"/Shared/*.java \
	"$TCP_DIR"/Client/Client/*.java \
	"$SCRIPT_DIR"/TCPIntegrationTest.java
java -ea -cp "$TEST_CLASSES" TCPIntegrationTest
