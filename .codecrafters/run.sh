#!/bin/sh
# Server run script

set -e # Exit on failure

exec /tmp/codecrafters-build-kafka-go server "$@"
