#!/bin/sh
#
# Use this script to run the server LOCALLY.
#

set -e # Exit early if any commands fail

# Build the samsa binary
(
  cd "$(dirname "$0")"
  go build -o samsa cmd/samsa/main.go
)

# Run the samsa server
exec ./samsa server "$@"
