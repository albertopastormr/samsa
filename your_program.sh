#!/bin/sh
#
# Use this script to run the server LOCALLY.
#

set -e # Exit early if any commands fail

# Build the server binary
(
  cd "$(dirname "$0")"
  go build -o server_bin cmd/server/*.go
)

# Run the server binary
exec ./server_bin "$@"
