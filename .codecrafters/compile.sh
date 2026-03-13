# Server build script

set -e # Exit on failure

go build -o /tmp/codecrafters-build-kafka-go cmd/server/*.go
