# Makefile for Samsa (Kafka clone)

.PHONY: build test clean run-server

BINARY_NAME=samsa
BIN_DIR=bin
MAIN_PATH=./cmd/samsa

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY_NAME) $(MAIN_PATH)

test:
	go test -v ./...

clean:
	rm -rf $(BIN_DIR)
	rm -f *.log

run-server:
	go run $(MAIN_PATH) server
