package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

// handleConn handles a single client connection.
// It sends an 8-byte response: 4 bytes message_size (0) + 4 bytes correlation_id (7).
func handleConn(conn net.Conn) {
	defer conn.Close()

	// Build the 8-byte response using direct big-endian encoding (no reflection).
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], 0) // message_size: 0 (any value works for this stage)
	binary.BigEndian.PutUint32(buf[4:8], 7) // correlation_id: 7

	if _, err := conn.Write(buf[:]); err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
