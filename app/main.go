package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		// 1. Read message_size (4 bytes)
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading message size: %v\n", err)
			}
			return
		}
		messageSize := int32(binary.BigEndian.Uint32(sizeBuf))

		// 2. Read the rest of the message as indicated by messageSize
		requestBuf := make([]byte, messageSize)
		if _, err := io.ReadFull(conn, requestBuf); err != nil {
			fmt.Printf("Error reading request body: %v\n", err)
			return
		}

		// 3. Extract correlation_id (request_api_key: 2, request_api_version: 2, correlation_id: 4)
		// It starts at offset 4 of the request payload (after size field)
		if len(requestBuf) < 8 {
			fmt.Println("Request too small to contain correlation_id")
			return
		}
		correlationID := binary.BigEndian.Uint32(requestBuf[4:8])

		// 4. Send 8-byte response: 4 bytes message_size + 4 bytes correlation_id
		resp := make([]byte, 8)
		binary.BigEndian.PutUint32(resp[0:4], 0)             // message_size: 0
		binary.BigEndian.PutUint32(resp[4:8], correlationID) // echoed correlation_id

		if _, err := conn.Write(resp); err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			return
		}
	}
}
