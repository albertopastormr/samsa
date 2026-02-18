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

		// 3. Extract header fields
		// request_api_key: offset 0 (2 bytes)
		// request_api_version: offset 2 (2 bytes)
		// correlation_id: offset 4 (4 bytes)
		if len(requestBuf) < 8 {
			fmt.Println("Request too small to contain header")
			return
		}

		apiVersion := int16(binary.BigEndian.Uint16(requestBuf[2:4]))
		correlationID := binary.BigEndian.Uint32(requestBuf[4:8])

		// 4. Determine error_code
		// For ApiVersions (assumed API key 18), we support versions 0-4.
		var errorCode int16 = 0
		if apiVersion < 0 || apiVersion > 4 {
			errorCode = 35 // UNSUPPORTED_VERSION
		}

		// 5. Construct Response Body (v4)
		// Header (Response Header v0): correlation_id (4 bytes)
		// Body:
		//   error_code (INT16) - 2 bytes
		//   api_keys length (COMPACT_ARRAY) - VARINT (1 byte for small arrays)
		//   api_key 18 entry:
		//     api_key (INT16) - 2 bytes
		//     min_version (INT16) - 2 bytes
		//     max_version (INT16) - 2 bytes
		//     TAG_BUFFER - 1 byte
		//   throttle_time_ms (INT32) - 4 bytes
		//   TAG_BUFFER - 1 byte
		// Total: 4 (header) + 2 + 1 + 7 + 4 + 1 = 19 bytes payload

		resp := make([]byte, 4+19)                           // 4 for size + 19 for payload
		binary.BigEndian.PutUint32(resp[0:4], 19)            // message_size (header + body)
		binary.BigEndian.PutUint32(resp[4:8], correlationID) // correlation_id

		// Body starts at offset 8
		binary.BigEndian.PutUint16(resp[8:10], uint16(errorCode))
		resp[10] = 2 // api_keys array length (1 element + 1 for compact array)

		// Entry for API 18 (ApiVersions)
		binary.BigEndian.PutUint16(resp[11:13], 18) // api_key
		binary.BigEndian.PutUint16(resp[13:15], 0)  // min_version
		binary.BigEndian.PutUint16(resp[15:17], 4)  // max_version
		resp[17] = 0                                // Entry Tag Buffer

		binary.BigEndian.PutUint32(resp[18:22], 0) // throttle_time_ms
		resp[22] = 0                               // Main Tag Buffer

		if _, err := conn.Write(resp); err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			return
		}
	}
}
