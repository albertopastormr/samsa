package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/albertopastormr/samsa/internal/protocol"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate a reasonably sized buffer for small requests/responses
		return make([]byte, 1024)
	},
}

type Server struct {
	addr string
}

func NewServer(addr string) *Server {
	return &Server{addr: addr}
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
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

		// 2. Read the body
		requestBuf := make([]byte, messageSize)
		if _, err := io.ReadFull(conn, requestBuf); err != nil {
			fmt.Printf("Error reading request body: %v\n", err)
			return
		}

		// 3. Parse Header
		reader := protocol.NewReader(requestBuf)
		header := protocol.DecodeRequestHeader(reader)

		// 4. Handle Request
		s.dispatch(conn, header)
	}
}

func (s *Server) dispatch(conn net.Conn, header protocol.RequestHeader) {
	switch header.ApiKey {
	case protocol.ApiKeyVersions:
		s.handleApiVersions(conn, header)
	default:
		// Unknown API, for now just close or ignore
		fmt.Printf("Unknown ApiKey: %d\n", header.ApiKey)
	}
}

func (s *Server) handleApiVersions(conn net.Conn, header protocol.RequestHeader) {
	errorCode := int16(protocol.ErrNone)
	if header.ApiVersion < 0 || header.ApiVersion > 4 {
		errorCode = protocol.ErrUnsupportedVersion
	}

	respBody := protocol.ApiVersionsResponse{
		ErrorCode:      errorCode,
		ApiKeys:        protocol.SupportedApis,
		ThrottleTimeMs: 0,
	}

	// 4 bytes for correlation_id + body size
	payloadSize := 4 + respBody.TotalSize()

	// Create response writer
	// 4 bytes for message_size + payloadSize
	writer := protocol.NewWriter(4 + payloadSize)
	writer.WriteInt32(int32(payloadSize))
	writer.WriteInt32(header.CorrelationID)
	respBody.Write(writer)

	if _, err := conn.Write(writer.Bytes()); err != nil {
		fmt.Printf("Error writing response: %v\n", err)
	}
}
