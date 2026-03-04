package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/albertopastormr/samsa/internal/handlers"
	"github.com/albertopastormr/samsa/internal/protocol"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate a reasonably sized buffer for small requests/responses
		return make([]byte, 1024)
	},
}

type HandlerFunc func(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error)

type Server struct {
	addr     string
	handlers map[int16]HandlerFunc
}

func NewServer(addr string) *Server {
	s := &Server{
		addr:     addr,
		handlers: make(map[int16]HandlerFunc),
	}
	s.registerHandlers()
	return s
}

func (s *Server) registerHandlers() {
	s.handlers[protocol.ApiKeyVersions] = handlers.HandleApiVersions
	s.handlers[protocol.ApiKeyDescribeTopicPartitions] = handlers.HandleDescribeTopicPartitions
	s.handlers[protocol.ApiKeyFetch] = handlers.HandleFetch
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
		fmt.Printf("Received request: ApiKey=%d, ApiVersion=%d, CorrelationID=%d\n", header.ApiKey, header.ApiVersion, header.CorrelationID)

		// 4. Handle Request
		encoder, err := s.dispatch(header, reader)
		if err != nil {
			fmt.Printf("Error handling request: %v\n", err)
			return
		}

		if encoder != nil {
			s.sendResponse(conn, encoder, header.CorrelationID)
		}
	}
}

func (s *Server) dispatch(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	handler, ok := s.handlers[header.ApiKey]
	if !ok {
		return nil, fmt.Errorf("unknown ApiKey: %d", header.ApiKey)
	}
	return handler(header, reader)
}

func (s *Server) sendResponse(conn net.Conn, encoder protocol.Encoder, correlationID int32) {
	payloadSize := encoder.TotalSize()
	writer := protocol.NewWriter(4 + payloadSize)
	writer.WriteInt32(int32(payloadSize))
	encoder.Encode(writer, correlationID)

	if _, err := conn.Write(writer.Bytes()); err != nil {
		fmt.Printf("Error writing response: %v\n", err)
	}
}
