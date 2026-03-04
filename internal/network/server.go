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
		s.dispatch(conn, header, reader)
	}
}

func (s *Server) dispatch(conn net.Conn, header protocol.RequestHeader, reader *protocol.Reader) {
	switch header.ApiKey {
	case protocol.ApiKeyVersions:
		s.handleApiVersions(conn, header, reader)
	case protocol.ApiKeyDescribeTopicPartitions:
		s.handleDescribeTopicPartitions(conn, header, reader)
	default:
		// Unknown API, for now just close or ignore
		fmt.Printf("Unknown ApiKey: %d\n", header.ApiKey)
	}
}

func (s *Server) sendResponse(conn net.Conn, payloadSize int, writeBody func(*protocol.Writer)) {
	writer := protocol.NewWriter(4 + payloadSize)
	writer.WriteInt32(int32(payloadSize))
	writeBody(writer)

	if _, err := conn.Write(writer.Bytes()); err != nil {
		fmt.Printf("Error writing response: %v\n", err)
	}
}

func (s *Server) handleDescribeTopicPartitions(conn net.Conn, header protocol.RequestHeader, reader *protocol.Reader) {
	// Parse remainder of body
	req := protocol.DecodeDescribeTopicPartitionsRequest(reader)

	topics := make([]protocol.DescribeTopicResponseTopic, len(req.Topics))
	for i, topicName := range req.Topics {
		topics[i] = protocol.DescribeTopicResponseTopic{
			ErrorCode:                 protocol.ErrUnknownTopicOrPartition,
			Name:                      topicName,
			TopicId:                   [16]byte{},
			IsInternal:                false,
			TopicAuthorizedOperations: 0,
		}
	}

	respBody := protocol.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		NextCursor:     -1,
	}

	payloadSize := respBody.TotalSize()
	s.sendResponse(conn, payloadSize, func(w *protocol.Writer) {
		respBody.Write(w, header.CorrelationID)
	})
}

func (s *Server) handleApiVersions(conn net.Conn, header protocol.RequestHeader, reader *protocol.Reader) {
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
	s.sendResponse(conn, payloadSize, func(w *protocol.Writer) {
		w.WriteInt32(header.CorrelationID)
		respBody.Write(w)
	})
}
