package network

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/albertopastormr/samsa/internal/protocol"
)

type Client struct {
	conn net.Conn
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) SendRequest(apiKey, apiVersion int16, correlationID int32, clientId *string, body protocol.Storable) error {
	header := protocol.RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientId:      clientId,
	}

	payloadSize := header.TotalSize() + body.TotalSize()
	w := protocol.NewWriter(4 + payloadSize)
	w.WriteInt32(int32(payloadSize))
	header.Encode(w)
	body.Encode(w)

	_, err := c.conn.Write(w.Bytes())
	return err
}

func (c *Client) ReceiveResponse() (*protocol.Reader, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, sizeBuf); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf)

	respBuf := make([]byte, size)
	if _, err := io.ReadFull(c.conn, respBuf); err != nil {
		return nil, err
	}

	return protocol.NewReader(respBuf), nil
}
