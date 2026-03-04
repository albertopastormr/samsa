package handlers

import (
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleFetch(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	// For this stage, we don't need to parse the request body.
	// But we should consume it if we were doing more.
	// tester sends Fetch (v16) with empty array of topics.

	return &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionId:      0,
	}, nil
}
