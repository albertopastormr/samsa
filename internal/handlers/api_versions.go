package handlers

import (
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleApiVersions(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	errorCode := int16(protocol.ErrNone)
	if header.ApiVersion < 0 || header.ApiVersion > 4 {
		errorCode = protocol.ErrUnsupportedVersion
	}

	return &protocol.ApiVersionsResponse{
		ErrorCode:      errorCode,
		ApiKeys:        protocol.SupportedApis,
		ThrottleTimeMs: 0,
	}, nil
}
