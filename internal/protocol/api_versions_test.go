package protocol

import (
	"bytes"
	"testing"
)

func TestApiVersionsResponse_Encode(t *testing.T) {
	resp := ApiVersionsResponse{
		ErrorCode:      ErrNone,
		ApiKeys:        SupportedApis,
		ThrottleTimeMs: 0,
	}

	size := resp.TotalSize()
	w := NewWriter(size)
	resp.Encode(w, 0x67890abc)

	got := w.Bytes()
	expected := []byte{
		0x67, 0x89, 0x0a, 0xbc, // Correlation ID
		0x00, 0x00, // ErrorCode
		0x05,                                     // Compact Array Length (4 entries + 1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00, // ApiKey 0 (Produce), v0-11, tag
		0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, // ApiKey 18, v0-4, tag
		0x00, 0x4B, 0x00, 0x00, 0x00, 0x00, 0x00, // ApiKey 75, v0-0, tag
		0x00, 0x01, 0x00, 0x00, 0x00, 0x10, 0x00, // ApiKey 1, v0-16, tag
		0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs
		0x00, // Main Tag Buffer
	}

	if !bytes.Equal(got, expected) {
		t.Errorf("ApiVersionsResponse.Write() = \n%x\nwant\n%x", got, expected)
	}

	if size != len(expected) {
		t.Errorf("TotalSize() = %d; want %d", size, len(expected))
	}
}

func TestDecodeRequestHeader(t *testing.T) {
	data := []byte{
		0x00, 0x12, // ApiKey 18
		0x00, 0x04, // ApiVersion 4
		0x67, 0x89, 0x0a, 0xbc, // CorrelationID
	}
	r := NewReader(data)
	header := DecodeRequestHeader(r)

	if header.ApiKey != ApiKeyVersions {
		t.Errorf("ApiKey = %d; want %d", header.ApiKey, ApiKeyVersions)
	}
	if header.ApiVersion != 4 {
		t.Errorf("ApiVersion = %d; want 4", header.ApiVersion)
	}
	if header.CorrelationID != 0x67890abc {
		t.Errorf("CorrelationID = %x; want %x", header.CorrelationID, 0x67890abc)
	}
}
