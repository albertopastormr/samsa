package protocol

import (
	"bytes"
	"testing"
)

func TestApiVersionsResponse_Write(t *testing.T) {
	resp := ApiVersionsResponse{
		ErrorCode: ErrNone,
		ApiKeys: []ApiVersionEntry{
			{ApiKey: ApiKeyVersions, MinVersion: 0, MaxVersion: 4},
		},
		ThrottleTimeMs: 0,
	}

	size := resp.TotalSize()
	w := NewWriter(size)
	resp.Write(w)

	got := w.Bytes()
	// Expected body (v4):
	// errorCode(2) + arrayLen(1) + [key(2)+min(2)+max(2)+tag(1)] + throttle(4) + tag(1)
	// 0000          02            0012   0000   0004   00         00000000    00
	expected := []byte{
		0x00, 0x00, // ErrorCode
		0x02,                                     // Compact Array Length (1 entry + 1)
		0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, // ApiKey 18, v0-4, tag
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
