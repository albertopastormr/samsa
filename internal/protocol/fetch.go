package protocol

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
}

func (f *FetchResponse) TotalSize() int {
	size := 4 // Correlation ID
	size += 1 // Main Tag Buffer
	size += 4 // ThrottleTimeMs
	size += 2 // ErrorCode
	size += 4 // SessionId
	size += 1 // Responses Compact Array length (empty = 1)
	size += 1 // Final Tag Buffer
	return size
}

func (f *FetchResponse) Encode(w *Writer, correlationID int32) {
	w.WriteInt32(correlationID)
	w.WriteUint8(0) // Response Header V1 Tag Buffer (empty)

	w.WriteInt32(f.ThrottleTimeMs)
	w.WriteInt16(f.ErrorCode)
	w.WriteInt32(f.SessionId)

	// Responses (Compact Array)
	w.WriteUint8(1) // Empty array = length 0 + 1

	w.WriteUint8(0) // Final Tag Buffer
}
