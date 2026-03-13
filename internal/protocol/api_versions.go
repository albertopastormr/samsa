package protocol

type ApiVersionsRequest struct{}

func (req *ApiVersionsRequest) Encode(w *Writer) {
	w.WriteUint8(0) // Tag Buffer
}

func (req *ApiVersionsRequest) TotalSize() int {
	return 1
}

type ApiVersionEntry struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

var SupportedApis = []ApiVersionEntry{
	{ApiKey: ApiKeyProduce, MinVersion: 0, MaxVersion: 11},
	{ApiKey: ApiKeyVersions, MinVersion: 0, MaxVersion: 4},
	{ApiKey: ApiKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0},
	{ApiKey: ApiKeyFetch, MinVersion: 0, MaxVersion: 16},
}

type ApiVersionsResponse struct {
	ErrorCode      int16
	ApiKeys        []ApiVersionEntry
	ThrottleTimeMs int32
}

func (resp *ApiVersionsResponse) Encode(w *Writer, correlationID int32) {
	w.WriteInt32(correlationID)
	w.WriteInt16(resp.ErrorCode)

	// Compact Array length: N+1
	w.WriteUint8(uint8(len(resp.ApiKeys) + 1))
	for _, entry := range resp.ApiKeys {
		w.WriteInt16(entry.ApiKey)
		w.WriteInt16(entry.MinVersion)
		w.WriteInt16(entry.MaxVersion)
		w.WriteUint8(0) // Tag Buffer for entry
	}

	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteUint8(0) // Main Tag Buffer
}

func (resp *ApiVersionsResponse) TotalSize() int {
	size := 4                                // correlation ID
	size += SizeInt16                        // errorCode
	size += 1                                // api_keys array length (compact)
	size += len(resp.ApiKeys) * (SizeInt16 + // api_key
		SizeInt16 + // min_version
		SizeInt16 + // max_version
		SizeTagBuffer)
	size += SizeInt32     // throttle_time_ms
	size += SizeTagBuffer // main tag buffer
	return size
}

func DecodeApiVersionsResponse(r *Reader) ApiVersionsResponse {
	resp := ApiVersionsResponse{}
	_ = r.ReadInt32() // CorrelationID (skip as it's checked by client)
	resp.ErrorCode = r.ReadInt16()

	// Compact Array length
	numApisVar, _ := r.ReadVarint()
	numApis := int(numApisVar) - 1
	if numApis > 0 {
		resp.ApiKeys = make([]ApiVersionEntry, numApis)
		for i := 0; i < numApis; i++ {
			entry := ApiVersionEntry{}
			entry.ApiKey = r.ReadInt16()
			entry.MinVersion = r.ReadInt16()
			entry.MaxVersion = r.ReadInt16()
			r.ReadUint8() // tag buffer
			resp.ApiKeys[i] = entry
		}
	}

	resp.ThrottleTimeMs = r.ReadInt32()
	r.ReadUint8() // main tag buffer

	return resp
}
