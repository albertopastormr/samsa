package protocol

type ApiVersionEntry struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

var SupportedApis = []ApiVersionEntry{
	{ApiKey: ApiKeyVersions, MinVersion: 0, MaxVersion: 4},
	{ApiKey: ApiKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0},
}

type ApiVersionsResponse struct {
	ErrorCode      int16
	ApiKeys        []ApiVersionEntry
	ThrottleTimeMs int32
}

func (resp *ApiVersionsResponse) Write(w *Writer) {
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
	size := SizeInt16                        // errorCode
	size += 1                                // api_keys array length (compact)
	size += len(resp.ApiKeys) * (SizeInt16 + // api_key
		SizeInt16 + // min_version
		SizeInt16 + // max_version
		SizeTagBuffer)
	size += SizeInt32     // throttle_time_ms
	size += SizeTagBuffer // main tag buffer
	return size
}
