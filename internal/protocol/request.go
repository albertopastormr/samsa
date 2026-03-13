package protocol

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientId      *string
}

func DecodeRequestHeader(r *Reader) RequestHeader {
	req := RequestHeader{
		ApiKey:        r.ReadInt16(),
		ApiVersion:    r.ReadInt16(),
		CorrelationID: r.ReadInt32(),
	}

	if r.Remaining() >= 2 {
		req.ClientId = r.ReadNullableString()
	}

	// Header V2 applies to ApiVersions v3+ and DescribeTopicPartitions v0+
	isHeaderV2 := false
	if req.ApiKey == ApiKeyVersions && req.ApiVersion >= 3 {
		isHeaderV2 = true
	} else if req.ApiKey == ApiKeyDescribeTopicPartitions {
		isHeaderV2 = true
	} else if req.ApiKey == ApiKeyFetch && req.ApiVersion >= 12 {
		isHeaderV2 = true
	} else if req.ApiKey == ApiKeyProduce && req.ApiVersion >= 9 {
		isHeaderV2 = true
	}

	if isHeaderV2 && r.Remaining() > 0 {
		r.ReadVarint() // TAG_BUFFER count, usually 0
	}

	return req
}

func (h *RequestHeader) Encode(w *Writer) {
	w.WriteInt16(h.ApiKey)
	w.WriteInt16(h.ApiVersion)
	w.WriteInt32(h.CorrelationID)

	if h.ClientId != nil {
		w.WriteInt16(int16(len(*h.ClientId)))
		w.WriteBytes([]byte(*h.ClientId))
	} else {
		w.WriteInt16(-1)
	}

	// Header V2 applies to ApiVersions v3+ and DescribeTopicPartitions v0+
	isHeaderV2 := false
	if h.ApiKey == ApiKeyVersions && h.ApiVersion >= 3 {
		isHeaderV2 = true
	} else if h.ApiKey == ApiKeyDescribeTopicPartitions {
		isHeaderV2 = true
	} else if h.ApiKey == ApiKeyFetch && h.ApiVersion >= 12 {
		isHeaderV2 = true
	} else if h.ApiKey == ApiKeyProduce && h.ApiVersion >= 9 {
		isHeaderV2 = true
	}

	if isHeaderV2 {
		w.WriteUint8(0) // TAG_BUFFER
	}
}

func (h *RequestHeader) TotalSize() int {
	size := 8 // ApiKey(2) + ApiVersion(2) + CorrelationID(4)
	if h.ClientId != nil {
		size += 2 + len(*h.ClientId)
	} else {
		size += 2
	}

	isHeaderV2 := false
	if h.ApiKey == ApiKeyVersions && h.ApiVersion >= 3 {
		isHeaderV2 = true
	} else if h.ApiKey == ApiKeyDescribeTopicPartitions {
		isHeaderV2 = true
	} else if h.ApiKey == ApiKeyFetch && h.ApiVersion >= 12 {
		isHeaderV2 = true
	} else if h.ApiKey == ApiKeyProduce && h.ApiVersion >= 9 {
		isHeaderV2 = true
	}

	if isHeaderV2 {
		size += 1
	}
	return size
}
