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
