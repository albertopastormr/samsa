package protocol

type DescribeTopicPartitionsRequest struct {
	Topics                 []string
	ResponsePartitionLimit int32
	Cursor                 int8
}

func DecodeDescribeTopicPartitionsRequest(r *Reader) DescribeTopicPartitionsRequest {
	req := DescribeTopicPartitionsRequest{}

	// topics COMPACT_ARRAY
	topicCount := r.ReadInt8() - 1
	if topicCount > 0 {
		req.Topics = make([]string, topicCount)
		for i := 0; i < int(topicCount); i++ {
			req.Topics[i] = r.ReadCompactString()
			// TAG_BUFFER for topic
			_ = r.ReadInt8()
		}
	}

	req.ResponsePartitionLimit = r.ReadInt32()
	req.Cursor = r.ReadInt8()

	// TAG_BUFFER for main body
	// _ = r.ReadInt8()

	return req
}

type DescribeTopicResponseTopic struct {
	ErrorCode                 int16
	Name                      string
	TopicId                   [16]byte
	IsInternal                bool
	TopicAuthorizedOperations int32
}

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32
	Topics         []DescribeTopicResponseTopic
	NextCursor     int8
}

func (resp *DescribeTopicPartitionsResponse) Encode(w *Writer, correlationID int32) {
	// Header v1
	w.WriteInt32(correlationID)
	w.WriteUint8(0) // TAG_BUFFER for header

	// Body v0
	w.WriteInt32(resp.ThrottleTimeMs)

	// Topics array (Compact)
	w.WriteUint8(uint8(len(resp.Topics) + 1))
	for _, topic := range resp.Topics {
		w.WriteInt16(topic.ErrorCode)
		w.WriteCompactString(topic.Name)
		w.WriteBytes(topic.TopicId[:])
		if topic.IsInternal {
			w.WriteInt8(1)
		} else {
			w.WriteInt8(0)
		}
		// Empty partitions array (Compact array = 1 byte for null/empty = length 1 -> size 0)
		w.WriteUint8(1)
		w.WriteInt32(topic.TopicAuthorizedOperations)
		w.WriteUint8(0) // TAG_BUFFER for topic
	}

	w.WriteInt8(resp.NextCursor)
	w.WriteUint8(0) // TAG_BUFFER for main response
}

func (resp *DescribeTopicPartitionsResponse) TotalSize() int {
	size := 4 + 1 // Header: correlation id (4) + tag buffer (1)
	size += 4     // ThrottleTimeMs
	size += 1     // Topics array length
	for _, topic := range resp.Topics {
		size += 2                   // ErrorCode
		size += 1 + len(topic.Name) // Compact String length + content
		size += 16                  // TopicId
		size += 1                   // IsInternal
		size += 1                   // Partitions array length
		size += 4                   // TopicAuthorizedOperations
		size += 1                   // TAG_BUFFER
	}
	size += 1 // NextCursor
	size += 1 // TAG_BUFFER for main response
	return size
}
