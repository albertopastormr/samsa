package protocol

import "encoding/binary"

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Topics         []FetchResponseTopic
}

type FetchResponseTopic struct {
	TopicId    [16]byte
	Partitions []FetchResponsePartition
}

type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []byte // Nullable/Compact
	PreferredReadReplica int32
	Records              []byte // Compact
}

type FetchRequest struct {
	MaxWaitMs       int32
	MinBytes        int32
	MaxBytes        int32
	IsolationLevel  int8
	SessionId       int32
	SessionEpoch    int32
	Topics          []FetchRequestTopic
	ForgottenTopics []FetchRequestForgottenTopic
	RackId          string
}

type FetchRequestTopic struct {
	TopicId    [16]byte
	Partitions []FetchRequestPartition
}

type FetchRequestPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int64
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

type FetchRequestForgottenTopic struct {
	TopicId    [16]byte
	Partitions []int32
}

func DecodeFetchRequest(r *Reader) FetchRequest {
	req := FetchRequest{}
	req.MaxWaitMs = r.ReadInt32()
	req.MinBytes = r.ReadInt32()
	req.MaxBytes = r.ReadInt32()
	req.IsolationLevel = r.ReadInt8()
	req.SessionId = r.ReadInt32()
	req.SessionEpoch = r.ReadInt32()

	// Topics array (Compact)
	topicCountVar, _ := binary.Uvarint(r.buf[r.pos:])
	r.pos += 1 // assume length < 128 for simplicity in this stage
	topicCount := int(topicCountVar) - 1

	if topicCount > 0 {
		req.Topics = make([]FetchRequestTopic, topicCount)
		for i := 0; i < topicCount; i++ {
			t := FetchRequestTopic{}
			r.ReadBytes(t.TopicId[:])

			partCountVar, _ := binary.Uvarint(r.buf[r.pos:])
			r.pos += 1
			partCount := int(partCountVar) - 1

			t.Partitions = make([]FetchRequestPartition, partCount)
			for j := 0; j < partCount; j++ {
				p := FetchRequestPartition{}
				p.Partition = r.ReadInt32()
				p.CurrentLeaderEpoch = r.ReadInt32()
				p.FetchOffset = r.ReadInt64()
				p.LastFetchedEpoch = r.ReadInt64()
				p.LogStartOffset = r.ReadInt64()
				p.PartitionMaxBytes = r.ReadInt32()
				r.pos += 1 // partition tag buffer
				t.Partitions[j] = p
			}
			r.pos += 1 // topic tag buffer
			req.Topics[i] = t
		}
	}

	// ForgottenTopics (Compact)
	forgottenCountVar, _ := binary.Uvarint(r.buf[r.pos:])
	r.pos += 1
	forgottenCount := int(forgottenCountVar) - 1
	for i := 0; i < forgottenCount; i++ {
		r.pos += 16 // TopicId
		pCountVar, _ := binary.Uvarint(r.buf[r.pos:])
		r.pos += 1
		pCount := int(pCountVar) - 1
		r.pos += pCount * 4 // Partitions
		r.pos += 1          // tag buffer
	}

	req.RackId = r.ReadCompactString()
	// main tag buffer is already handled by remaining or explicitly if needed
	if r.pos < len(r.buf) {
		r.pos++
	}

	return req
}

func (f *FetchResponse) TotalSize() int {
	size := 4 // Correlation ID
	size += 1 // Main Tag Buffer
	size += 4 // ThrottleTimeMs
	size += 2 // ErrorCode
	size += 4 // SessionId
	size += 1 // Responses Compact Array length

	for _, topic := range f.Topics {
		size += 16 // TopicId
		size += 1  // Partitions Compact Array length
		for _, part := range topic.Partitions {
			size += 4                                 // PartitionIndex
			size += 2                                 // ErrorCode
			size += 8                                 // HighWatermark
			size += 8                                 // LastStableOffset
			size += 8                                 // LogStartOffset
			size += 1 + len(part.AbortedTransactions) // AbortedTransactions
			size += 4                                 // PreferredReadReplica
			size += 1 + len(part.Records)             // Records size (Compact)
			size += 1                                 // Tag buffer
		}
		size += 1 // Topic tag buffer
	}

	size += 1 // Final Tag Buffer
	return size
}

func (f *FetchResponse) Encode(w *Writer, correlationID int32) {
	w.WriteInt32(correlationID)
	w.WriteUint8(0) // Response Header V1 Tag Buffer

	w.WriteInt32(f.ThrottleTimeMs)
	w.WriteInt16(f.ErrorCode)
	w.WriteInt32(f.SessionId)

	// Topics
	w.WriteUint8(uint8(len(f.Topics) + 1))
	for _, topic := range f.Topics {
		w.WriteBytes(topic.TopicId[:])
		w.WriteUint8(uint8(len(topic.Partitions) + 1))
		for _, part := range topic.Partitions {
			w.WriteInt32(part.PartitionIndex)
			w.WriteInt16(part.ErrorCode)
			w.WriteInt64(part.HighWatermark)
			w.WriteInt64(part.LastStableOffset)
			w.WriteInt64(part.LogStartOffset)

			// AbortedTransactions (Compact Array)
			w.WriteUint8(1) // Empty
			w.WriteInt32(part.PreferredReadReplica)

			// Records (Compact Records)
			w.WriteUint8(1) // Empty

			w.WriteUint8(0) // Partition tags
		}
		w.WriteUint8(0) // Topic tags
	}

	w.WriteUint8(0) // Final Tag Buffer
}
