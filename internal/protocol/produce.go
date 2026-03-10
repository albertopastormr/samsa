package protocol

type ProduceRequest struct {
	TransactionalId *string
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceRequestTopic
}

type ProduceRequestTopic struct {
	Name       string
	Partitions []ProduceRequestPartition
}

type ProduceRequestPartition struct {
	Index   int32
	Records []byte
}

type ProduceResponse struct {
	Responses      []ProduceResponseTopic
	ThrottleTimeMs int32
}

type ProduceResponseTopic struct {
	Name       string
	Partitions []ProduceResponsePartition
}

type ProduceResponsePartition struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
	RecordErrors    []ProduceResponseRecordError // Empty for now
	ErrorMessage    *string                      // Null for now
}

type ProduceResponseRecordError struct {
	BatchIndex             int32
	BatchIndexErrorMessage *string
}

func DecodeProduceRequest(r *Reader) ProduceRequest {
	req := ProduceRequest{}

	// TransactionalId (Compact Nullable String)
	lenTxnVar, _ := r.ReadVarint()
	lenTxn := int(lenTxnVar) - 1
	if lenTxn > 0 {
		str := string(r.Buf[r.Pos : r.Pos+lenTxn])
		req.TransactionalId = &str
		r.Pos += lenTxn
	}

	req.Acks = r.ReadInt16()
	req.TimeoutMs = r.ReadInt32()

	// Topics (Compact Array)
	lenTopicsVar, _ := r.ReadVarint()
	lenTopics := int(lenTopicsVar) - 1
	if lenTopics > 0 {
		req.Topics = make([]ProduceRequestTopic, lenTopics)
		for i := 0; i < lenTopics; i++ {
			topic := ProduceRequestTopic{}
			topic.Name = r.ReadCompactString()

			// Partitions (Compact Array)
			lenPartsVar, _ := r.ReadVarint()
			lenParts := int(lenPartsVar) - 1
			if lenParts > 0 {
				topic.Partitions = make([]ProduceRequestPartition, lenParts)
				for j := 0; j < lenParts; j++ {
					part := ProduceRequestPartition{}
					part.Index = r.ReadInt32()

					// Records (Compact Records, length encoded as Uvarint)
					lenRecordsVar, _ := r.ReadVarint()
					lenRecords := int(lenRecordsVar) - 1
					if lenRecords > 0 {
						part.Records = make([]byte, lenRecords)
						copy(part.Records, r.Buf[r.Pos:r.Pos+lenRecords])
						r.Pos += lenRecords
					}

					topic.Partitions[j] = part

					// Partition Tag Buffer
					tagCountVar, n := r.ReadVarint()
					if tagCountVar > 0 {
						// Skip tags, this is rudimentary but safe if no tags expected
						// A full parser needs to parse tag length and skip
					}
					_ = n
				}
			}

			// Topic Tag Buffer
			r.ReadVarint() // 0 length for tag buffer usually

			req.Topics[i] = topic
		}
	}

	// Request Tag Buffer
	r.ReadVarint() // 0 length for tag buffer usually

	return req
}

func (resp *ProduceResponse) TotalSize() int {
	size := 4 // CorrelationID
	// Flexible Response Header V1 uses a TagBuffer (1 byte if empty)
	size += 1

	// Topics length (Compact Array)
	size += SizeVarint(uint64(len(resp.Responses) + 1))
	for _, topic := range resp.Responses {
		// Topic Name (Compact String)
		size += SizeVarint(uint64(len(topic.Name)+1)) + len(topic.Name)

		// Partitions length (Compact Array)
		size += SizeVarint(uint64(len(topic.Partitions) + 1))
		for range topic.Partitions {
			size += 4             // Index
			size += 2             // ErrorCode
			size += 8             // BaseOffset
			size += 8             // LogAppendTimeMs
			size += 8             // LogStartOffset
			size += SizeVarint(1) // RecordErrors array length (1 meaning 0 elements)
			size += SizeVarint(0) // ErrorMessage length (0 meaning Null string)
			size += 1             // TagBuffer for current_leader (we omit tags, so empty tag buffer)
		}
		size += 1 // TagBuffer for Topic
	}

	size += 4 // ThrottleTimeMs
	size += 1 // TagBuffer for Response Tag Buffer

	return size
}

func (resp *ProduceResponse) Encode(w *Writer, correlationID int32) {
	w.WriteInt32(correlationID)
	w.WriteUint8(0) // Tag Buffer for Response Header V1

	// Topics
	w.WriteVarint(uint64(len(resp.Responses) + 1))
	for _, topic := range resp.Responses {
		w.WriteCompactString(topic.Name)

		w.WriteVarint(uint64(len(topic.Partitions) + 1))
		for _, part := range topic.Partitions {
			w.WriteInt32(part.Index)
			w.WriteInt16(part.ErrorCode)
			w.WriteInt64(part.BaseOffset)
			w.WriteInt64(part.LogAppendTimeMs)
			w.WriteInt64(part.LogStartOffset)
			w.WriteVarint(1) // RecordErrors (empty)
			w.WriteVarint(0) // ErrorMessage (Null)
			w.WriteUint8(0)  // Tag Buffer for Partition
		}

		w.WriteUint8(0) // Tag Buffer for Topic
	}

	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteUint8(0) // Tag Buffer for Response
}
