package protocol

type CreateTopicsRequest struct {
	Topics       []CreateTopicsRequestTopic
	TimeoutMs    int32
	ValidateOnly bool
}

type CreateTopicsRequestTopic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Assignments       []CreateTopicsRequestAssignment
	Configs           []CreateTopicsRequestConfig
}

type CreateTopicsRequestAssignment struct {
	PartitionIndex int32
	BrokerIds      []int32
}

type CreateTopicsRequestConfig struct {
	Name  string
	Value *string
}

func (req *CreateTopicsRequest) Encode(w *Writer) {
	// Topics (Compact Array)
	w.WriteVarint(uint64(len(req.Topics) + 1))
	for _, t := range req.Topics {
		w.WriteCompactString(t.Name)
		w.WriteInt32(t.NumPartitions)
		w.WriteInt16(t.ReplicationFactor)

		// Assignments (Compact Array)
		w.WriteVarint(uint64(len(t.Assignments) + 1))
		for _, a := range t.Assignments {
			w.WriteInt32(a.PartitionIndex)
			w.WriteVarint(uint64(len(a.BrokerIds) + 1))
			for _, b := range a.BrokerIds {
				w.WriteInt32(b)
			}
			w.WriteUint8(0) // Tags
		}

		// Configs (Compact Array)
		w.WriteVarint(uint64(len(t.Configs) + 1))
		for _, c := range t.Configs {
			w.WriteCompactString(c.Name)
			if c.Value != nil {
				w.WriteVarint(uint64(len(*c.Value) + 1))
				w.WriteBytes([]byte(*c.Value))
			} else {
				w.WriteVarint(0)
			}
			w.WriteUint8(0) // Tags
		}
		w.WriteUint8(0) // Topic Tags
	}
	w.WriteInt32(req.TimeoutMs)
	if req.ValidateOnly {
		w.WriteUint8(1)
	} else {
		w.WriteUint8(0)
	}
	w.WriteUint8(0) // Request Tags
}

func (req *CreateTopicsRequest) TotalSize() int {
	size := 0
	size += SizeVarint(uint64(len(req.Topics) + 1))
	for _, t := range req.Topics {
		size += SizeCompactString(t.Name)
		size += 4 // NumPartitions
		size += 2 // ReplicationFactor

		size += SizeVarint(uint64(len(t.Assignments) + 1))
		for _, a := range t.Assignments {
			size += 4 // PartitionIndex
			size += SizeVarint(uint64(len(a.BrokerIds) + 1))
			size += len(a.BrokerIds) * 4
			size += 1 // Tags
		}

		size += SizeVarint(uint64(len(t.Configs) + 1))
		for _, c := range t.Configs {
			size += SizeCompactString(c.Name)
			if c.Value != nil {
				size += SizeVarint(uint64(len(*c.Value) + 1)) + len(*c.Value)
			} else {
				size += 1
			}
			size += 1 // Tags
		}
		size += 1 // Topic Tags
	}
	size += 4 // TimeoutMs
	size += 1 // ValidateOnly
	size += 1 // Request Tags
	return size
}

func DecodeCreateTopicsRequest(r *Reader) CreateTopicsRequest {
	req := CreateTopicsRequest{}
	numTopicsVar, _ := r.ReadVarint()
	numTopics := int(numTopicsVar) - 1
	if numTopics > 0 {
		req.Topics = make([]CreateTopicsRequestTopic, numTopics)
		for i := 0; i < numTopics; i++ {
			t := CreateTopicsRequestTopic{}
			t.Name = r.ReadCompactString()
			t.NumPartitions = r.ReadInt32()
			t.ReplicationFactor = r.ReadInt16()

			// Assignments
			numAssVar, _ := r.ReadVarint()
			numAss := int(numAssVar) - 1
			if numAss > 0 {
				t.Assignments = make([]CreateTopicsRequestAssignment, numAss)
				for j := 0; j < numAss; j++ {
					a := CreateTopicsRequestAssignment{}
					a.PartitionIndex = r.ReadInt32()
					numBVar, _ := r.ReadVarint()
					numB := int(numBVar) - 1
					if numB > 0 {
						a.BrokerIds = make([]int32, numB)
						for k := 0; k < numB; k++ {
							a.BrokerIds[k] = r.ReadInt32()
						}
					}
					r.ReadUint8() // Tags
					t.Assignments[j] = a
				}
			}

			// Configs
			numConfVar, _ := r.ReadVarint()
			numConf := int(numConfVar) - 1
			if numConf > 0 {
				t.Configs = make([]CreateTopicsRequestConfig, numConf)
				for j := 0; j < numConf; j++ {
					c := CreateTopicsRequestConfig{}
					c.Name = r.ReadCompactString()
					valLenVar, _ := r.ReadVarint()
					valLen := int(valLenVar) - 1
					if valLen >= 0 {
						val := string(r.Buf[r.Pos : r.Pos+valLen])
						c.Value = &val
						r.Pos += valLen
					}
					r.ReadUint8() // Tags
					t.Configs[j] = c
				}
			}
			r.ReadUint8() // Topic Tags
			req.Topics[i] = t
		}
	}
	req.TimeoutMs = r.ReadInt32()
	req.ValidateOnly = r.ReadUint8() == 1
	r.ReadUint8() // Request Tags
	return req
}

type CreateTopicsResponse struct {
	ThrottleTimeMs int32
	Topics         []CreateTopicsResponseTopic
}

type CreateTopicsResponseTopic struct {
	Name              string
	ErrorCode         int16
	ErrorMessage      *string
	NumPartitions     int32
	ReplicationFactor int16
	Configs           []CreateTopicsResponseConfig
}

type CreateTopicsResponseConfig struct {
	Name         string
	Value        *string
	ReadOnly     bool
	ConfigSource int8
	IsDefault    bool
}

func (resp *CreateTopicsResponse) TotalSize() int {
	size := 4 // CorrelationID
	size += 1 // Header Tags
	size += 4 // ThrottleTimeMs
	size += SizeVarint(uint64(len(resp.Topics) + 1))
	for _, t := range resp.Topics {
		size += SizeCompactString(t.Name)
		size += 2 // ErrorCode
		if t.ErrorMessage != nil {
			size += SizeVarint(uint64(len(*t.ErrorMessage) + 1)) + len(*t.ErrorMessage)
		} else {
			size += 1
		}
		size += 4 // NumPartitions
		size += 2 // ReplicationFactor
		size += SizeVarint(uint64(len(t.Configs) + 1))
		for _, c := range t.Configs {
			size += SizeCompactString(c.Name)
			if c.Value != nil {
				size += SizeVarint(uint64(len(*c.Value) + 1)) + len(*c.Value)
			} else {
				size += 1
			}
			size += 1 // ReadOnly
			size += 1 // ConfigSource
			size += 1 // IsDefault
			size += 1 // Tags
		}
		size += 1 // Topic Tags
	}
	size += 1 // Main Tags
	return size
}

func (resp *CreateTopicsResponse) Encode(w *Writer, correlationID int32) {
	w.WriteInt32(correlationID)
	w.WriteUint8(0) // Header Tags
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteVarint(uint64(len(resp.Topics) + 1))
	for _, t := range resp.Topics {
		w.WriteCompactString(t.Name)
		w.WriteInt16(t.ErrorCode)
		if t.ErrorMessage != nil {
			w.WriteVarint(uint64(len(*t.ErrorMessage) + 1))
			w.WriteBytes([]byte(*t.ErrorMessage))
		} else {
			w.WriteVarint(0)
		}
		w.WriteInt32(t.NumPartitions)
		w.WriteInt16(t.ReplicationFactor)
		w.WriteVarint(uint64(len(t.Configs) + 1))
		for _, c := range t.Configs {
			w.WriteCompactString(c.Name)
			if c.Value != nil {
				w.WriteVarint(uint64(len(*c.Value) + 1))
				w.WriteBytes([]byte(*c.Value))
			} else {
				w.WriteVarint(0)
			}
			if c.ReadOnly {
				w.WriteUint8(1)
			} else {
				w.WriteUint8(0)
			}
			w.WriteInt8(c.ConfigSource)
			if c.IsDefault {
				w.WriteUint8(1)
			} else {
				w.WriteUint8(0)
			}
			w.WriteUint8(0) // Tags
		}
		w.WriteUint8(0) // Topic Tags
	}
	w.WriteUint8(0) // Main Tags
}

func DecodeCreateTopicsResponse(r *Reader) CreateTopicsResponse {
	resp := CreateTopicsResponse{}
	_ = r.ReadInt32() // CorrelationID
	_ = r.ReadUint8() // Header Tags
	resp.ThrottleTimeMs = r.ReadInt32()
	numTopicsVar, _ := r.ReadVarint()
	numTopics := int(numTopicsVar) - 1
	if numTopics > 0 {
		resp.Topics = make([]CreateTopicsResponseTopic, numTopics)
		for i := 0; i < numTopics; i++ {
			t := CreateTopicsResponseTopic{}
			t.Name = r.ReadCompactString()
			t.ErrorCode = r.ReadInt16()
			errMsgLenVar, _ := r.ReadVarint()
			errMsgLen := int(errMsgLenVar) - 1
			if errMsgLen >= 0 {
				msg := string(r.Buf[r.Pos : r.Pos+errMsgLen])
				t.ErrorMessage = &msg
				r.Pos += errMsgLen
			}
			t.NumPartitions = r.ReadInt32()
			t.ReplicationFactor = r.ReadInt16()

			numConfVar, _ := r.ReadVarint()
			numConf := int(numConfVar) - 1
			if numConf > 0 {
				t.Configs = make([]CreateTopicsResponseConfig, numConf)
				for j := 0; j < numConf; j++ {
					c := CreateTopicsResponseConfig{}
					c.Name = r.ReadCompactString()
					valLenVar, _ := r.ReadVarint()
					valLen := int(valLenVar) - 1
					if valLen >= 0 {
						val := string(r.Buf[r.Pos : r.Pos+valLen])
						c.Value = &val
						r.Pos += valLen
					}
					c.ReadOnly = r.ReadUint8() == 1
					c.ConfigSource = r.ReadInt8()
					c.IsDefault = r.ReadUint8() == 1
					r.ReadUint8() // Tags
					t.Configs[j] = c
				}
			}
			r.ReadUint8() // Topic Tags
			resp.Topics[i] = t
		}
	}
	r.ReadUint8() // Main Tags
	return resp
}
