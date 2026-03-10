package handlers

import (
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleProduce(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	req := protocol.DecodeProduceRequest(reader)

	resp := &protocol.ProduceResponse{
		ThrottleTimeMs: 0,
		Responses:      make([]protocol.ProduceResponseTopic, len(req.Topics)),
	}

	for i, t := range req.Topics {
		resp.Responses[i] = protocol.ProduceResponseTopic{
			Name:       t.Name,
			Partitions: make([]protocol.ProduceResponsePartition, len(t.Partitions)),
		}

		for j, p := range t.Partitions {
			resp.Responses[i].Partitions[j] = protocol.ProduceResponsePartition{
				Index:           p.Index,
				ErrorCode:       protocol.ErrUnknownTopicOrPartition, // Hardcoded for this stage
				BaseOffset:      -1,
				LogAppendTimeMs: -1,
				LogStartOffset:  -1,
				RecordErrors:    nil,
				ErrorMessage:    nil,
			}
		}
		resp.Responses[i].Name = t.Name
	}

	return resp, nil
}
