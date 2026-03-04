package handlers

import (
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleFetch(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	req := protocol.DecodeFetchRequest(reader)

	resp := &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionId:      req.SessionId,
		Topics:         make([]protocol.FetchResponseTopic, len(req.Topics)),
	}

	for i, t := range req.Topics {
		resp.Topics[i] = protocol.FetchResponseTopic{
			TopicId: t.TopicId,
			Partitions: []protocol.FetchResponsePartition{
				{
					PartitionIndex: 0,
					ErrorCode:      protocol.ErrUnknownTopicId,
				},
			},
		}
	}

	return resp, nil
}
