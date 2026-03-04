package handlers

import (
	"fmt"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleFetch(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	req := protocol.DecodeFetchRequest(reader)

	// Fetch metadata from KRaft log
	logPath := fmt.Sprintf("%s/__cluster_metadata-0/00000000000000000000.log", config.LogDirs)
	metadataTopics, _, err := metadata.ReadClusterMetadata(logPath)
	if err != nil {
		fmt.Printf("Error reading metadata: %v\n", err)
	}

	resp := &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionId:      req.SessionId,
		Topics:         make([]protocol.FetchResponseTopic, len(req.Topics)),
	}

	for i, t := range req.Topics {
		var errCode int16 = protocol.ErrNone
		if _, exists := metadataTopics[string(t.TopicId[:])]; !exists {
			errCode = protocol.ErrUnknownTopicId
		}

		resp.Topics[i] = protocol.FetchResponseTopic{
			TopicId:    t.TopicId,
			Partitions: make([]protocol.FetchResponsePartition, len(t.Partitions)),
		}
		for j, p := range t.Partitions {
			resp.Topics[i].Partitions[j] = protocol.FetchResponsePartition{
				PartitionIndex:   p.Partition,
				ErrorCode:        errCode,
				HighWatermark:    0,
				LastStableOffset: 0,
				LogStartOffset:   0,
				Records:          nil,
			}
		}
	}

	return resp, nil
}
