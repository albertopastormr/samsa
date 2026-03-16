package handlers

import (
	"fmt"
	"os"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleFetch(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	req := protocol.DecodeFetchRequest(reader)

	// Fetch metadata from cached store
	metadataTopics := metadata.GetTopics()

	resp := &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionId:      req.SessionId,
		Topics:         make([]protocol.FetchResponseTopic, len(req.Topics)),
	}

	for i, t := range req.Topics {
		var errCode int16 = protocol.ErrNone
		topicMetadata, exists := metadataTopics[string(t.TopicId[:])]
		if !exists {
			errCode = protocol.ErrUnknownTopicId
		}

		resp.Topics[i] = protocol.FetchResponseTopic{
			TopicId:    t.TopicId,
			Partitions: make([]protocol.FetchResponsePartition, len(t.Partitions)),
		}
		for j, p := range t.Partitions {
			var records []byte
			if exists {
				records = func() []byte {
					logPath := fmt.Sprintf("%s/%s-%d/00000000000000000000.log", config.LogDirs, topicMetadata.Name, p.Partition)
					f, err := os.Open(logPath)
					if err != nil {
						return nil
					}
					defer f.Close()
					
					readSize := p.PartitionMaxBytes
					if readSize <= 0 {
						readSize = 1024 * 1024 // 1MB default
					}
					
					buf := make([]byte, readSize)
					n, _ := f.ReadAt(buf, p.FetchOffset)
					if n > 0 {
						return buf[:n]
					}
					return nil
				}()
			}

			resp.Topics[i].Partitions[j] = protocol.FetchResponsePartition{
				PartitionIndex:       p.Partition,
				ErrorCode:            errCode,
				HighWatermark:        0,
				LastStableOffset:     0,
				LogStartOffset:       0,
				PreferredReadReplica: -1,
				Records:              records,
			}
		}
	}

	return resp, nil
}
