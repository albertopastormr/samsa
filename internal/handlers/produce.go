package handlers

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleProduce(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	req := protocol.DecodeProduceRequest(reader)

	resp := &protocol.ProduceResponse{
		ThrottleTimeMs: 0,
		Responses:      make([]protocol.ProduceResponseTopic, len(req.Topics)),
	}

	// Fetch all partitions
	metadataPartitions := metadata.GetPartitions()

	for i, t := range req.Topics {
		resp.Responses[i] = protocol.ProduceResponseTopic{
			Name:       t.Name,
			Partitions: make([]protocol.ProduceResponsePartition, len(t.Partitions)),
		}

		// Validate topic existence
		topic, topicExists := metadata.GetTopicByName(t.Name)

		for j, p := range t.Partitions {
			var errCode int16 = protocol.ErrUnknownTopicOrPartition
			var logStartOffset, baseOffset, logAppendTimeMs int64 = -1, -1, -1

			if topicExists {
				// Validate partition existence
				parts := metadataPartitions[string(topic.TopicId[:])]
				for _, mp := range parts {
					if mp.PartitionId == p.Index {
						// Valid Topic and Partition
						// Write records to disk
						if len(p.Records) > 0 {
							logDir := filepath.Join(config.LogDirs, fmt.Sprintf("%s-%d", t.Name, p.Index))
							if err := os.MkdirAll(logDir, 0755); err != nil {
								fmt.Printf("Error creating log directory: %v\n", err)
								errCode = protocol.ErrUnknownServerError
								break
							}
							logPath := filepath.Join(logDir, "00000000000000000000.log")
							f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
							if err != nil {
								fmt.Printf("Error opening log file: %v\n", err)
								errCode = protocol.ErrUnknownServerError
								break
							}
							if _, err := f.Write(p.Records); err != nil {
								fmt.Printf("Error writing to log file: %v\n", err)
								f.Close()
								errCode = protocol.ErrUnknownServerError
								break
							}
							f.Close()
						}

						errCode = protocol.ErrNone
						logStartOffset = 0
						baseOffset = 0
						logAppendTimeMs = -1
						break
					}
				}
			}

			resp.Responses[i].Partitions[j] = protocol.ProduceResponsePartition{
				Index:           p.Index,
				ErrorCode:       errCode,
				BaseOffset:      baseOffset,
				LogAppendTimeMs: logAppendTimeMs,
				LogStartOffset:  logStartOffset,
				RecordErrors:    nil,
				ErrorMessage:    nil,
			}
		}
	}

	return resp, nil
}
