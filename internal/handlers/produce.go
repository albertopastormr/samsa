package handlers

import (
	"fmt"
	"log/slog"
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
		if !topicExists {
			slog.Warn("produce error: topic not found", slog.String("topic", t.Name))
		}

		for j, p := range t.Partitions {
			var errCode int16 = protocol.ErrUnknownTopicOrPartition
			var logStartOffset, baseOffset, logAppendTimeMs int64 = -1, -1, -1

			if topicExists {
				// Validate partition existence
				parts := metadataPartitions[string(topic.TopicId[:])]
				foundPartition := false
				for _, mp := range parts {
					if mp.PartitionId == p.Index {
						foundPartition = true
						// Valid Topic and Partition
						// Write records to disk
						if len(p.Records) > 0 {
							logDir := filepath.Join(config.LogDirs, fmt.Sprintf("%s-%d", t.Name, p.Index))
							if err := os.MkdirAll(logDir, 0755); err != nil {
								slog.Error("error creating log directory", slog.String("dir", logDir), slog.Any("error", err))
								errCode = protocol.ErrUnknownServerError
								break
							}
							logPath := filepath.Join(logDir, config.DefaultLogSegment)
							f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
							if err != nil {
								slog.Error("error opening log file", slog.String("path", logPath), slog.Any("error", err))
								errCode = protocol.ErrUnknownServerError
								break
							}
							if _, err := f.Write(p.Records); err != nil {
								slog.Error("error writing to log file", slog.String("path", logPath), slog.Any("error", err))
								f.Close()
								errCode = protocol.ErrUnknownServerError
								break
							}
							if err := f.Sync(); err != nil {
								slog.Error("error syncing log file", slog.String("path", logPath), slog.Any("error", err))
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
				if !foundPartition {
					slog.Warn("produce error: partition not found", slog.String("topic", t.Name), slog.Int("partition", int(p.Index)))
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
