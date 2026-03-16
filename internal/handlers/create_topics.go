package handlers

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleCreateTopics(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	req := protocol.DecodeCreateTopicsRequest(reader)
	resp := &protocol.CreateTopicsResponse{
		ThrottleTimeMs: 0,
		Topics:         make([]protocol.CreateTopicsResponseTopic, len(req.Topics)),
	}

	for i, t := range req.Topics {
		var errCode int16 = protocol.ErrNone
		var errMsg *string

		// Check if topic already exists
		if _, exists := metadata.GetTopicByName(t.Name); exists {
			errCode = protocol.ErrTopicAlreadyExists
			msg := "Topic already exists"
			errMsg = &msg
		} else {
			// Generate Topic ID
			var topicID [16]byte
			rand.Read(topicID[:])

			// Create log directories
			for p := 0; p < int(t.NumPartitions); p++ {
				logDir := filepath.Join(config.LogDirs, fmt.Sprintf("%s-%d", t.Name, p))
				if err := os.MkdirAll(logDir, 0755); err != nil {
					slog.Error("failed to create log directory", slog.String("dir", logDir), slog.Any("error", err))
					errCode = protocol.ErrUnknownServerError
					break
				}
			}

			if errCode == protocol.ErrNone {
				metadata.AddTopic(t.Name, t.NumPartitions, topicID)
				slog.Info("topic created", slog.String("name", t.Name), slog.Int("partitions", int(t.NumPartitions)))
			}
		}

		resp.Topics[i] = protocol.CreateTopicsResponseTopic{
			Name:              t.Name,
			ErrorCode:         errCode,
			ErrorMessage:      errMsg,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		}
	}

	return resp, nil
}
