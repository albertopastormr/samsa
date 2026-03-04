package handlers

import (
	"fmt"

	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleDescribeTopicPartitions(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	// Parse remainder of body
	req := protocol.DecodeDescribeTopicPartitionsRequest(reader)

	// Fetch metadata from KRaft log (handled per request for simplicity in this stage)
	// Codecrafters provides cluster metadata log here
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	metadataTopics, metadataPartitions, err := metadata.ReadClusterMetadata(logPath)
	if err != nil {
		fmt.Printf("Error reading metadata: %v\n", err)
		// Usually we still respond, just with unknown topics if we failed to read the log
	}

	topics := make([]protocol.DescribeTopicResponseTopic, len(req.Topics))
	for i, topicName := range req.Topics {
		var foundTopic *metadata.Topic
		// reverse lookup topic by name since ReadClusterMetadata maps UUID -> Topic
		for _, v := range metadataTopics {
			if v.Name == topicName {
				t := v
				foundTopic = &t
				break
			}
		}

		if foundTopic != nil {
			var partReps []protocol.DescribeTopicResponsePartition
			rawParts := metadataPartitions[string(foundTopic.TopicId[:])]

			for _, rp := range rawParts {
				partReps = append(partReps, protocol.DescribeTopicResponsePartition{
					ErrorCode:    protocol.ErrNone,
					PartitionId:  rp.PartitionId,
					Leader:       rp.Leader,
					LeaderEpoch:  rp.LeaderEpoch,
					ReplicaNodes: rp.Replicas,
					IsrNodes:     rp.Isr,
				})
			}

			topics[i] = protocol.DescribeTopicResponseTopic{
				ErrorCode:                 protocol.ErrNone,
				Name:                      topicName,
				TopicId:                   foundTopic.TopicId,
				IsInternal:                false,
				Partitions:                partReps,
				TopicAuthorizedOperations: 0,
			}
		} else {
			topics[i] = protocol.DescribeTopicResponseTopic{
				ErrorCode:                 protocol.ErrUnknownTopicOrPartition,
				Name:                      topicName,
				TopicId:                   [16]byte{},
				IsInternal:                false,
				Partitions:                nil,
				TopicAuthorizedOperations: 0,
			}
		}
	}

	return &protocol.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		NextCursor:     -1,
	}, nil
}
