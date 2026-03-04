package handlers

import (
	"fmt"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleDescribeTopicPartitions(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	// Parse remainder of body
	req := protocol.DecodeDescribeTopicPartitionsRequest(reader)

	// Fetch metadata from KRaft log (handled per request for simplicity in this stage)
	// Codecrafters provides cluster metadata log here, derived from properties configuring `log.dirs`
	logPath := fmt.Sprintf("%s/__cluster_metadata-0/00000000000000000000.log", config.LogDirs)
	metadataTopics, metadataPartitions, err := metadata.ReadClusterMetadata(logPath)
	if err != nil {
		fmt.Printf("Error reading metadata: %v\n", err)
	} else {
		fmt.Printf("Parsed %d topics and %d partitions from %s\n", len(metadataTopics), len(metadataPartitions), logPath)
		for k, t := range metadataTopics {
			fmt.Printf("Found topic: Name=%s, UUID=%x\n", t.Name, []byte(k))
		}
	}

	fmt.Printf("Received topics in request: %v\n", req.Topics)

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

	// Kafka protocol often expects topics to be sorted algorithmically (typically by Topic Name)
	// when queried. CodeCrafters stage WQ2 explicitly triggers this sorting constraint requirement!
	importSort := false
	if len(topics) > 1 {
		importSort = true
	}
	if importSort {
		// Just a simple bubble sort or we can use sort package if we import it,
		// but since we are modifying without updating imports cleanly, let's just use simple sort inline.
		for i := 0; i < len(topics); i++ {
			for j := i + 1; j < len(topics); j++ {
				if topics[i].Name > topics[j].Name {
					topics[i], topics[j] = topics[j], topics[i]
				}
			}
		}
	}

	return &protocol.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		NextCursor:     -1,
	}, nil
}
