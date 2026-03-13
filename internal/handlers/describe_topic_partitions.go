package handlers

import (
	"fmt"

	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func HandleDescribeTopicPartitions(header protocol.RequestHeader, reader *protocol.Reader) (protocol.Encoder, error) {
	// Parse remainder of body
	req := protocol.DecodeDescribeTopicPartitionsRequest(reader)

	// Fetch metadata from cached store
	metadataTopics := metadata.GetTopics()
	metadataPartitions := metadata.GetPartitions()
	fmt.Printf("Metadata cache: %d topics, %d partition groups\n", len(metadataTopics), len(metadataPartitions))

	requestedTopics := req.Topics
	if len(requestedTopics) == 0 {
		for _, v := range metadataTopics {
			requestedTopics = append(requestedTopics, v.Name)
		}
	}

	topics := make([]protocol.DescribeTopicResponseTopic, len(requestedTopics))
	for i, topicName := range requestedTopics {
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
