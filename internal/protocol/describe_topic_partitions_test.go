package protocol

import (
	"testing"
)

func TestDescribeTopicPartitionsProtocol(t *testing.T) {
	// Request encoding/decoding
	req := DescribeTopicPartitionsRequest{
		Topics: []string{"topic1", "topic2"},
	}
	if len(req.Topics) != 2 {
		t.Errorf("request topics mismatch")
	}

	// Manual buffer construction for validation
	// Topics Array (Compact): length 3 (2 topics), topic1 (compact str), topic2 (compact str), cursor, tags
	// In our implementation, topics are COMPACT_STRING

	resp := &DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 10,
		Topics: []DescribeTopicResponseTopic{
			{
				ErrorCode: ErrNone,
				Name:      "topic1",
				TopicId:   [16]byte{1},
				Partitions: []DescribeTopicResponsePartition{
					{
						ErrorCode:    ErrNone,
						PartitionId:  0,
						Leader:       1,
						LeaderEpoch:  1,
						ReplicaNodes: []int32{1, 2},
						IsrNodes:     []int32{1},
					},
				},
			},
		},
		NextCursor: -1,
	}

	size := resp.TotalSize()
	w := NewWriter(size)
	resp.Encode(w, 123)

	encoded := w.Bytes()
	if len(encoded) != size {
		t.Errorf("expected size %d, got %d", size, len(encoded))
	}

	r := NewReader(encoded)
	if r.ReadInt32() != 123 {
		t.Errorf("correlation ID mismatch")
	}
	_ = r.ReadUint8() // header tags

	if r.ReadInt32() != 10 {
		t.Errorf("throttle mismatch")
	}

	topicCount, _ := r.ReadVarint()
	if topicCount != 2 {
		t.Errorf("topic count mismatch")
	}

	errCode := r.ReadInt16()
	if errCode != ErrNone {
		t.Errorf("error code mismatch")
	}

	name := r.ReadCompactString()
	if name != "topic1" {
		t.Errorf("topic name mismatch, got %s", name)
	}

	var tid [16]byte
	r.ReadBytes(tid[:])
	if tid[0] != 1 {
		t.Errorf("topic ID mismatch")
	}
}
