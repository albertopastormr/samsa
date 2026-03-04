package protocol

import (
	"reflect"
	"testing"
)

func TestDecodeFetchRequest(t *testing.T) {
	// A more complete Fetch Request (v16)
	// MaxWaitMs, MinBytes, MaxBytes, IsolationLevel, SessionId, SessionEpoch...
	// Topics: 1 (Compact Array Length 2)
	//   TopicId: UUID
	//   Partitions: 1 (Compact Array Length 2)
	//     PartitionIndex: 0
	//     CurrentLeaderEpoch: 0
	//     FetchOffset: 0
	//     LastFetchedEpoch: 0
	//     LogStartOffset: 0
	//     PartitionMaxBytes: 1024
	//     TagBuffer: 0
	//   Topic TagBuffer: 0
	// ForgottenTopics: 0
	// RackId: ""
	// Final TagBuffer: 0
	buf := []byte{
		0x00, 0x00, 0x01, 0xF4, // MaxWaitMs: 500
		0x00, 0x00, 0x00, 0x01, // MinBytes: 1
		0x00, 0x00, 0x04, 0x00, // MaxBytes: 1024
		0x00,                   // IsolationLevel: 0
		0x00, 0x00, 0x00, 0x00, // SessionId: 0
		0xFF, 0xFF, 0xFF, 0xFF, // SessionEpoch: -1
		0x02,                                                                                           // Topics Count (1+1)
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // TopicId
		0x02,                   // Partitions Count (1+1)
		0x00, 0x00, 0x00, 0x00, // Partition: 0
		0x00, 0x00, 0x00, 0x00, // CurrentLeaderEpoch: 0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // FetchOffset: 0
		0x00, 0x00, 0x00, 0x00, // LastFetchedEpoch: 0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // LogStartOffset: 0
		0x00, 0x00, 0x04, 0x00, // PartitionMaxBytes: 1024
		0x00, // Partition TagBuffer
		0x00, // Topic TagBuffer
		0x01, // ForgottenTopics Count (0+1)
		0x01, // RackId length (0+1)
		0x00, // Final TagBuffer
	}

	r := NewReader(buf)
	req := DecodeFetchRequest(r)

	if len(req.Topics) != 1 {
		t.Errorf("expected 1 topic, got %d", len(req.Topics))
	} else {
		if len(req.Topics[0].Partitions) != 1 {
			t.Errorf("expected 1 partition, got %d", len(req.Topics[0].Partitions))
		}
	}
}

func TestFetchResponseEncode(t *testing.T) {
	resp := &FetchResponse{
		ThrottleTimeMs: 100,
		ErrorCode:      0,
		SessionId:      123,
		Topics: []FetchResponseTopic{
			{
				TopicId: [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10},
				Partitions: []FetchResponsePartition{
					{
						PartitionIndex:       0,
						ErrorCode:            100,
						HighWatermark:        1000,
						LastStableOffset:     900,
						LogStartOffset:       500,
						PreferredReadReplica: -1,
						Records:              []byte{0xDE, 0xAD, 0xBE, 0xEF},
					},
				},
			},
		},
	}

	w := NewWriter(resp.TotalSize())
	resp.Encode(w, 42) // CorrelationID 42

	// Verify size matches
	encoded := w.Bytes()
	if len(encoded) != resp.TotalSize() {
		t.Errorf("encoded length %d does not match TotalSize %d", len(encoded), resp.TotalSize())
	}

	r := NewReader(encoded)
	corrId := r.ReadInt32()
	if corrId != 42 {
		t.Errorf("expected CorrelationID 42, got %d", corrId)
	}
	_ = r.ReadUint8() // tag buffer

	throttle := r.ReadInt32()
	if throttle != 100 {
		t.Errorf("expected throttle 100, got %d", throttle)
	}

	errCode := r.ReadInt16()
	if errCode != 0 {
		t.Errorf("expected error code 0, got %d", errCode)
	}

	sessId := r.ReadInt32()
	if sessId != 123 {
		t.Errorf("expected session id 123, got %d", sessId)
	}

	topicCount, _ := r.ReadVarint()
	if topicCount != 2 {
		t.Errorf("expected topic count 2 (1 topic), got %d", topicCount)
	}

	var topicId [16]byte
	r.ReadBytes(topicId[:])
	if !reflect.DeepEqual(topicId, resp.Topics[0].TopicId) {
		t.Errorf("topic ID mismatch")
	}

	partCount, _ := r.ReadVarint()
	if partCount != 2 {
		t.Errorf("expected partition count 2 (1 partition), got %d", partCount)
	}

	partIndex := r.ReadInt32()
	if partIndex != 0 {
		t.Errorf("expected partition index 0, got %d", partIndex)
	}

	partErr := r.ReadInt16()
	if partErr != 100 {
		t.Errorf("expected partition error 100, got %d", partErr)
	}
}
