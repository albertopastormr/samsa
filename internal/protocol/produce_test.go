package protocol

import (
	"reflect"
	"testing"
)

func TestDecodeProduceRequest(t *testing.T) {
	// A basic ProduceRequest v11
	// TransactionalId: Null (Length 0)
	// Acks: 1 (0x0001)
	// TimeoutMs: 1000 (0x000003E8)
	// Topics: 1 (Compact Array Length 2)
	//   Name: "test" (Length 5)
	//   Partitions: 1 (Compact Array Length 2)
	//     Index: 0
	//     Records: "abcd" (Length 5 -> 4 bytes)
	//     TagBuffer: 0
	//   Topic TagBuffer: 0
	// TagBuffer: 0

	buf := []byte{
		0x00,       // TransactionalId Length (0 means Null)
		0x00, 0x01, // Acks: 1
		0x00, 0x00, 0x03, 0xE8, // TimeoutMs: 1000
		0x02,                     // Topics Length (1 + 1)
		0x05, 't', 'e', 's', 't', // Topic Name: "test"
		0x02,                   // Partitions Length (1 + 1)
		0x00, 0x00, 0x00, 0x00, // Index: 0
		0x05, 'a', 'b', 'c', 'd', // Records: "abcd" (Len 4+1=5)
		0x00, // Partition TagBuffer
		0x00, // Topic TagBuffer
		0x00, // Global TagBuffer
	}

	r := NewReader(buf)
	req := DecodeProduceRequest(r)

	if req.TransactionalId != nil {
		t.Errorf("expected null TransactionalId, got %v", *req.TransactionalId)
	}
	if req.Acks != 1 {
		t.Errorf("expected Acks 1, got %d", req.Acks)
	}
	if req.TimeoutMs != 1000 {
		t.Errorf("expected TimeoutMs 1000, got %d", req.TimeoutMs)
	}
	if len(req.Topics) != 1 {
		t.Errorf("expected 1 topic, got %d", len(req.Topics))
		return
	}
	if req.Topics[0].Name != "test" {
		t.Errorf("expected topic name 'test', got '%s'", req.Topics[0].Name)
	}
	if len(req.Topics[0].Partitions) != 1 {
		t.Errorf("expected 1 partition, got %d", len(req.Topics[0].Partitions))
		return
	}
	if req.Topics[0].Partitions[0].Index != 0 {
		t.Errorf("expected partition index 0, got %d", req.Topics[0].Partitions[0].Index)
	}
	expectedRecords := []byte{'a', 'b', 'c', 'd'}
	if !reflect.DeepEqual(req.Topics[0].Partitions[0].Records, expectedRecords) {
		t.Errorf("expected records %v, got %v", expectedRecords, req.Topics[0].Partitions[0].Records)
	}
}

func TestProduceResponseEncode(t *testing.T) {
	resp := &ProduceResponse{
		ThrottleTimeMs: 42,
		Responses: []ProduceResponseTopic{
			{
				Name: "foo",
				Partitions: []ProduceResponsePartition{
					{
						Index:           1,
						ErrorCode:       3,
						BaseOffset:      -1,
						LogAppendTimeMs: -1,
						LogStartOffset:  -1,
						RecordErrors:    nil,
						ErrorMessage:    nil,
					},
				},
			},
		},
	}

	size := resp.TotalSize()
	w := NewWriter(size)
	resp.Encode(w, 99) // CorrelationID

	encoded := w.Bytes()
	if len(encoded) != size {
		t.Errorf("expected size %d, got %d", size, len(encoded))
	}

	r := NewReader(encoded)
	if r.ReadInt32() != 99 {
		t.Errorf("expected correlation ID 99")
	}
	_ = r.ReadUint8() // Response Header Tag Buffer

	topicCount, _ := r.ReadVarint()
	if topicCount != 2 { // 1 element + 1
		t.Errorf("expected topic count 2, got %d", topicCount)
	}

	topicName := r.ReadCompactString()
	if topicName != "foo" {
		t.Errorf("expected topic name 'foo', got '%s'", topicName)
	}

	partCount, _ := r.ReadVarint()
	if partCount != 2 { // 1 element + 1
		t.Errorf("expected part count 2, got %d", partCount)
	}

	if r.ReadInt32() != 1 {
		t.Errorf("expected partition index 1")
	}
	if r.ReadInt16() != 3 {
		t.Errorf("expected error code 3")
	}
}
