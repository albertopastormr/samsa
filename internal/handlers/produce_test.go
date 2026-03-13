package handlers

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func setupTestMetadata() ([16]byte, string) {
	topicName := "test-topic"
	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	topics := map[string]metadata.Topic{
		string(topicID[:]): {Name: topicName, TopicId: topicID},
	}
	partitions := map[string][]metadata.Partition{
		string(topicID[:]): {
			{PartitionId: 0, TopicId: topicID},
			{PartitionId: 1, TopicId: topicID},
		},
	}
	metadata.SetMetadataForTest(topics, partitions)
	return topicID, topicName
}

func createProduceRequest(topicName string, partitionIndex int32, records []byte) []byte {
	w := protocol.NewWriter(1024)
	w.WriteUint8(0)                  // TransactionalId (Null)
	w.WriteInt16(1)                  // Acks
	w.WriteInt32(1000)               // TimeoutMs
	w.WriteVarint(2)                 // Topics count
	w.WriteCompactString(topicName)  // Topic Name
	w.WriteVarint(2)                 // Partitions count
	w.WriteInt32(partitionIndex)     // Partition Index
	w.WriteVarint(uint64(len(records) + 1))
	w.WriteBytes(records)
	w.WriteUint8(0) // Partition Tag Buffer
	w.WriteUint8(0) // Topic Tag Buffer
	w.WriteUint8(0) // Global Tag Buffer
	return w.Bytes()
}

func TestHandleProduce_Success(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "kafka-produce-success")
	defer os.RemoveAll(tmpDir)
	config.LogDirs = tmpDir
	_, topicName := setupTestMetadata()

	records := []byte("hello success")
	reader := protocol.NewReader(createProduceRequest(topicName, 0, records))
	header := protocol.RequestHeader{CorrelationID: 123}

	resp, _ := HandleProduce(header, reader)
	pResp := resp.(*protocol.ProduceResponse)

	if pResp.Responses[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Errorf("expected ErrNone, got %d", pResp.Responses[0].Partitions[0].ErrorCode)
	}

	logPath := filepath.Join(tmpDir, fmt.Sprintf("%s-0", topicName), "00000000000000000000.log")
	content, _ := os.ReadFile(logPath)
	if !bytes.Equal(content, records) {
		t.Errorf("expected %s, got %s", string(records), string(content))
	}
}

func TestHandleProduce_InvalidTopic(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "kafka-produce-invalid-topic")
	defer os.RemoveAll(tmpDir)
	config.LogDirs = tmpDir
	setupTestMetadata()

	reader := protocol.NewReader(createProduceRequest("non-existent", 0, []byte("data")))
	header := protocol.RequestHeader{CorrelationID: 456}

	resp, _ := HandleProduce(header, reader)
	pResp := resp.(*protocol.ProduceResponse)

	if pResp.Responses[0].Partitions[0].ErrorCode != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("expected ErrUnknownTopicOrPartition, got %d", pResp.Responses[0].Partitions[0].ErrorCode)
	}
}

func TestHandleProduce_InvalidPartition(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "kafka-produce-invalid-partition")
	defer os.RemoveAll(tmpDir)
	config.LogDirs = tmpDir
	_, topicName := setupTestMetadata()

	reader := protocol.NewReader(createProduceRequest(topicName, 99, []byte("data")))
	header := protocol.RequestHeader{CorrelationID: 789}

	resp, _ := HandleProduce(header, reader)
	pResp := resp.(*protocol.ProduceResponse)

	if pResp.Responses[0].Partitions[0].ErrorCode != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("expected ErrUnknownTopicOrPartition, got %d", pResp.Responses[0].Partitions[0].ErrorCode)
	}
}

func TestHandleProduce_MultipleItems(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "kafka-produce-multiple")
	defer os.RemoveAll(tmpDir)
	config.LogDirs = tmpDir
	_, topicName := setupTestMetadata()

	w := protocol.NewWriter(1024)
	w.WriteUint8(0)                 // TransactionalId
	w.WriteInt16(1)                 // Acks
	w.WriteInt32(1000)              // TimeoutMs
	w.WriteVarint(2)                // Topics count (1 topic)
	w.WriteCompactString(topicName) // Topic Name
	w.WriteVarint(3)                // Partitions count (2 partitions)
	
	// Part 0
	w.WriteInt32(0)
	w.WriteVarint(uint64(len("rec0") + 1))
	w.WriteBytes([]byte("rec0"))
	w.WriteUint8(0)
	
	// Part 1
	w.WriteInt32(1)
	w.WriteVarint(uint64(len("rec1") + 1))
	w.WriteBytes([]byte("rec1"))
	w.WriteUint8(0)

	w.WriteUint8(0) // Topic Tag
	w.WriteUint8(0) // Global Tag

	reader := protocol.NewReader(w.Bytes())
	header := protocol.RequestHeader{CorrelationID: 101}

	resp, _ := HandleProduce(header, reader)
	pResp := resp.(*protocol.ProduceResponse)

	if len(pResp.Responses[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(pResp.Responses[0].Partitions))
	}

	for i := 0; i < 2; i++ {
		if pResp.Responses[0].Partitions[i].ErrorCode != protocol.ErrNone {
			t.Errorf("partition %d: expected ErrNone, got %d", i, pResp.Responses[0].Partitions[i].ErrorCode)
		}
		logPath := filepath.Join(tmpDir, fmt.Sprintf("%s-%d", topicName, i), "00000000000000000000.log")
		content, _ := os.ReadFile(logPath)
		expected := fmt.Sprintf("rec%d", i)
		if string(content) != expected {
			t.Errorf("partition %d: expected %s, got %s", i, expected, string(content))
		}
	}
}
