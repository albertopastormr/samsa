package handlers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/metadata"
	"github.com/albertopastormr/samsa/internal/protocol"
)

func TestProduceIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka-integration")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	config.LogDirs = tmpDir
	
	topicName := "integ-topic"
	topicID := [16]byte{0xDE, 0xAD, 0xBE, 0xEF, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}
	
	// Manually inject into store to simulate a "synced" state
	metadata.SetMetadataForTest(
		map[string]metadata.Topic{
			string(topicID[:]): {Name: topicName, TopicId: topicID},
		},
		map[string][]metadata.Partition{
			string(topicID[:]): {
				{PartitionId: 0, TopicId: topicID},
			},
		},
	)
	
	records := []byte("integration test data")
	w := protocol.NewWriter(1024)
	w.WriteUint8(0)                  // TransactionalId (Null)
	w.WriteInt16(1)                  // Acks
	w.WriteInt32(1000)               // TimeoutMs
	w.WriteVarint(2)                 // Topics count
	w.WriteCompactString(topicName)  // Topic Name
	w.WriteVarint(2)                 // Partitions count
	w.WriteInt32(0)                  // Partition Index
	w.WriteVarint(uint64(len(records) + 1))
	w.WriteBytes(records)
	w.WriteUint8(0) // Partition Tag Buffer
	w.WriteUint8(0) // Topic Tag Buffer
	w.WriteUint8(0) // Global Tag Buffer
	
	header := protocol.RequestHeader{CorrelationID: 999}
	reader := protocol.NewReader(w.Bytes())
	
	resp, err := HandleProduce(header, reader)
	if err != nil {
		t.Fatalf("HandleProduce failed: %v", err)
	}
	
	pResp := resp.(*protocol.ProduceResponse)
	if len(pResp.Responses) == 0 || len(pResp.Responses[0].Partitions) == 0 {
		t.Fatalf("unexpected empty response")
	}
	if pResp.Responses[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Errorf("expected 0, got %d", pResp.Responses[0].Partitions[0].ErrorCode)
	}
	
	// Check disk
	logPath := filepath.Join(tmpDir, "integ-topic-0", "00000000000000000000.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Fatalf("log file was not created at %s", logPath)
	}
	
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	if string(content) != string(records) {
		t.Errorf("expected %s, got %s", string(records), string(content))
	}
}
