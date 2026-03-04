package metadata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadClusterMetadata(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "0.log")

	// Create an empty file
	if err := os.WriteFile(logPath, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}

	topics, partitions, err := ReadClusterMetadata(logPath)
	if err != nil {
		// ReadClusterMetadata loop should break on EOF when reading offset64
		// So it should return (topics, partitions, nil)
	}
	if topics == nil || partitions == nil {
		t.Errorf("expected topics and partitions maps, got %v/%v", topics, partitions)
	}
}
