package metadata

import (
	"fmt"
	"sync"
	"time"

	"github.com/albertopastormr/samsa/internal/config"
)

type Store struct {
	mu           sync.RWMutex
	topics       map[string]Topic
	topicsByName map[string]string // Maps Topic Name to UUID
	partitions   map[string][]Partition
	lastUpdate   time.Time
}

var globalStore = &Store{
	topics:       make(map[string]Topic),
	topicsByName: make(map[string]string),
	partitions:   make(map[string][]Partition),
}

func GetTopics() map[string]Topic {
	syncIfNecessary()
	globalStore.mu.RLock()
	defer globalStore.mu.RUnlock()
	return globalStore.topics
}

func GetPartitions() map[string][]Partition {
	syncIfNecessary()
	globalStore.mu.RLock()
	defer globalStore.mu.RUnlock()
	return globalStore.partitions
}

func GetTopicByName(name string) (Topic, bool) {
	syncIfNecessary()
	globalStore.mu.RLock()
	defer globalStore.mu.RUnlock()

	uuid, ok := globalStore.topicsByName[name]
	if !ok {
		return Topic{}, false
	}
	topic, ok := globalStore.topics[uuid]
	return topic, ok
}

func syncIfNecessary() {
	globalStore.mu.RLock()
	isFresh := time.Since(globalStore.lastUpdate) < 5*time.Second
	globalStore.mu.RUnlock()

	if isFresh && len(globalStore.topics) > 0 {
		return
	}

	globalStore.mu.Lock()
	defer globalStore.mu.Unlock()

	// Double check
	if time.Since(globalStore.lastUpdate) < 5*time.Second && len(globalStore.topics) > 0 {
		return
	}

	logPath := fmt.Sprintf("%s/__cluster_metadata-0/00000000000000000000.log", config.LogDirs)
	topics, partitions, err := ReadClusterMetadata(logPath)
	if err != nil {
		fmt.Printf("metadata sync error: %v\n", err)
		return
	}

	globalStore.topics = topics
	globalStore.partitions = partitions
	globalStore.topicsByName = make(map[string]string)
	for uuid, topic := range topics {
		globalStore.topicsByName[topic.Name] = uuid
	}
	globalStore.lastUpdate = time.Now()
}

func SetMetadataForTest(topics map[string]Topic, partitions map[string][]Partition) {
	globalStore.mu.Lock()
	defer globalStore.mu.Unlock()
	globalStore.topics = topics
	globalStore.partitions = partitions
	globalStore.topicsByName = make(map[string]string)
	for uuid, topic := range topics {
		globalStore.topicsByName[topic.Name] = uuid
	}
	globalStore.lastUpdate = time.Now()
}
