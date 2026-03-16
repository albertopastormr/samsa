package metadata

import (
	"log/slog"
	"path/filepath"
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
	
	topicsMap := make(map[string]Topic, len(globalStore.topics))
	for k, v := range globalStore.topics {
		topicsMap[k] = v
	}
	return topicsMap
}

func GetPartitions() map[string][]Partition {
	syncIfNecessary()
	globalStore.mu.RLock()
	defer globalStore.mu.RUnlock()

	partitionsMap := make(map[string][]Partition, len(globalStore.partitions))
	for k, v := range globalStore.partitions {
		pCopy := make([]Partition, len(v))
		copy(pCopy, v)
		partitionsMap[k] = pCopy
	}
	return partitionsMap
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
	hasTopics := len(globalStore.topics) > 0
	globalStore.mu.RUnlock()

	if isFresh && hasTopics {
		return
	}

	globalStore.mu.Lock()
	defer globalStore.mu.Unlock()

	// Double check
	if time.Since(globalStore.lastUpdate) < 5*time.Second && len(globalStore.topics) > 0 {
		return
	}

	logPath := filepath.Join(config.LogDirs, "__cluster_metadata-0", config.DefaultLogSegment)
	topics, partitions, err := ReadClusterMetadata(logPath)
	if err != nil {
		slog.Error("metadata sync error", slog.Any("error", err))
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

func AddTopic(name string, numPartitions int32, topicID [16]byte) {
	globalStore.mu.Lock()
	defer globalStore.mu.Unlock()

	uuid := string(topicID[:])
	globalStore.topics[uuid] = Topic{Name: name, TopicId: topicID}
	globalStore.topicsByName[name] = uuid

	parts := make([]Partition, numPartitions)
	for i := 0; i < int(numPartitions); i++ {
		parts[i] = Partition{
			PartitionId: int32(i),
			TopicId:     topicID,
			Leader:      1,
			LeaderEpoch: 1,
			Replicas:    []int32{1},
			Isr:         []int32{1},
		}
	}
	globalStore.partitions[uuid] = parts
	globalStore.lastUpdate = time.Now()
}
