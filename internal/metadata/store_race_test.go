package metadata

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestMetadataRace(t *testing.T) {
	// Setup initial metadata
	topicID := [16]byte{}
	copy(topicID[:], "test-topic-id")
	AddTopic("race-topic", 1, topicID)

	start := make(chan struct{})
	var wg sync.WaitGroup

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				_ = GetTopics()
				_, _ = GetTopicByName("race-topic")
				_ = GetPartitions()
			}
		}()
	}

	// Writer goroutines (simulating sync or dynamic topic creation)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				var tid [16]byte
				rand.Read(tid[:])
				AddTopic(fmt.Sprintf("topic-%d-%d", id, j), 1, tid)
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()
}
