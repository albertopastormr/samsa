package cli

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/albertopastormr/samsa/internal/client"
	"github.com/albertopastormr/samsa/internal/protocol"
	"github.com/spf13/cobra"
)

var (
	topic     string
	topicID   string
	partition int32
	offset    int64
	message   string
	numPartitions int32
)

var apiVersionsCmd = &cobra.Command{
	Use:   "apiversions",
	Short: "Get supported broker API versions",
	Run: func(cmd *cobra.Command, args []string) {
		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.ApiVersions()
		if err != nil {
			slog.Error("apiversions failed", slog.Any("error", err))
			os.Exit(1)
		}
		slog.Info("apiversions response", slog.Int("error_code", int(resp.ErrorCode)))
		for _, entry := range resp.ApiKeys {
			slog.Info("supported api", 
				slog.Int("api_key", int(entry.ApiKey)), 
				slog.Int("min_version", int(entry.MinVersion)), 
				slog.Int("max_version", int(entry.MaxVersion)))
		}
	},
}

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Manage Kafka topics",
}

var topicDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Fetch topic and partition metadata",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" {
			slog.Warn("name is required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.DescribeTopicPartitions([]string{topic})
		if err != nil {
			slog.Error("metadata failed", slog.Any("error", err))
			os.Exit(1)
		}
		for _, t := range resp.Topics {
			slog.Info("topic metadata", 
				slog.String("name", t.Name), 
				slog.String("id", hex.EncodeToString(t.TopicId[:])), 
				slog.Int("error_code", int(t.ErrorCode)))
			for _, p := range t.Partitions {
				slog.Info("partition metadata", 
					slog.Int("partition_id", int(p.PartitionId)), 
					slog.Int("leader", int(p.Leader)), 
					slog.Int("error_code", int(p.ErrorCode)))
			}
		}
	},
}

var topicCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new Kafka topic",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" {
			slog.Warn("name is required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.CreateTopic(topic, numPartitions)
		if err != nil {
			slog.Error("topic creation failed", slog.Any("error", err))
			os.Exit(1)
		}

		for _, t := range resp.Topics {
			if t.ErrorCode == 0 {
				slog.Info("topic created successfully", 
					slog.String("name", t.Name), 
					slog.Int("partitions", int(t.NumPartitions)))
			} else {
				errMsg := "unknown error"
				if t.ErrorMessage != nil {
					errMsg = *t.ErrorMessage
				}
				slog.Error("failed to create topic", 
					slog.String("name", t.Name), 
					slog.Int("error_code", int(t.ErrorCode)),
					slog.String("error_message", errMsg))
			}
		}
	},
}

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce a message to a topic",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" || message == "" {
			slog.Warn("topic and message are required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.Produce(topic, partition, []byte(message))
		if err != nil {
			slog.Error("produce failed", slog.Any("error", err))
			os.Exit(1)
		}
		for _, r := range resp.Responses {
			slog.Info("produce topic response", slog.String("topic", r.Name))
			for _, p := range r.Partitions {
				slog.Info("produce partition response", 
					slog.Int("partition", int(p.Index)), 
					slog.Int("error_code", int(p.ErrorCode)), 
					slog.Int64("offset", p.BaseOffset))
			}
		}
	},
}

var fetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch records from a topic partition",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" {
			slog.Warn("topic is required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		// 1. Resolve Topic Name to Topic ID
		meta, err := kc.DescribeTopicPartitions([]string{topic})
		if err != nil {
			slog.Error("failed to resolve topic metadata", slog.String("topic", topic), slog.Any("error", err))
			os.Exit(1)
		}

		var tid [16]byte
		found := false
		for _, t := range meta.Topics {
			if t.Name == topic && t.ErrorCode == 0 {
				tid = t.TopicId
				found = true
				break
			}
		}

		if !found {
			slog.Error("topic not found or error in metadata", slog.String("topic", topic))
			os.Exit(1)
		}

		// 2. Perform Fetch
		resp, err := kc.Fetch(tid, partition, offset)
		if err != nil {
			slog.Error("fetch failed", slog.Any("error", err))
			os.Exit(1)
		}

		slog.Info("fetch completed", slog.Int("error_code", int(resp.ErrorCode)))
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if p.ErrorCode != 0 {
					slog.Warn("partition error", slog.Int("partition", int(p.PartitionIndex)), slog.Int("error_code", int(p.ErrorCode)))
					continue
				}
				if len(p.Records) > 0 {
					records, err := protocol.DecodeRecordBatch(p.Records)
					if err != nil {
						slog.Error("failed to decode record batch", slog.Any("error", err))
						continue
					}
					for _, r := range records {
						fmt.Println(r.String())
					}
				}
			}
		}
	},
}

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from a topic in real-time",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" {
			slog.Warn("topic is required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		// 1. Resolve Topic Name to Topic ID
		meta, err := kc.DescribeTopicPartitions([]string{topic})
		if err != nil {
			slog.Error("failed to resolve topic metadata", slog.String("topic", topic), slog.Any("error", err))
			os.Exit(1)
		}
		var tid [16]byte
		found := false
		for _, t := range meta.Topics {
			if t.Name == topic && t.ErrorCode == 0 {
				tid = t.TopicId
				found = true
				break
			}
		}
		if !found {
			slog.Error("topic not found or error in metadata", slog.String("topic", topic))
			os.Exit(1)
		}

		slog.Info("starting consumer", slog.String("topic", topic), slog.String("topic_id", hex.EncodeToString(tid[:])), slog.Int64("offset", offset))

		// 2. Setup Signal Handling for Graceful Exit
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		currentOffset := offset
		
		for {
			select {
			case <-sigChan:
				slog.Info("stopping consumer...")
				return
			default:
				resp, err := kc.Fetch(tid, partition, currentOffset)
				if err != nil {
					slog.Error("fetch error", slog.Any("error", err))
					time.Sleep(1 * time.Second)
					continue
				}

				if resp.ErrorCode != 0 {
					slog.Warn("fetch returned error", slog.Int("error_code", int(resp.ErrorCode)))
					time.Sleep(1 * time.Second)
					continue
				}

				foundNewMessages := false
				for _, t := range resp.Topics {
					for _, p := range t.Partitions {
						if len(p.Records) > 0 {
							records, err := protocol.DecodeRecordBatch(p.Records)
							if err != nil {
								slog.Error("failed to decode record batch", slog.Any("error", err))
								continue
							}
							if len(records) > 0 {
								foundNewMessages = true
								for _, r := range records {
									fmt.Println(r.String())
									if r.Offset >= currentOffset {
										currentOffset = r.Offset + 1
									}
								}
							}
						}
					}
				}

				if !foundNewMessages {
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	},
}

var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all topics",
	Run: func(cmd *cobra.Command, args []string) {
		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.DescribeTopicPartitions(nil) // Empty list means "all" in our refactored handler
		if err != nil {
			slog.Error("topic listing failed", slog.Any("error", err))
			os.Exit(1)
		}

		for _, t := range resp.Topics {
			if t.ErrorCode == 0 {
				slog.Info("topic", 
					slog.String("name", t.Name), 
					slog.String("id", hex.EncodeToString(t.TopicId[:])), 
					slog.Int("partitions", len(t.Partitions)))
			}
		}
	},
}

func init() {
	topicDescribeCmd.Flags().StringVar(&topic, "name", "", "Topic name")
	
	topicCreateCmd.Flags().StringVar(&topic, "name", "", "Topic name")
	topicCreateCmd.Flags().Int32Var(&numPartitions, "partitions", 1, "Number of partitions")

	produceCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	produceCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	produceCmd.Flags().StringVar(&message, "message", "", "Message content")

	fetchCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	fetchCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	fetchCmd.Flags().Int64Var(&offset, "offset", 0, "Fetch offset")

	consumeCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	consumeCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	consumeCmd.Flags().Int64Var(&offset, "offset", 0, "Initial offset")

	rootCmd.AddCommand(apiVersionsCmd)
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(fetchCmd)
	rootCmd.AddCommand(consumeCmd)

	topicCmd.AddCommand(topicListCmd)
	topicCmd.AddCommand(topicDescribeCmd)
	topicCmd.AddCommand(topicCreateCmd)
	rootCmd.AddCommand(topicCmd)
}
