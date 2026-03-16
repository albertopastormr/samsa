package cli

import (
	"encoding/hex"
	"log/slog"
	"os"

	"github.com/albertopastormr/samsa/internal/client"
	"github.com/spf13/cobra"
)

var (
	topic     string
	topicID   string
	partition int32
	offset    int64
	message   string
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
		if topicID == "" {
			slog.Warn("topic-id is required")
			cmd.Usage()
			os.Exit(1)
		}

		tidBytes, err := hex.DecodeString(topicID)
		if err != nil || len(tidBytes) != 16 {
			slog.Error("invalid topic ID", 
				slog.String("topic_id", topicID), 
				slog.Int("len", len(topicID)), 
				slog.Any("error", err))
			os.Exit(1)
		}
		var tid [16]byte
		copy(tid[:], tidBytes)

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			slog.Error("failed to connect", slog.Any("error", err))
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.Fetch(tid, partition, offset)
		if err != nil {
			slog.Error("fetch failed", slog.Any("error", err))
			os.Exit(1)
		}

		slog.Info("fetch response", slog.Int("error_code", int(resp.ErrorCode)))
		for _, t := range resp.Topics {
			slog.Info("fetch topic", slog.String("topic_id", hex.EncodeToString(t.TopicId[:])))
			for _, p := range t.Partitions {
				slog.Info("fetch partition", 
					slog.Int("partition", int(p.PartitionIndex)), 
					slog.Int("error_code", int(p.ErrorCode)), 
					slog.Int64("high_watermark", p.HighWatermark))
				if len(p.Records) > 0 {
					slog.Info("fetched records", 
						slog.Int("size_bytes", len(p.Records)), 
						slog.String("content", string(p.Records)))
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
	
	produceCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	produceCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	produceCmd.Flags().StringVar(&message, "message", "", "Message content")

	fetchCmd.Flags().StringVar(&topicID, "topic-id", "", "Topic UUID in hex")
	fetchCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	fetchCmd.Flags().Int64Var(&offset, "offset", 0, "Fetch offset")

	rootCmd.AddCommand(apiVersionsCmd)
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(fetchCmd)

	topicCmd.AddCommand(topicListCmd)
	topicCmd.AddCommand(topicDescribeCmd)
	rootCmd.AddCommand(topicCmd)
}
