package cli

import (
	"encoding/hex"
	"fmt"
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
			fmt.Printf("Failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.ApiVersions()
		if err != nil {
			fmt.Printf("ApiVersions failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("ErrorCode: %d\n", resp.ErrorCode)
		for _, entry := range resp.ApiKeys {
			fmt.Printf(" API %d: v%d-v%d\n", entry.ApiKey, entry.MinVersion, entry.MaxVersion)
		}
	},
}

var metadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "Fetch topic and partition metadata",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" {
			fmt.Println("Error: --topic is required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			fmt.Printf("Failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.DescribeTopicPartitions([]string{topic})
		if err != nil {
			fmt.Printf("Metadata failed: %v\n", err)
			os.Exit(1)
		}
		for _, t := range resp.Topics {
			fmt.Printf("Topic: %s (ID: %x, Error: %d)\n", t.Name, t.TopicId, t.ErrorCode)
			for _, p := range t.Partitions {
				fmt.Printf("  Partition %d: Leader %d, Error %d\n", p.PartitionId, p.Leader, p.ErrorCode)
			}
		}
	},
}

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce a message to a topic",
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" || message == "" {
			fmt.Println("Error: --topic and --message are required")
			cmd.Usage()
			os.Exit(1)
		}

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			fmt.Printf("Failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.Produce(topic, partition, []byte(message))
		if err != nil {
			fmt.Printf("Produce failed: %v\n", err)
			os.Exit(1)
		}
		for _, r := range resp.Responses {
			fmt.Printf("Topic: %s\n", r.Name)
			for _, p := range r.Partitions {
				fmt.Printf("  Partition %d: Error %d, Offset %d\n", p.Index, p.ErrorCode, p.BaseOffset)
			}
		}
	},
}

var fetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch records from a topic partition",
	Run: func(cmd *cobra.Command, args []string) {
		if topicID == "" {
			fmt.Println("Error: --topic-id is required (hex format)")
			cmd.Usage()
			os.Exit(1)
		}

		tidBytes, err := hex.DecodeString(topicID)
		if err != nil || len(tidBytes) != 16 {
			fmt.Printf("Invalid Topic ID: must be 32 hex characters (16 bytes), got %d chars: %v\n", len(topicID), err)
			os.Exit(1)
		}
		var tid [16]byte
		copy(tid[:], tidBytes)

		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			fmt.Printf("Failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.Fetch(tid, partition, offset)
		if err != nil {
			fmt.Printf("Fetch failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("ErrorCode: %d\n", resp.ErrorCode)
		for _, t := range resp.Topics {
			fmt.Printf("Topic ID: %x\n", t.TopicId)
			for _, p := range t.Partitions {
				fmt.Printf("  Partition %d: Error %d, HighWatermark %d\n", p.PartitionIndex, p.ErrorCode, p.HighWatermark)
				if len(p.Records) > 0 {
					fmt.Printf("    Records (%d bytes): %s\n", len(p.Records), string(p.Records))
				}
			}
		}
	},
}

var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "List all topics",
	Run: func(cmd *cobra.Command, args []string) {
		kc, err := client.NewKafkaClient(BrokerAddr)
		if err != nil {
			fmt.Printf("Failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer kc.Close()

		resp, err := kc.DescribeTopicPartitions(nil) // Empty list means "all" in our refactored handler
		if err != nil {
			fmt.Printf("Topic listing failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("%-20s %-32s %-10s\n", "NAME", "ID", "PARTITIONS")
		for _, t := range resp.Topics {
			if t.ErrorCode == 0 {
				fmt.Printf("%-20s %x %-10d\n", t.Name, t.TopicId, len(t.Partitions))
			}
		}
	},
}

func init() {
	metadataCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	
	produceCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	produceCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	produceCmd.Flags().StringVar(&message, "message", "", "Message content")

	fetchCmd.Flags().StringVar(&topicID, "topic-id", "", "Topic UUID in hex")
	fetchCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	fetchCmd.Flags().Int64Var(&offset, "offset", 0, "Fetch offset")

	rootCmd.AddCommand(apiVersionsCmd)
	rootCmd.AddCommand(metadataCmd)
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(fetchCmd)
	rootCmd.AddCommand(topicsCmd)
}
