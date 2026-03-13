package cli

import (
	"fmt"
	"os"

	"github.com/albertopastormr/samsa/internal/client"
	"github.com/spf13/cobra"
)

var (
	topic     string
	partition int32
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

func init() {
	metadataCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	
	produceCmd.Flags().StringVar(&topic, "topic", "", "Topic name")
	produceCmd.Flags().Int32Var(&partition, "partition", 0, "Partition index")
	produceCmd.Flags().StringVar(&message, "message", "", "Message content")

	rootCmd.AddCommand(apiVersionsCmd)
	rootCmd.AddCommand(metadataCmd)
	rootCmd.AddCommand(produceCmd)
}
