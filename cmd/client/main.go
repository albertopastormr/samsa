package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/albertopastormr/samsa/internal/client"
)

func main() {
	addr := flag.String("addr", "localhost:9092", "Kafka broker address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: client [flags] <command> [args]")
		fmt.Println("Commands: apiversions, metadata <topic>, produce <topic> <partition> <message>")
		os.Exit(1)
	}

	cmd := args[0]
	kc, err := client.NewKafkaClient(*addr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer kc.Close()

	switch cmd {
	case "apiversions":
		resp, err := kc.ApiVersions()
		if err != nil {
			fmt.Printf("ApiVersions failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("ErrorCode: %d\n", resp.ErrorCode)
		for _, entry := range resp.ApiKeys {
			fmt.Printf(" API %d: v%d-v%d\n", entry.ApiKey, entry.MinVersion, entry.MaxVersion)
		}

	case "metadata":
		if len(args) < 2 {
			fmt.Println("Usage: metadata <topic>")
			os.Exit(1)
		}
		resp, err := kc.DescribeTopicPartitions(args[1:])
		if err != nil {
			fmt.Printf("Metadata failed: %v\n", err)
			os.Exit(1)
		}
		for _, topic := range resp.Topics {
			fmt.Printf("Topic: %s (ID: %x, Error: %d)\n", topic.Name, topic.TopicId, topic.ErrorCode)
			for _, p := range topic.Partitions {
				fmt.Printf("  Partition %d: Leader %d, Error %d\n", p.PartitionId, p.Leader, p.ErrorCode)
			}
		}

	case "produce":
		if len(args) < 4 {
			fmt.Println("Usage: produce <topic> <partition> <message>")
			os.Exit(1)
		}
		topic := args[1]
		var part int32
		fmt.Sscanf(args[2], "%d", &part)
		message := args[3]

		resp, err := kc.Produce(topic, part, []byte(message))
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

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		os.Exit(1)
	}
}
