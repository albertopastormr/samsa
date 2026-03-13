package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "samsa",
	Short: "Samsa is a high-performance Kafka clone",
	Long:  `Samsa is a lightweight Kafka broker and client implementation in Go.`,
}

var BrokerAddr string

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&BrokerAddr, "broker", "localhost:9092", "Kafka broker address")
}
