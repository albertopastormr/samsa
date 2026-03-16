package cli

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

func SetupLogger(isServer bool) {
	var handler slog.Handler
	if isServer {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	}
	slog.SetDefault(slog.New(handler))
}

var rootCmd = &cobra.Command{
	Use:   "samsa",
	Short: "Samsa is a high-performance Kafka clone",
	Long:  `Samsa is a lightweight Kafka broker and client implementation in Go.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		SetupLogger(false)
	},
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
