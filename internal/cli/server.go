package cli

import (
	"fmt"
	"os"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/network"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server [config-file]",
	Short: "Start the Samsa Kafka broker",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			err := config.Load(args[0])
			if err != nil {
				fmt.Printf("Failed to load config: %v\n", err)
			}
		}

		srv := network.NewServer("0.0.0.0:9092")
		fmt.Println("Starting Kafka broker on 0.0.0.0:9092")
		if err := srv.ListenAndServe(); err != nil {
			fmt.Printf("Server failed: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}
