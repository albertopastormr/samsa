package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		srv := network.NewServer("0.0.0.0:9092")
		fmt.Println("Starting Kafka broker on 0.0.0.0:9092")

		go func() {
			if err := srv.ListenAndServe(); err != nil {
				fmt.Printf("Server failed: %v\n", err)
				stop() // Signal main thread to exit if server fails
			}
		}()

		<-ctx.Done()
		srv.Shutdown()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}
