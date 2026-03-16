package cli

import (
	"context"
	"log/slog"
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
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		SetupLogger(true)
	},
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			err := config.Load(args[0])
			if err != nil {
				slog.Error("failed to load config", slog.String("file", args[0]), slog.Any("error", err))
			}
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		srv := network.NewServer("0.0.0.0:9092")
		slog.Info("starting Kafka broker", slog.String("addr", "0.0.0.0:9092"))

		go func() {
			if err := srv.ListenAndServe(); err != nil {
				slog.Error("server failed", slog.Any("error", err))
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
