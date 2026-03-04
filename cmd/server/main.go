package main

import (
	"fmt"
	"os"

	"github.com/albertopastormr/samsa/internal/config"
	"github.com/albertopastormr/samsa/internal/network"
)

func main() {
	if len(os.Args) > 1 {
		err := config.Load(os.Args[1])
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
}
