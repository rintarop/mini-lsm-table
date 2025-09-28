package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	httpHandler "github.com/Bloom0716/mini-bigtable/internal/interface/http"
	"github.com/Bloom0716/mini-bigtable/internal/service"
)

func main() {
	fmt.Println("Mini LSM-Tree Table API Server")

	// Create persistent directory for data storage
	dataDir := filepath.Join("data", "mini_lsm")

	// Create LSM service
	service, err := service.NewLSMTableService(dataDir, 3)
	if err != nil {
		log.Fatalf("Failed to create LSM service: %v", err)
	}
	defer service.Close()

	// Create HTTP handler and server
	handler := httpHandler.NewHandler(service)

	port := "8080"
	if envPort := os.Getenv("PORT"); envPort != "" {
		port = envPort
	}

	server := httpHandler.NewServer(handler, port)

	fmt.Printf("🚀 Starting LSM-Tree API server on port %s...\n", port)
	fmt.Printf("📖 API Documentation: http://localhost:%s/\n", port)
	fmt.Printf("💚 Health Check: http://localhost:%s/health\n", port)
	fmt.Printf("📊 Status: http://localhost:%s/api/status\n", port)
	fmt.Printf("💾 Data Directory: %s\n", dataDir)

	log.Fatal(server.Start())
}
