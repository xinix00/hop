package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xinix00/hopprom/internal/collector"
	"github.com/xinix00/hopprom/internal/exporter"
)

const (
	defaultPollInterval = 5 * time.Second
	shutdownTimeout     = 5 * time.Second
)

func main() {
	var (
		listen       = flag.String("listen", ":9090", "Listen address for /metrics endpoint")
		agent        = flag.String("agent", "http://127.0.0.1:8080", "Hop agent URL")
		pollInterval = flag.Duration("interval", defaultPollInterval, "Poll interval for collecting metrics")
		apiKey       = flag.String("api-key", "", "API key for hop agent authentication")
	)
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("EasyProm starting...")
	log.Printf("Agent URL: %s", *agent)
	log.Printf("Poll interval: %s", *pollInterval)
	log.Printf("Metrics endpoint: http://%s/metrics", *listen)

	// Create collector
	col := collector.New(*agent, *apiKey)

	// Initial collection
	if err := col.Collect(); err != nil {
		log.Printf("Initial collection failed: %v", err)
	}

	// Start polling loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pollLoop(ctx, col, *pollInterval)

	// Setup HTTP server
	exp := exporter.New(col)
	mux := http.NewServeMux()
	mux.Handle("/metrics", exp)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Start server
	go func() {
		log.Printf("Listening on %s", *listen)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}

func pollLoop(ctx context.Context, col *collector.Collector, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := col.Collect(); err != nil {
				log.Printf("Collection error: %v", err)
			}
		}
	}
}
