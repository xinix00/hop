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

	"github.com/xinix00/hoplb/internal/lb"
	"github.com/xinix00/hoplb/internal/metrics"
)

func main() {
	listenAddr := flag.String("listen", ":80", "Address to listen on for HTTP traffic")
	adminAddr := flag.String("admin-listen", ":9091", "Address to listen on for admin endpoints (/health, /metrics)")
	agentAddr := flag.String("agent", "http://127.0.0.1:8080", "Local hop agent address")
	tagFilter := flag.String("tag", "", "Only route jobs with this tag (e.g., lb:haas)")
	apiKey := flag.String("api-key", "", "API key for hop agent authentication")
	flag.Parse()

	log.Printf("Starting hoplb")
	log.Printf("  HTTP traffic: %s", *listenAddr)
	log.Printf("  Admin:        %s (/health, /metrics)", *adminAddr)
	log.Printf("  Agent:        %s", *agentAddr)
	log.Printf("  Tag filter:   %q", *tagFilter)

	// Create metrics collector
	m := metrics.New()

	// Create route table and watcher
	routeTable := lb.NewRouteTable()
	watcher := lb.NewWatcher(*agentAddr, routeTable, *tagFilter, *apiKey)
	proxy := lb.NewProxy(routeTable, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start watcher (polls hop for jobs/tasks)
	go watcher.Run(ctx)

	// Start HTTP traffic server
	httpServer := &http.Server{
		Addr:    *listenAddr,
		Handler: proxy,
		// Slowloris guard only. Deliberately NO ReadTimeout (large/slow uploads
		// must pass through to backends) and NO WriteTimeout (streaming/SSE
		// responses must not be cut) — only headers must arrive promptly.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Printf("HTTP server listening on %s", *listenAddr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start admin server (health + metrics)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", handleHealth)
	adminMux.Handle("/metrics", metrics.NewExporter(m))

	adminServer := &http.Server{
		Addr:              *adminAddr,
		Handler:           adminMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Printf("Admin server listening on %s", *adminAddr)
		if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Admin server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	httpServer.Close()
	adminServer.Close()
}

// handleHealth returns a simple health check response
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}
