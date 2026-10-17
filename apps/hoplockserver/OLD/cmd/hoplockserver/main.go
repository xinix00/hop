// Command hoplockserver is a tiny HTTP server that exposes the minimum
// conditional-write API needed to back a hoplock.Backend.
//
// Think of it as a fake-S3 for lease files: GET/PUT/DELETE on opaque keys,
// with If-Match and If-None-Match preconditions, and a single shared API
// key for authentication. No buckets, no sigv4, no AWS account.
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

	"github.com/xinix00/hoplockserver/internal/server"
	"github.com/xinix00/hoplockserver/internal/store"
)

var version = "dev"

func main() {
	listen := flag.String("listen", ":8090", "Address to listen on")
	dataDir := flag.String("data", "./data", "Directory to store lease files in")
	apiKey := flag.String("api-key", "", "Required X-API-Key header (empty = no auth)")
	flag.Parse()

	if env := os.Getenv("HOPLOCK_API_KEY"); env != "" && *apiKey == "" {
		*apiKey = env
	}

	st, err := store.New(*dataDir)
	if err != nil {
		log.Fatalf("hoplockserver: open store: %v", err)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           server.New(st, *apiKey),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second, // bodies are tiny (lease/state, capped 1 MiB)
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("hoplockserver %s listening on %s (data=%s, auth=%s)",
		version, *listen, *dataDir, authLabel(*apiKey))

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("hoplockserver: %v", err)
	}
}

func authLabel(key string) string {
	if key == "" {
		return "off"
	}
	return fmt.Sprintf("on (%d chars)", len(key))
}
