package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xinix00/hopdns/internal/dns"
)

// stringSlice implements flag.Value for repeated -peer flags
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	listenAddr := flag.String("listen", ":8053", "DNS address to listen on")
	domain := flag.String("domain", "internal", "DNS domain suffix")
	defaultAPIKey := flag.String("api-key", "", "Default API key (overridden by key@ in peer URL)")
	configPath := flag.String("config", "", "Optional YAML config (currently provides static CNAMEs)")
	var peers stringSlice
	flag.Var(&peers, "peer", "Cluster agent endpoint (repeatable, e.g., -peer http://host:8080 -peer http://key@host:8080)")
	flag.Parse()

	if len(peers) == 0 {
		log.Fatal("at least one -peer required (e.g., -peer http://127.0.0.1:8080)")
	}

	var cnames *dns.CNAMEs
	if *configPath != "" {
		cfg, err := dns.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("load config %s: %v", *configPath, err)
		}
		cnames = dns.NewCNAMEs(cfg.CNAMEs)
	}

	log.Printf("Starting hopdns on %s, domain=%s, peers=%d, cnames=%d", *listenAddr, *domain, len(peers), cnames.Len())

	cache := dns.NewCache()
	server := dns.NewServer(cache, *listenAddr, *domain)
	server.SetCNAMEs(cnames)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a watcher for each peer (including local cluster)
	for _, raw := range peers {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		endpoint, apiKey := parsePeer(raw, *defaultAPIKey)
		w := dns.NewWatcher(endpoint, cache, apiKey)
		go w.Run(ctx)
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		_ = server.Shutdown()
	}()

	if err := server.Run(); err != nil {
		log.Fatalf("DNS server error: %v", err)
	}
}

// parsePeer extracts the API key from the peer URL's userinfo (key@host).
// Returns the clean endpoint (without userinfo) and the API key to use.
func parsePeer(raw, defaultKey string) (endpoint, apiKey string) {
	u, err := url.Parse(raw)
	if err != nil {
		return raw, defaultKey
	}

	if u.User != nil {
		key := u.User.Username()
		u.User = nil
		return u.String(), key
	}

	return raw, defaultKey
}
