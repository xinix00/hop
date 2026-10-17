//go:build tamago

// hoplb-hopos is hoplb als HopOS-slot-app: dezelfde watcher/proxy/metrics
// als cmd/hoplb, maar met het app-skelet uit hop-os (applib voor de
// node-handshake, appnet voor de eigen netstack). Config komt uit de
// jobspec-env in plaats van flags:
//
//	HOP_ADDR        agent-API (default 10.100.0.1:8080 — de node zelf)
//	HOP_API_KEY     HMAC-key voor X-Hop-Auth
//	ER_PORT_HTTP    traffic-poort, door hop gezet uit ports:{http:...}
//	ER_PORT_ADMIN   admin-poort (/health, /metrics), uit ports:{admin:...}
//	HOPLB_TAG       optioneel tag-filter (bijv. "lb:haas")
//
// Jobspec:
//
//	{"name":"hoplb","driver":"hop","count":1,
//	 "artifacts":[{"url":"https://github.com/xinix00/hop/releases/download/rolling-release/hoplb-arm64-tamago.elf"}],
//	 "memory_limit":134217728,
//	 "ports":{"http":80,"admin":9091},
//	 "env":{"HOP_API_KEY":"..."}}
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/xinix00/HopOS/metal/v2/app/applib"
	"github.com/xinix00/HopOS/metal/v2/app/applib/appnet"

	"github.com/xinix00/hoplb/internal/lb"
	"github.com/xinix00/hoplb/internal/metrics"
)

var version = "dev" // -ldflags "-X main.version=vX.Y.Z"

// ringWriter stuurt stdlib-log (watcher en proxy loggen via log.Printf)
// naar de hop-ABI-logring, zodat `run logs hoplb` ze gewoon laat zien.
type ringWriter struct{ app *applib.App }

func (w ringWriter) Write(p []byte) (int, error) {
	w.app.Logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func main() {
	app := applib.Init()
	log.SetFlags(0) // de ring stempelt zelf; geen dubbele timestamps
	log.SetOutput(ringWriter{app: app})

	ip, err := appnet.Up(app) // eigen TCP/IP-stack, eigen IP
	if err != nil {
		app.Logf("net: %v", err)
		app.Exit(1)
	}

	agent := app.Env("HOP_ADDR")
	if agent == "" {
		agent = "10.100.0.1:8080"
	}
	if !strings.Contains(agent, "://") {
		agent = "http://" + agent
	}
	httpPort := app.Env("ER_PORT_HTTP")
	if httpPort == "" {
		httpPort = "80"
	}
	adminPort := app.Env("ER_PORT_ADMIN")
	if adminPort == "" {
		adminPort = "9091"
	}
	tag := app.Env("HOPLB_TAG")

	app.Logf("hoplb %s: agent %s, tag %q, traffic %s:%s, admin :%s", version, agent, tag, ip, httpPort, adminPort)

	m := metrics.New()
	routeTable := lb.NewRouteTable()
	watcher := lb.NewWatcher(agent, routeTable, tag, app.Env("HOP_API_KEY"))
	proxy := lb.NewProxy(routeTable, m)

	go watcher.Run(context.Background())

	// Beide servers moeten blijven staan; valt er één om, dan is de hele lb
	// stuk en is een slot-restart (Exit → hop restart-beleid) de juiste zet.
	fail := make(chan error, 2)

	httpServer := &http.Server{
		Addr:    ":" + httpPort,
		Handler: proxy,
		// Slowloris-guard alleen. Bewust GEEN ReadTimeout (grote/trage uploads
		// moeten door naar backends) en GEEN WriteTimeout (streaming/SSE mag
		// niet afgekapt) — zelfde afweging als cmd/hoplb.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() { fail <- fmt.Errorf("http: %w", httpServer.ListenAndServe()) }()

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	adminMux.Handle("/metrics", metrics.NewExporter(m))
	adminServer := &http.Server{
		Addr:              ":" + adminPort,
		Handler:           adminMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() { fail <- fmt.Errorf("admin: %w", adminServer.ListenAndServe()) }()

	app.Logf("server: %v", <-fail)
	app.Exit(1) // een lb die stopt met serveren is een crash, by design
}
