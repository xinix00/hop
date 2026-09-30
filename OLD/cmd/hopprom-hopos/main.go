//go:build tamago

// hopprom-hopos is hopprom als HopOS-slot-app: dezelfde collector/exporter
// als cmd/hopprom, maar met het app-skelet uit hop-os (applib voor de
// node-handshake — READY, heartbeats, kill-flag — en appnet voor de eigen
// netstack). Config komt uit de jobspec-env in plaats van flags:
//
//	HOP_ADDR          agent-API (default 10.100.0.1:8080 — de node zelf)
//	HOP_API_KEY       HMAC-key voor X-Hop-Auth (zelfde als de CLI-conventie)
//	ER_PORT_METRICS   luisterpoort, door hop gezet uit ports:{metrics:...}
//	HOPPROM_INTERVAL  poll-interval (Go duration, default 5s)
//
// Jobspec:
//
//	{"name":"hopprom","driver":"hop","count":1,
//	 "artifacts":[{"url":"https://github.com/xinix00/hop/releases/download/rolling-release/hopprom-arm64-tamago.elf"}],
//	 "memory_limit":134217728,
//	 "ports":{"metrics":9090},
//	 "env":{"HOP_API_KEY":"..."}}
package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/xinix00/HopOS/metal/v2/app/applib"
	"github.com/xinix00/HopOS/metal/v2/app/applib/appnet"

	"github.com/xinix00/hopprom/internal/collector"
	"github.com/xinix00/hopprom/internal/exporter"
)

var version = "dev" // -ldflags "-X main.version=vX.Y.Z"

// ringWriter stuurt stdlib-log (collector en exporter loggen via log.Printf)
// naar de hop-ABI-logring, zodat `run logs hopprom` ze gewoon laat zien.
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
	port := app.Env("ER_PORT_METRICS")
	if port == "" {
		port = "9090"
	}
	interval := 5 * time.Second
	if v := app.Env("HOPPROM_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			app.Logf("HOPPROM_INTERVAL %q ongeldig (%v), gebruik %s", v, err, interval)
		} else {
			interval = d
		}
	}

	app.Logf("hopprom %s: agent %s, interval %s, metrics op %s:%s", version, agent, interval, ip, port)

	col := collector.New(agent, app.Env("HOP_API_KEY"))
	if err := col.Collect(); err != nil {
		app.Logf("initial collect: %v", err)
	}
	go func() {
		for range time.Tick(interval) {
			if err := col.Collect(); err != nil {
				app.Logf("collect: %v", err)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", exporter.New(col))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	app.Logf("http: %v", srv.ListenAndServe())
	app.Exit(1) // een exporter die stopt met serveren is een crash, by design
}
