//go:build tamago

// hopdns-hopos is hopdns als HopOS-slot-app: dezelfde cache/watcher/server
// als cmd/hopdns, maar met het app-skelet uit hop-os (applib voor de
// node-handshake, appnet voor de eigen netstack — UDP-listen loopt via
// go-net's gvisor-socket, net als op de host). Config komt uit de
// jobspec-env in plaats van flags:
//
//	HOP_ADDR        default-peer als HOPDNS_PEERS leeg is (10.100.0.1:8080)
//	HOP_API_KEY     default HMAC-key (per peer te overriden met key@host)
//	ER_PORT_DNS     luisterpoort, door hop gezet uit ports:{dns:...}
//	HOPDNS_PEERS    komma-gescheiden peer-URLs (federation), key@host mag
//	HOPDNS_DOMAIN   DNS-domein-suffix (default "internal", zoals cmd/hopdns)
//
// Jobspec (count:-1 — hopdns hoort op elke node):
//
//	{"name":"hopdns","driver":"hop","count":-1,
//	 "artifacts":[{"url":"https://github.com/xinix00/hop/releases/download/rolling-release/hopdns-arm64-tamago.elf"}],
//	 "memory_limit":134217728,
//	 "ports":{"dns":5353},
//	 "env":{"HOP_API_KEY":"...","HOPDNS_DOMAIN":"hop.local"}}
package main

import (
	"context"
	"log"
	"net/url"
	"strings"

	"github.com/xinix00/HopOS/metal/v2/app/applib"
	"github.com/xinix00/HopOS/metal/v2/app/applib/appnet"

	"github.com/xinix00/hopdns/internal/dns"
)

var version = "dev" // -ldflags "-X main.version=vX.Y.Z"

// ringWriter stuurt stdlib-log (watcher en server loggen via log.Printf)
// naar de hop-ABI-logring, zodat `run logs hopdns` ze gewoon laat zien.
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

	port := app.Env("ER_PORT_DNS")
	if port == "" {
		port = "5353"
	}
	domain := app.Env("HOPDNS_DOMAIN")
	if domain == "" {
		domain = "internal" // zelfde default als cmd/hopdns
	}
	defaultKey := app.Env("HOP_API_KEY")

	// Peers: expliciete federation-lijst, anders de lokale agent — op HopOS
	// altijd bereikbaar op de interne gateway (10.100.0.1), niet één byte
	// verlaat de NIC.
	var peers []string
	for _, p := range strings.Split(app.Env("HOPDNS_PEERS"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			peers = append(peers, p)
		}
	}
	if len(peers) == 0 {
		agent := app.Env("HOP_ADDR")
		if agent == "" {
			agent = "10.100.0.1:8080"
		}
		if !strings.Contains(agent, "://") {
			agent = "http://" + agent
		}
		peers = []string{agent}
	}

	app.Logf("hopdns %s: domain=%s, peers=%d, dns op %s:%s (udp)", version, domain, len(peers), ip, port)

	cache := dns.NewCache()
	server := dns.NewServer(cache, ":"+port, domain)

	for _, raw := range peers {
		endpoint, apiKey := parsePeer(raw, defaultKey)
		w := dns.NewWatcher(endpoint, cache, apiKey)
		go w.Run(context.Background())
	}

	app.Logf("dns: %v", server.Run())
	app.Exit(1) // een resolver die stopt met luisteren is een crash, by design
}

// parsePeer extraheert de API-key uit de userinfo van de peer-URL (key@host).
// Zelfde helper als cmd/hopdns — bewust gedupliceerd, het is een leaf van
// een paar regels en de mains delen verder niets.
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
