# cloudflared-lean

Een node achter NAT publiek bereikbaar via een Cloudflare-tunnel, zonder
inkomende poort. De app belt uit naar de edge van Cloudflare en praat het
tunnelprotocol zelf, op applib en de lean-crates. Voor de volledige
cloudflared (zijn eigen CLI in een slot) zie `go/cloudflared`.

```
4,44 MB RAM   cloudflared-lean in Go (TamaGo, gemeten 19-08)
  530 kB      dit image, geladen deel (.text, .rodata, .data; release, 30-09)
```

## Wat het is

| laag | wie | doet |
| --- | --- | --- |
| TCP | `applib::appnet` | de eigen netstack van het slot (leannet) |
| TLS 1.3 | leanhttps over leantls | SNI `h2.cftunnel.com`, geen ALPN, keten tegen Cloudflare's eigen drie CA's (`src/cfroots.pem`) |
| HTTP/2 als server | leanh2 | de edge is de client: hij stuurt de preface, pingt en opent de streams |
| registratie | `src/register.rs`, `src/capnp.rs` | Cap'n Proto op de control-stream: bootstrap, `registerConnection`, het antwoord |
| ingress | `src/ingress.rs`, `src/regex.rs` | de routeertabel: hostname exact of `*.x`, pad als reguliere expressie, eerste regel wint |
| oorsprong | leanhttp, `src/origin.rs` | elk verzoek als HTTP/1.1 naar de lokale dienst, de koppen terug in de bundel die de edge eist |

De vier dingen die het protocol anders doet dan je zou denken (de edge is de
HTTP/2-client, geen ALPN, een niet-publiek certificaat, geen stream zonder
PING-antwoord) staan in de README van de Go-voorganger en in de `//!`-koppen
van `edge.rs` en `tunnel.rs`. Op 30-09 is de protocolkern (TLS met de
ketentoets, leanh2 als server, de control-stream, de registratie) vanaf de
host tegen `region1.v2.argotunnel.com:7844` gedraaid met een verzonnen token:
de edge antwoordde netjes `Unauthorized: Invalid tunnel secret` met "niet
opnieuw". Met een echt token is hij nog niet gedraaid (dat kan alleen op ijzer
met het token van het dashboard).

Een remote-managed tunnel krijgt zijn ingress van het dashboard geduwd (een
stream met `update-configuration`); de tabel wisselt dan in zijn geheel om, of
blijft staan als de push kapot is (de edge hoort waarom). Vóór de eerste push
geldt `TUNNEL_INGRESS` of één regel naar `TUNNEL_URL`.

## De env

| sleutel | standaard | wat |
| --- | --- | --- |
| `TUNNEL_TOKEN` | (verplicht) | het token van de named tunnel uit het dashboard (Zero Trust, Networks, Tunnels, "Install connector": de lange base64 na `--token`) |
| `DNS` | (geen) | de DNS-server van het slot, voor de edge-namen en voor diensten met een naam; zonder `DNS` alleen IP-adressen (zie `TUNNEL_EDGE`) |
| `TUNNEL_URL` | `http://$HOPOS_HOST`, anders `http_status:503` | waar verkeer heen gaat vóór de eerste push |
| `TUNNEL_INGRESS` | (geen) | een eigen tabel als JSON, in de vorm van het dashboard: `{"ingress":[{"hostname":"…","path":"…","service":"http://…"},{"service":"http_status:404"}]}`; wint van `TUNNEL_URL`, en elke push wint van deze |
| `TUNNEL_CONNECTIONS` | `4` | aantal edge-verbindingen, 1 tot 8 (cloudflared's standaard is 4) |
| `TUNNEL_EDGE` | `region1.v2.argotunnel.com,region2.v2.argotunnel.com` | de edge, als namen of IPv4-adressen met komma's; verbinding `i` begint bij naam `i` en schuift door bij elke nieuwe poging |

Het token is een geheim: het staat alleen in de jobspec die naar de node gaat,
nooit in deze repo, en de app zet het nooit in een logregel.

De dienst in een regel is `http://host[:poort][/voorvoegsel]` (de host een
IPv4-adres of een naam via `DNS`), of `http_status:<code>`. Een dienst in een
ander slot op dezelfde node is `http://10.100.0.<slot+1>:<poort>`; een dienst
op het LAN zijn eigen adres.

## Markers

| marker | wanneer |
| --- | --- |
| `HOPOS_CFTUNNEL_UP edge=<ip>:7844 conn=<uuid> colo=<code> index=<i> up=<n>` | een verbinding is geregistreerd; `conn` is de verbindings-id van de edge |
| `HOPOS_CFTUNNEL_REQ n=<n>` | een doorgegeven verzoek (methode, host, pad zonder query, dienst, status): de eerste zestien elk, daarna elk honderdste |
| `HOPOS_CFTUNNEL_REQ_FAIL n=<n>` | een verzoek dat niet bij de dienst aankwam (zelfde ritme) |
| `HOPOS_CFTUNNEL_CONFIG version=<v> rules=<n>` | de tabel van de start, en elke toegepaste push, met de regels eronder |
| `HOPOS_CFTUNNEL_CONFIG_FAIL version=<v>` | een geweigerde push; `v` is de versie die blijft staan |
| `HOPOS_CFTUNNEL_RETRY index=<i> up=<n>` | een verbinding viel of kwam niet op, met de reden en de wachttijd |
| `HOPOS_CFTUNNEL_GIVEUP index=<i>` | de edge zei "niet opnieuw" (een ingetrokken of verkeerd token) |
| `HOPOS_CFTUNNEL_FAIL <reden>` | de app stopt: de env is onbruikbaar, of de edge weigerde elke verbinding met "niet opnieuw" |
| `HOPOS_APP_RNG source=<bron>` | eenmalig, van applib: waar de willekeur van TLS vandaan komt (`hardware` of `jitter`, het zaad van de kern gemengd met eigen jitter); `HOPOS_APP_RNG_NONE` als de kern geen zaad legt |

## Bouwen en toetsen

```sh
cargo test -p cloudflared-lean
cargo clippy --release --target aarch64-unknown-none-softfloat -p cloudflared-lean -- -D warnings
cargo build --release --target aarch64-unknown-none-softfloat -p cloudflared-lean
rust-objcopy --strip-debug target/aarch64-unknown-none-softfloat/release/cloudflared-lean cloudflared-lean.elf
```

De host-toetsen leggen de protocollagen vast: de registratieberichten en de
antwoorden byte voor byte tegen wat de Go-voorganger maakte, het token en de
Cap'n Proto-rondgang uit de Go-toetsen, de ingress-regels uit de Go-toetsen,
de kopbundel tegen Go, en de drie CA's door leantls.

## Op de O6N

De O6N staat op `192.168.1.205`, Hop luistert op `:9080`. Zonder `hop`-CLI gaat
de jobspec met `curl` naar de leader (de bench-node draait met
`hopos.insecure=1`; met `hopos.apikey` vraagt de API een
`X-Hop-Auth`-handtekening die curl niet zelf zet).

1. Zet het image op een HTTP-server die de node kan bereiken, bijvoorbeeld op
   de laptop naast het board:

   ```sh
   python3 -m http.server 8000 --bind 0.0.0.0
   ```

2. Plaats hem (`LAPTOP` is het adres van de laptop, het token uit het
   dashboard, `DNS` de resolver van het LAN of een publieke):

   ```sh
   curl -X POST -H 'Content-Type: application/json' \
     -d '{"name":"cloudflared-lean","driver":"hop",
          "artifacts":[{"url":"http://LAPTOP:8000/cloudflared-lean.elf"}],
          "memory_limit":33554432,
          "env":{"TUNNEL_TOKEN":"eyJh…","DNS":"1.1.1.1",
                 "TUNNEL_URL":"http://10.100.0.3:80"}}' \
     http://192.168.1.205:9080/v1/jobs
   ```

   Geen `ports`: er komt niets binnen, de tunnel belt uit. 32 MiB is ruim (het
   image is een halve megabyte; de rest is de netstack, vier verbindingen en
   hun HTTP/2-vensters). `TUNNEL_URL` hier wijst naar welcome in slot 2; zet
   het op de dienst die je wilt laten zien, of laat het dashboard de ingress
   duwen.

3. Op de console: `HOPOS_CFTUNNEL_CONFIG version=0`, dan vier keer
   `HOPOS_CFTUNNEL_UP edge=…:7844 conn=… colo=AMS`, en na een push van het
   dashboard `HOPOS_CFTUNNEL_CONFIG version=<n>` met de regels. Een bezoek aan
   de publieke hostname geeft `HOPOS_CFTUNNEL_REQ n=1`.

4. Weg: `curl -X DELETE http://192.168.1.205:9080/v1/jobs/cloudflared-lean`.
   De edge ziet de verbindingen vallen en haalt ze uit de rotatie.

Een verkeerd token geeft per verbinding `HOPOS_CFTUNNEL_GIVEUP` (de edge zegt
`Unauthorized: Invalid tunnel secret`) en daarna `HOPOS_CFTUNNEL_FAIL`. Staat
de wandklok van de kern nog niet (geen SNTP), dan zegt `HOPOS_CFTUNNEL_RETRY`
dat het certificaat van de edge nog niet te toetsen is, en probeert hij het
met terugval opnieuw.

## Wat het (nog) niet draagt

Bewust weigeren met een reden is duidelijker dan half werken:

- **Websockets**: een 502 met een reden; het pad bestaat in leanh2, maar
  zonder toets tegen een echte websocket-oorsprong beloven we het niet (als in
  Go).
- **Kale TCP-stromen** (WARP, `cloudflared access`): 502 (als in Go).
- **https-oorsprongen**: vraagt leanhttps plus een keuze over
  certificaatverificatie (als in Go).
- **De quic-transport**, metrics, auto-update, diagnostiek (als in Go).
- **Het SRV-record** `_v2-origintunneld._tcp.argotunnel.com`: de resolver van
  applib vraagt alleen A-records, en één adres per naam. De twee vaste
  regionamen zijn waar het SRV-record naar wijst; een nieuwe poging vraagt
  opnieuw en krijgt dan vaak een ander adres.
- **Een nette afmelding** (`unregisterConnection`): de kern stopt een slot met
  de kill-vlag, zonder afscheidsronde, dus er is geen moment om hem te sturen.
  De Go-voorganger had hem wel, maar riep hem nergens aan.
- **Een stiltetermijn op de body van de dienst**: na de antwoordkop wacht de
  tunnel zo lang als de dienst erover doet (als in Go); een dienst die midden
  in een body stilvalt, houdt die ene stream vast.

## Herkomst

Deze code is nieuw. Wat eruit is overgenomen zijn feiten, niet regels: de
kopnamen, de interface- en methode-id's van `RegistrationServer`, de
struct-layouts en de drie CA-certificaten, alle uit de gepinde
[cloudflared](https://github.com/cloudflare/cloudflared) (Apache 2.0) en
capnproto2's `rpc.capnp`, via de Go-voorganger. De naam is
`cloudflared-lean` en niet `cloudflared`: Apache 2.0 geeft geen merkrechten,
en dit is niet hun build.
