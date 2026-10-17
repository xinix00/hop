# hop-gui

A static cluster dashboard for Hop, built with the shared Haasstyle components.

The dashboard itself is static: `index.html`, `app.js`, `appearance.js`,
`style.css` and `vendor/`, with no build step, CDN or frontend framework.
This repository also carries its host: the crate `hop-gui` (in `server/`)
bakes those files into one binary with `include_bytes!` and serves them,
either on a plain host or as an app on a HopOS node that Hop places like
any other job.

## Hosting on HopOS

`hop-gui-hopos` is an app for a HopOS slot (aarch64, `applib` with
`leanhttp`). Build it and strip the debug info (the symbols stay; the
kernel reads them when it places the image):

```sh
cargo build --release --target aarch64-unknown-none-softfloat --features hopos --bin hop-gui-hopos
rust-objcopy --strip-debug target/aarch64-unknown-none-softfloat/release/hop-gui-hopos hop-gui.elf
```

Put `hop-gui.elf` where the node can download it (any HTTP or HTTPS URL,
for example a GitHub release) and give Hop this jobspec:

```json
{
  "name": "hop-gui",
  "driver": "hop",
  "artifacts": [{"url": "http://LAPTOP:8000/hop-gui.elf"}],
  "memory_limit": 67108864,
  "ports": {"http": 80}
}
```

`"ports":{"http":80}` publishes port 80 of the node: the kernel forwards it
to the slot, and Hop tells the app the port in `ER_PORT_HTTP`. The node's
console shows `HOPOS_SLOT_PUBLISH` and then
`hop-gui: serving the Hop dashboard on ... HOPOS_HOPGUI_UP files=15 bytes=4153524 port=80`.
Open `http://NODE/`. Port 80 is free because Hop itself uses 8080 (agent)
and 9080 (leader). To keep it on a fresh node, put the same JSON on one line
behind `hopos.init[]=` in `hopos.cfg`. `DELETE /v1/jobs/hop-gui` removes it
and withdraws the port.

The image is about 4.5 MB, most of it the icon font in `vendor/`; the
`memory_limit` of 64 MiB leaves ample room.

## Hosting on a host

```sh
cargo build --release --features std --bin hop-gui
target/release/hop-gui -listen :3000
```

`-listen` takes `[host]:port` (`:3000`, the default, is every interface).
It prints `HOPGUI_UP listen=... files=... bytes=...` on stderr. Eight
worker threads, each owning one connection at a time; the files are
read-only, so there is no shared state.

Any static web server works as well, since the dashboard is plain files:

```sh
python3 -m http.server 3000
```

Both hosts send `ETag` and `Cache-Control: no-cache`, so a browser
revalidates every file and gets a `304` until it changes, and a new
dashboard shows up without anyone clearing a cache.

## Connecting to a cluster

Open the dashboard, choose **New server** below the server list, and enter
a name, an agent address (`http://NODE:8080`) and the cluster's API key, if
it has one. `?name=NODE&ip=IP` prefills the form.

**The browser talks to the agent directly.** The page is served by
`hop-gui`, but every API call goes from the browser to the agent address
you entered: that address must be reachable from the browser, and the agent
must allow the cross-origin request. Hop agents do (on the agent port,
8080 by default, on every answer, including answers that come from the
leader and streams):

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PATCH, OPTIONS
Access-Control-Allow-Headers: Content-Type, X-Hop-Auth
```

The preflight (`OPTIONS`) needs no signature. When a public origin asks
for a LAN address, Chrome's Private Network Access preflight is answered
with `Access-Control-Allow-Private-Network: true`. The leader port (9080)
is not meant for browsers and sends no CORS headers.

With an API key, every request carries
`X-Hop-Auth: hex(HMAC-SHA256(key, METHOD + "\n" + PATH + "\n" + hex(sha256(body))))`,
computed with Web Crypto, so the key itself never travels. Web Crypto only
works in a secure context: serve the dashboard on `localhost` or over HTTPS
(or use a cluster without a key, `hopos.insecure=1`).

The calls the dashboard makes, all on the agent, which hands the `/v1/`
ones to the leader:

| Call | For |
| --- | --- |
| `GET /leader` | the leader address in the header |
| `GET /v1/status` | `agents`, `jobs`, `total_placed`, `settling`, `placed` |
| `GET /v1/agents` | the agents table |
| `GET /v1/agents/{id}/capacity` | CPU, memory, tasks and attributes per agent |
| `GET /v1/tasks` | the tasks of an open agent (`tasks_by_agent`), the kernel and Hop as `system` |
| `GET /v1/jobs`, `POST /v1/jobs` | the jobs table, a new job, a redeploy |
| `DELETE /v1/jobs/{name}` | delete a job |
| `PATCH /v1/jobs/{name}/priority` | the drag order (`{"priority": index}`) |
| `GET /v1/jobs/{name}/status` | the task table (`tasks_by_agent`) |
| `GET /v1/agents/{id}/logs/{task}/{stdout,stderr}` | the live log (SSE; without a query it follows, `?follow=0` is a snapshot) |
| `GET /v1/events` | live updates (SSE) |

The hop repository checks that every one of these exists, with this
shape: its test `api/src/tests_gui.rs` greps this `app.js` when the two
repositories sit side by side.

## Tests

```sh
sh tools/gate.sh        # host tests, clippy, rustfmt, the HopOS build
sh tools/host-test.sh   # hop-gui -listen next to agentd (HOP_DIR, default ../hop)
sh tools/qemu-test.sh   # HopOS on QEMU: Hop places hop-gui-hopos with ports 80
```

`tools/qemu-test.sh` (`HOPOS_DIR`, default `../../hop-os`, and `HOP_DIR`)
boots HopOS with Hop in slot 1, posts the jobspec above to the agent,
fetches `/`, the scripts, the stylesheet and the font through the
published port and compares them byte for byte, checks a `304`, and then
runs the dashboard's API sequence against the agent on the node with
`tools/dashboard-api.py` (signed like `app.js`, with an `Origin`, CORS
required on every answer): the preflight, `/leader`, status, agents, jobs,
the job status, the capacity, the first lines of `/v1/events` and of the
app's live log, the priority, and the delete. `tools/host-test.sh` runs the
same sequence against `agentd` with a real key and also requires a `401`
for an unsigned call.

Dependencies come from git tags only, never a path across a repository:
hop `v3.0.0` (`hostnet`), HopOS `v3.0.0` (`applib` with
`http`, and `sync`), lean `v3.1.3` (`leanhttp`).

## Dashboard

- Saved clusters, live updates over SSE, agent failover and fallback polling.
- Agent CPU, memory, temperature and task counts.
- Jobs with draggable priority, creation, redeployment and deletion.
- Job details, task status and live stdout/stderr logs.
- Direct links to `#agents`, `#agents/<encoded-id>`, `#jobs` and `#jobs/<encoded-name>`.
- `?name=NODE&ip=IP` prefills the cluster form without connecting automatically.
- Responsive tables and keyboard-accessible forms, confirmations and dialogs.

## Appearance

Use **Appearance** at the bottom of the left sidebar, in the same place as EasyACP. Choose these independently:

- Theme: Senior / Classic 95, Medior / Glossy 08, Junior / Tactile Matte.
- Accent colour: purple, red, blue, petrol or green.
- Mode: dark or light.

Glossy 08, purple and dark are the defaults. Appearance is stored as `hop-appearance` in this browser, applied before the stylesheet loads, and synchronized across open tabs. Existing `hop-clusters`, `hop-active-cluster` and endpoint pool settings are retained.

## Maintaining the styles

`style.css` handles Hop's layout. The material, controls, spinner, select and decision dialogs come from `vendor/`; see [vendor/README.md](vendor/README.md). Keep these shared files in sync with Haasstyle instead of recreating their visuals in app-specific CSS.

`appearance.js` manages the picker and theme tokens. `app.js` handles the Hop API and renders the same shared components for dynamic content.

Saved servers use the shared vertical `t-nav` / `t-choice` navigation in the
left sidebar. **New server** sits immediately below the list; **Appearance**
sits in the sidebar footer. Up/Down and Home/End navigate the server list.
The dashboard's delete icon removes the selected server from this browser,
after confirmation; its workloads keep running. On narrow screens the sidebar
stacks above the dashboard, keeping the server list vertical.

Icon actions remain at least 36 × 36 px; tags keep the shared 32 px minimum
height and tables vertically center text, status labels and actions.
