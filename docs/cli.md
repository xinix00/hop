# The `hop` CLI

`hop` (crate `cli/`, binary `hop`) is the command for a Hop cluster. It talks
to the **leader** over the HTTP API ([api.md](api.md)); only `flip` talks to an
agent, and `image` talks to no one (it is the offline HopOS imager).

```sh
cargo build --release -p cli          # target/release/hop
```

```
Usage: hop [--leader host:port] [--agent host:port] [--api-key KEY] <command> [args]
```

| Command | What it does | API |
| --- | --- | --- |
| `apply <jobspec.json>` | create or update a job from a file (upsert by name) | `POST /v1/jobs` |
| `apply --name N ...` | the same, job built from flags | `POST /v1/jobs` |
| `jobs` | list jobs and their tasks | `/v1/jobs`, `/v1/status`, `/v1/agents`, `/v1/tasks` |
| `status` | cluster summary and per-job health | `/v1/status`, `/v1/jobs`, `/v1/tasks` |
| `agents [id]` | list agents, or one agent's details and capacity | `/v1/agents`, `/v1/agents/{id}/capacity` |
| `logs [-f] <job\|task>` | latest log lines, or follow one task live | `/v1/tasks`, `/v1/agents/{id}/logs/...` |
| `events` | follow cluster events | `GET /v1/events` (SSE) |
| `delete <job>` | delete a job and all its tasks | `DELETE /v1/jobs/{name}` |
| `flip <url> <sha256> [--cold]` | replace the kernel of a HopOS node | `POST /flip` on `--agent` |
| `image <file\|device> ...` | show or set the node config in a HopOS image, bundle or card | none (local) |
| `help`, `--help`, `-h` | print usage | |

On success `hop` exits 0. On failure it prints `Error: <reason>` on stderr and
exits 1; for an HTTP error the reason is the server's `{"error": ...}`
message, or `request failed: status N`.

## Global flags and environment

| Flag | Env | Default | Used by |
| --- | --- | --- | --- |
| `--leader host:port` | `HOP_LEADER` | `localhost:9080` | every networked command except `flip` |
| `--agent host:port` | `HOP_AGENT` | `localhost:8080` | `flip` |
| `--api-key KEY` | `HOP_API_KEY` | empty (no signing) | all networked commands |

- A flag overrides its environment variable.
- Global flags may appear anywhere on the command line, as `--leader X`,
  `--leader=X` or `-leader X`.
- An address is `host:port` (becomes `http://host:port`) or a full URL.
- With a key, every request carries `X-Hop-Auth` (see
  [api.md](api.md#authentication-x-hop-auth)). The key itself is never sent
  and never printed.
- Timeouts: 120 s per request (an `apply` waits for the rollout); a followed
  stream (`logs -f`, `events`) gives up after 60 s without bytes (the server
  sends a keepalive every 15 s).

Why the leader: an agent often sits on an address the CLI cannot reach (a
HopOS slot LAN, a private network behind the leader); the leader reaches them
all and proxies `/v1/agents/{id}/logs/...` and `/capacity`. Against an older
leader without these routes (`404`), `jobs`, `status` and `agents <id>` fall
back to calling the agents directly at their registered endpoints.

```sh
export HOP_LEADER=192.168.1.10:9080
export HOP_API_KEY=<shared secret>
hop status
```

## `hop apply`

```
hop apply <jobspec.json>
hop apply --name N (--command CMD | --image IMG | --driver hop --artifact URL) [flags]
```

**From a file**: the file is parsed locally first (a typo fails here with the
file name, not as `invalid json` from the leader) and must have a `name`; it
is then sent **unchanged**, so fields the CLI does not know still reach the
leader. Job spec fields: [data-structures.md](data-structures.md).

```sh
echo '{"name": "sleeper", "command": "sleep 30"}' > job.json
hop apply job.json
```

**From flags** (when a file is given, job flags are ignored):

| Flag | Job field | Notes |
| --- | --- | --- |
| `--name N` | `name` | required |
| `--command CMD` | `command` | |
| `--image IMG` | `image` | Docker image |
| `--driver exec\|docker\|hop` | `driver` | `hop` requires at least one `--artifact` |
| `--count N` | `count` | `-1` = every agent; default 0 (one instance) |
| `--cpu SHARES` | `cpu_shares` | 1024 per core |
| `--memory 512M` | `memory_limit` | bytes, or a `K`/`M`/`G` suffix (binary units) |
| `--priority N` | `priority` | `0` = first; unset puts the job at the back |
| `--update-policy rolling\|recreate\|blue-green` | `update_policy` | **default `rolling`** (always sent) |
| `--env K=V` | `env` | repeatable; a value keeps its commas and `=` |
| `--artifact [k=v,...::]URL` | `artifacts` | repeatable; `k=v` pairs before `::` become the artifact's `match` (node attributes; each agent takes the first artifact that matches) |
| `--affinity k=v[,k=v]` | `affinity` | repeatable |
| `--tag k=v[,k=v]` | `tags` | repeatable |
| `--check-type http\|tcp\|file` | `health_check.type` | a health check is added if `--check-type` or `--check-path` is set |
| `--check-path P` | `health_check.path` | |
| `--check-port NAME` | `health_check.port` | a port name |
| `--check-failures N` | `health_check.failure_threshold` | |

Flags take `--flag value`, `--flag=value` or a single dash. Without
`--driver hop`, one of `--command` or `--image` is required. Ports, volumes,
user, restart settings and artifact headers/auth have no flag: use a file.
`hop apply --help` prints the flag summary.

```sh
hop apply --name web --command ./web --count 2 --memory 256M --env MODE=prod
hop apply --name blinky --driver hop \
  --artifact board=licheerv::https://example.com/blinky-riscv64.elf \
  --artifact https://example.com/blinky-arm64.elf
```

Output, from the leader's answer:

```
Job 'web' dispatched
Job 'web' updated (policy=rolling)
Job 'web' stored, pending dispatch: <reason>
```

A fixed port with an explicit `rolling` or `blue-green` policy is refused by
the leader (`400`); see [api.md](api.md#post-v1jobs).

## `hop jobs`

Two tables: the jobs, then every task (if any).

```
NAME     DRIVER  PLACED  EXPECTED  TASKS
web      exec    2       2         2 running
blinky   hop     3       all(3)    2 running, 1 downloading

TASK       JOB     AGENT       STATE                          PID   PORTS       RESTARTS
web-1a2b   web     pi-kitchen  running                        4711  http=31002  0
...
```

- `EXPECTED` is the `count`, `all(N)` for `count: -1` (N = registered agents),
  `1` for a count of 0 or less.
- `TASKS` counts the job's tasks per state; the system tasks `kern` and `hop`
  never count as instances.
- `STATE` shows download progress as `downloading 42% (1.2/2.9 MB)`.
- Agents that did not answer the leader in time are reported on stderr:
  `agent <id>: did not answer the leader in time`.

## `hop status`

```
Cluster: home
Leader:  192.168.1.10:9080
Agents:  3
Placed:  5 total
Status:  settling...          (only while the leader settles)

NAME     PLACED  EXPECTED  STATUS
web      2       2         OK
dns      0       1         DEGRADED
```

`STATUS` is `OK` (placed >= expected), `DEGRADED`, `DEPLOYING` (the last
rollout did not finish), or the start phase of a task that is still
`QUEUED` / `DOWNLOADING n% (...)`. Task phases are best effort: if
`/v1/tasks` fails the table still prints.

## `hop agents [id]`

Without an id:

```
ID          ENDPOINT                   VERSION  TEMP    LAST SEEN
pi-kitchen  http://192.168.1.20:8080   3.0.11   48.0°C  12:00:05
```

`TEMP` is `-` without a sensor; `LAST SEEN` is the UTC clock time of the last
heartbeat.

With an id: the agent's identity and, through the leader
(`/v1/agents/{id}/capacity`), its capacity:

```
Agent:    pi-kitchen
Endpoint: http://192.168.1.20:8080
Version:  3.0.11
CPU temp: 48.0°C
LastSeen: 12:00:05

Tasks:    3 running
CPU:      1.5 / 4 cores (1536 / 4096 shares)
Memory:   0.8 / 4 GB

Attributes:
  zone = kitchen
```

If the capacity cannot be fetched, it prints `Capacity: (unavailable - <reason>)`
and still exits 0. An unknown id is an error (`agent <id> not found`).

## `hop logs`

```
hop logs [--stream stdout|stderr] [-f|--follow] <job|task>
```

The target is a job name or a task id, looked up in `GET /v1/tasks`.

- Without `-f`: the retained lines (default the last 50) of every matching
  task, through the leader with `?follow=0`. With more than one task each
  block starts with `== <task> on <agent> ==`.
- With `-f` / `--follow`: follows exactly one task live
  (`?follow=1`) until the task stops or the stream breaks. A job with several
  tasks is refused with the list of task ids to pick from.
- `--stream` (also `--stream=stderr`) selects the stream; default `stdout`.

```sh
hop logs sleeper
hop logs -f --stream stderr web-1a2b
```

No matching task: `no task of job or with id <x> on any agent`. A log route
counts as a stream on the leader; when too many are open the leader answers
`503 too many open streams` (see [api.md](api.md#stream-limits)).

## `hop events`

Follows `GET /v1/events` and prints one line per event, `<kind> <data>`,
until the leader ends the stream (it no longer leads) or Ctrl-C:

```
ping {}
job {"name":"web"}
task {"job":"web","event":"started"}
agent {"id":"pi-kitchen"}
status {}
```

Event types are listed in [api.md](api.md#get-v1events-sse).

## `hop delete <job>`

Deletes the job and all its tasks: `Job deleted (<job>)`.

## `hop flip <url> <sha256> [--cold]`

Asks the HopOS node at `--agent` (`HOP_AGENT`) to fetch, verify and replace
its kernel with the flip bundle at `url` (`POST /flip`). `sha256` must be 64
hex characters (checked locally). `--cold` stops the node's tasks first and
starts Hop cold: the path for a kernel the warm flip refuses. The answer is
`202` with a status line, printed as `<agent>: <status>`; the flip itself is
not confirmed by it. A host `agentd` answers `501`. See [hopos.md](hopos.md).

```sh
hop flip --agent 192.168.1.30:8080 http://192.168.1.10/hopos-o6n-headless.flip \
  "$(cut -d' ' -f1 hopos-o6n-headless.flip.sha256)"
```

## `hop image`: the HopOS imager

`hop image` reads and rewrites the node config (`hopos.cfg`) inside a HopOS
kernel, without a filesystem and without the network. Source:
`cli/src/image.rs`. The keys of the config are in [hopos.md](hopos.md) and
[configuration.md](configuration.md).

```
hop image <file|device>                         show the config in the window
hop image <file|device> --config <cfg>          put a config in the window, in place
hop image <image> --config <cfg> --write <dev>  write a card or stick with that config, and verify
hop image <image> --keep --write <dev>          the same, keeping the config already on <dev>
```

| Flag | Meaning |
| --- | --- |
| `--config <cfg>` | the config file to put in the window |
| `--write <dev>` | write the (configured) image to `<dev>` and read it back to verify |
| `--keep` | with `--write`: take the config from the window already on `<dev>` |
| `--force` | write a `/dev/...` device that does not look removable |
| `--help`, `-h` | usage |

Flags accept `--flag value`, `--flag=value` or a single dash. `--keep` and
`--config` exclude each other; `--keep` needs `--write`. Exactly one target.

**Targets**: a HopOS disk image (`.img`, unpacked: a `.gz` is refused for
`--write`), a flip bundle (`.flip`), a bare kernel or `fip.bin`, or a block
device. Anything starting with `/dev/` is treated as a device (needs root):
`/dev/rdiskN` on macOS, `/dev/sdX` on Linux (the whole disk, not a
partition).

### The config window

Every HopOS kernel carries a fixed-size config window (HopOS
`board/src/cfgwin.rs`, 16 KiB = 16384 bytes):

```
#HOPCFG1 window=16384 len=0000000432
hopos.node=pi-kitchen
hopos.cluster=home
...
################################################################
################################################################
```

- The head line is `#HOPCFG1 window=<size> len=<10 digits>`; `size` is the
  whole window (a multiple of 512), `len` the number of config bytes after
  the head line (ten fixed digits, so the head line keeps its length).
- The rest is padding: lines of `#` (comments to the kernel), so the whole
  window is a valid config file.
- `hop image` finds the window by its head line, scanning every byte offset
  (in the LicheeRV FIP it is not aligned) in the first 256 MiB, and requires
  **exactly one** window. Otherwise:
  - `no config window found: not a HopOS image with a config window (HopOS v3.1 or later), or not unpacked`
  - `N config windows found (at 0x..., 0x...), refusing`

### Show

```sh
hop image hopos-rpi4-headless.img
```

Prints `<path>: config window of <size> bytes at <offset>, <len> bytes of
config` on stderr and the config itself on stdout, so
`hop image x.img > my-node.cfg` saves it. Works read-only on a card too.

### Set in place (`--config`)

```sh
hop image hopos-rpi4-headless.img --config my-node.cfg
hop image hopos-o6n-headless.flip --config my-node.cfg
```

- The new config replaces the window in place; the window keeps its size. A
  config that is itself a window is stripped to its content first; a missing
  final newline is added.
- Refused: a NUL byte, text that is not UTF-8, or a config that does not fit
  (`a config of N bytes does not fit the 16384-byte window`), because the
  kernel would ignore it.
- For a file: prints the new SHA-256 of the whole file on stdout
  (`<sha256>  <path>`) and, if `<path>.sha256` exists, rewrites it. That is
  the sum `hop flip` needs for a re-configured bundle.
- For a device: the removable check (below) and, on macOS, `diskutil
  unmountDisk` first.

### Write a card (`--write`)

```sh
hop image hopos-rpi4-headless.img --config my-node.cfg --write /dev/rdisk4
hop image hopos-rpi4-headless.img --keep --write /dev/rdisk4
hop image hopos-rpi4-headless.img --write /dev/rdisk4
```

1. Reads the whole image into memory and finds its window (the image file on
   disk is not changed).
2. Picks the config: `--config <cfg>`; with `--keep` the config read from the
   window on `<dev>` (`<dev>: nothing to keep: ...` if it has none); with
   neither, the image's own config.
3. For a device: the removable check, unmount (macOS), and zero padding to a
   whole 512-byte sector.
4. Writes in 4 MiB chunks with progress on stderr, syncs, then reads
   everything back and compares (`verify failed in the MiB at N` on a
   mismatch).
5. Ends with `<dev>: <image> written and verified (<bytes> bytes), config <cfg>`
   (or `the config of the card kept` / `the config of the image`).

A `--write` target that does not start with `/dev/` is created as a regular
file: a configured copy of the image.

**Removable check** (devices only, skipped with `--force`): on macOS
`diskutil info` must report `Device Location: External` or
`Removable Media: Removable`; on Linux `/sys/block/<name>/removable` must be
`1`. Otherwise: `<dev> does not look like a removable card or stick; --force
writes it anyway`.

### Raw devices and the LicheeRV FIP

- All device I/O is read-modify-write on whole 512-byte sectors (a raw
  `/dev/rdiskN` accepts nothing else). `fsync` errors on a raw character
  device are ignored.
- If the image contains a Sophgo FIP (the LicheeRV: a `CVBL01` header and a
  `CVLD02` param2 block) and the window lies inside its MONITOR payload, the
  imager recomputes `MONITOR_CKSUM` (param2 + 48) and `PARAM2_CKSUM`
  (param2 + 8), CRC-16/XMODEM followed by `0xCAFE`, as `fiptool.py` does.
  Without that, the FSBL refuses the kernel. Images without a FIP are left
  alone.

The window format is a contract with HopOS `board/src/cfgwin.rs` and
`image/hopcfg.py`; see [hopos.md](hopos.md).
