#!/usr/bin/env python3
"""The API sequence of the dashboard (app.js), against one Hop agent.

The browser talks to an agent directly, so every call here goes to the
agent port, carries an Origin header and the X-Hop-Auth signature exactly
as app.js computes it with Web Crypto:

    X-Hop-Auth = hex(HMAC-SHA256(key, METHOD + "\\n" + PATH + "\\n" + hex(sha256(body))))

and every answer must carry the CORS headers, or the browser throws it away.

    dashboard-api.py post   --agent URL [--key K] --body JSON
    dashboard-api.py check  --agent URL [--key K] --job NAME [--log-marker M] [--secure]
    dashboard-api.py delete --agent URL [--key K] --job NAME
    dashboard-api.py wait   --agent URL [--key K] --job NAME   (until a task runs)

`check` prints one "ok" line per step and "ROOD" for a failure (exit 1):
the preflight, /leader, /v1/status, /v1/agents, /v1/jobs, the job status
(the task table), the capacity of the agent that runs the task, the first
lines of /v1/events, the first lines of the task's live log (no query: the
dashboard's call), and PATCH .../priority (the drag order). With --secure
an unsigned call must be refused with 401, CORS headers included.
"""

import argparse
import hashlib
import hmac
import http.client
import json
import re
import select
import socket
import sys
import time
import urllib.parse

ORIGIN = "http://localhost:3000"


def sign(key, method, path, body):
    """X-Hop-Auth for one request; the query is not signed (app.js: URL.pathname)."""
    msg = "%s\n%s\n%s" % (method.upper(), path, hashlib.sha256(body).hexdigest())
    return hmac.new(key.encode(), msg.encode(), hashlib.sha256).hexdigest()


class Agent:
    def __init__(self, url, key):
        u = urllib.parse.urlsplit(url)
        self.host, self.port = u.hostname, u.port or 80
        self.key = key

    def headers(self, method, target, body, signed=True):
        h = {"Origin": ORIGIN}
        path = urllib.parse.urlsplit(target).path
        if signed and self.key:
            h["X-Hop-Auth"] = sign(self.key, method, path, body)
        if body:
            h["Content-Type"] = "application/json"
        return h

    def call(self, method, target, body=b"", signed=True, extra=None, timeout=20):
        """One request; (status, headers as a lowercase dict, body bytes)."""
        c = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        h = self.headers(method, target, body, signed)
        h.update(extra or {})
        c.request(method, target, body=body or None, headers=h)
        r = c.getresponse()
        data = r.read()
        hs = {k.lower(): v for k, v in r.getheaders()}
        c.close()
        return r.status, hs, data

    def stream(self, target, until, seconds):
        """The first bytes of an SSE stream: read until `until(text)` or `seconds`.

        A raw socket with select, not http.client: a stream that is quiet
        for a while is normal here, and http.client cannot read again after
        a timeout. The body stays chunk-framed; `until` looks for text that
        one SSE write carries whole.
        """
        h = self.headers("GET", target, b"")
        h["Host"] = "%s:%d" % (self.host, self.port)
        req = "GET %s HTTP/1.1\r\n%s\r\n" % (
            target,
            "".join("%s: %s\r\n" % kv for kv in h.items()),
        )
        sock = socket.create_connection((self.host, self.port), timeout=10)
        sock.sendall(req.encode())
        raw = b""
        end = time.time() + seconds
        status, hs, text = 0, {}, ""
        while time.time() < end:
            ready, _, _ = select.select([sock], [], [], 0.2)
            if ready:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                raw += chunk
            head, sep, body = raw.partition(b"\r\n\r\n")
            if not sep:
                continue
            lines = head.decode(errors="replace").split("\r\n")
            status = int(lines[0].split()[1])
            hs = {k.strip().lower(): v.strip() for k, _, v in (l.partition(":") for l in lines[1:])}
            text = body.decode(errors="replace")
            if status != 200 or until(text):
                break
        sock.close()
        return status, hs, text


class Check:
    def __init__(self):
        self.failed = False

    def ok(self, what, detail=""):
        print("   ok  %s%s" % (what, (": " + detail) if detail else ""))

    def red(self, what, detail=""):
        print("   ROOD %s%s" % (what, (": " + detail) if detail else ""))
        self.failed = True

    def cors(self, what, hs):
        if hs.get("access-control-allow-origin") != "*":
            self.red(what, "no Access-Control-Allow-Origin (headers: %s)" % sorted(hs))
            return False
        return True

    def expect(self, what, got, want, hs, body):
        if got != want:
            self.red(what, "HTTP %s, want %s: %s" % (got, want, body[:300]))
            return False
        return self.cors(what, hs)


def first_lines(text, n=3):
    # Zonder de chunk-groottes van de draad (een regel met alleen hex).
    lines = [l for l in text.splitlines() if l and not re.fullmatch(r"[0-9a-fA-F]+", l)]
    return " | ".join(lines[:n])


def check(a, job, marker, secure):
    t = Check()
    # De preflight: geen handtekening, wel de methodes en koppen van app.js.
    s, hs, b = a.call(
        "OPTIONS",
        "/v1/jobs",
        signed=False,
        extra={
            "Access-Control-Request-Method": "PATCH",
            "Access-Control-Request-Headers": "content-type,x-hop-auth",
        },
    )
    methods = hs.get("access-control-allow-methods", "")
    allowed = hs.get("access-control-allow-headers", "")
    if (
        t.expect("OPTIONS /v1/jobs", s, 200, hs, b)
        and "PATCH" in methods
        and "DELETE" in methods
        and "X-Hop-Auth" in allowed
        and "Content-Type" in allowed
    ):
        t.ok("OPTIONS /v1/jobs", "HTTP 200, Allow-Methods: %s; Allow-Headers: %s" % (methods, allowed))
    elif s == 200:
        t.red("OPTIONS /v1/jobs", "methods %r headers %r" % (methods, allowed))

    s, hs, b = a.call("GET", "/leader")
    if t.expect("GET /leader", s, 200, hs, b):
        t.ok("GET /leader", b.decode().strip())

    s, hs, b = a.call("GET", "/v1/status")
    if t.expect("GET /v1/status", s, 200, hs, b):
        st = json.loads(b)
        missing = [k for k in ("cluster_name", "agents", "jobs", "total_placed", "settling", "placed") if k not in st]
        if missing:
            t.red("GET /v1/status", "missing %s" % missing)
        else:
            t.ok("GET /v1/status", b.decode().strip())

    s, hs, b = a.call("GET", "/v1/agents")
    agents = []
    if t.expect("GET /v1/agents", s, 200, hs, b):
        agents = json.loads(b)
        t.ok("GET /v1/agents", ", ".join("%s %s" % (x.get("id"), x.get("endpoint")) for x in agents))

    s, hs, b = a.call("GET", "/v1/jobs")
    if t.expect("GET /v1/jobs", s, 200, hs, b):
        jobs = json.loads(b)
        names = [j.get("name") for j in jobs]
        if job in names:
            t.ok("GET /v1/jobs", ", ".join("%s (priority %s)" % (j.get("name"), j.get("priority")) for j in jobs))
        else:
            t.red("GET /v1/jobs", "%s not in %s" % (job, names))

    s, hs, b = a.call("GET", "/v1/jobs/%s/status" % job)
    agent_id, task = None, None
    if t.expect("GET /v1/jobs/%s/status" % job, s, 200, hs, b):
        js = json.loads(b)
        for aid, tasks in (js.get("tasks_by_agent") or {}).items():
            for tk in tasks:
                agent_id, task = aid, tk
        if task:
            t.ok(
                "GET /v1/jobs/%s/status" % job,
                "task %s on %s, state %s, ports %s" % (task.get("id"), agent_id, task.get("state"), task.get("ports")),
            )
        else:
            t.red("GET /v1/jobs/%s/status" % job, "no task in %s" % b[:300])
    if agent_id is None and agents:
        agent_id = agents[0].get("id")

    if agent_id:
        s, hs, b = a.call("GET", "/v1/agents/%s/capacity" % agent_id)
        if t.expect("GET /v1/agents/%s/capacity" % agent_id, s, 200, hs, b):
            cap = json.loads(b)
            missing = [
                k
                for k in ("cpu_cores", "memory_bytes", "cpu_used_shares", "memory_used_bytes", "tasks_running")
                if k not in cap
            ]
            if missing:
                t.red("GET /v1/agents/%s/capacity" % agent_id, "missing %s" % missing)
            else:
                t.ok("GET /v1/agents/%s/capacity" % agent_id, b.decode().strip())

    s, hs, text = a.stream("/v1/events", lambda x: "event: ping" in x, 5)
    if t.expect("GET /v1/events", s, 200, hs, text.encode()):
        if "event: ping" in text and hs.get("content-type") == "text/event-stream":
            t.ok("GET /v1/events", first_lines(text))
        else:
            t.red("GET /v1/events", "no ping in %r" % text[:200])

    if task and agent_id:
        target = "/v1/agents/%s/logs/%s/stdout" % (agent_id, task.get("id"))
        want = (lambda x: marker in x) if marker else (lambda x: "data:" in x)
        s, hs, text = a.stream(target, want, 8)
        if t.expect("GET " + target, s, 200, hs, text.encode()):
            lines = [l for l in text.splitlines() if l.startswith("data:")]
            if want(text):
                t.ok("GET " + target, "%d line(s), first: %s" % (len(lines), lines[0][:160] if lines else ""))
            else:
                t.red("GET " + target, "no %s in %r" % (marker or "data line", text[:300]))

    body = json.dumps({"priority": 0}).encode()
    s, hs, b = a.call("PATCH", "/v1/jobs/%s/priority" % job, body)
    if t.expect("PATCH /v1/jobs/%s/priority" % job, s, 204, hs, b):
        t.ok("PATCH /v1/jobs/%s/priority" % job, "HTTP 204")

    if secure:
        s, hs, b = a.call("GET", "/v1/jobs", signed=False)
        if t.expect("unsigned GET /v1/jobs", s, 401, hs, b):
            t.ok("unsigned GET /v1/jobs", "HTTP 401 %s" % b.decode().strip())
    return 1 if t.failed else 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=("post", "check", "delete", "wait"))
    p.add_argument("--agent", required=True)
    p.add_argument("--key", default="")
    p.add_argument("--job", default="")
    p.add_argument("--body", default="")
    p.add_argument("--log-marker", default="")
    p.add_argument("--secure", action="store_true")
    o = p.parse_args()
    a = Agent(o.agent, o.key)
    t = Check()
    if o.command == "post":
        s, hs, b = a.call("POST", "/v1/jobs", o.body.encode())
        if s in (200, 201) and t.cors("POST /v1/jobs", hs):
            t.ok("POST /v1/jobs", "HTTP %d %s" % (s, b.decode().strip()))
        elif s not in (200, 201):
            t.red("POST /v1/jobs", "HTTP %d %s" % (s, b[:300]))
        return 1 if t.failed else 0
    if o.command == "wait":
        # Zonder stromen: een check opent er twee, en een stroom telt tot de
        # server merkt dat de lezer weg is (de keepalive, 15 s).
        end = time.time() + 30
        while time.time() < end:
            s, hs, b = a.call("GET", "/v1/jobs/%s/status" % o.job)
            if s == 200:
                js = json.loads(b)
                states = [tk.get("state") for ts in (js.get("tasks_by_agent") or {}).values() for tk in ts]
                if "running" in states:
                    t.ok("%s running" % o.job)
                    return 0
            time.sleep(0.3)
        t.red("%s never running" % o.job)
        return 1
    if o.command == "delete":
        s, hs, b = a.call("DELETE", "/v1/jobs/%s" % o.job)
        if t.expect("DELETE /v1/jobs/%s" % o.job, s, 204, hs, b):
            t.ok("DELETE /v1/jobs/%s" % o.job, "HTTP 204")
        return 1 if t.failed else 0
    return check(a, o.job, o.log_marker, o.secure)


if __name__ == "__main__":
    sys.exit(main())
