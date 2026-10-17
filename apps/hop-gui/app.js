const $ = id => document.getElementById(id);
const icon = name => `<span class="material-symbols-outlined t-icon" aria-hidden="true">${name}</span>`;
const spinner = '<span class="t-spinner" aria-hidden="true"></span>';
const notify = message => TactileDialog.notice(message, { title: 'Hop', confirmLabel: 'Close' });


function httpStatusMessage(status) {
    switch (status) {
        case 502: return 'HTTP 502 — agent cannot reach the leader (leader down or election in progress)';
        case 503: return 'HTTP 503 — service unavailable (no leader yet, or the node refused the request)';
        case 401: return 'HTTP 401 — authentication failed (check API key)';
        case 403: return 'HTTP 403 — access denied (check API key)';
        default:  return `HTTP ${status}`;
    }
}

// httpErrorMessage prefers the server's own explanation ({"error": "..."})
// over a guess from the status: a 503 is not always an election — "too many
// open streams" is one too, and the agent says so in the body.
async function httpErrorMessage(resp) {
    try {
        const body = await resp.clone().json();
        if (body && typeof body.error === 'string' && body.error) {
            return `HTTP ${resp.status} — ${body.error}`;
        }
    } catch (e) { /* no JSON body: fall back to the status */ }
    return httpStatusMessage(resp.status);
}

// withDefaultPort completes a bare host/IP with the agent port. A `?ip=10.0.0.5`
// would otherwise resolve to :80, which is never where an agent listens.
function withDefaultPort(addr) {
    if (/^https?:\/\//i.test(addr)) return addr;                        // already a full URL
    if (addr.startsWith('[')) return /\]:\d+$/.test(addr) ? addr : addr + ':8080';
    if ((addr.match(/:/g) || []).length > 1) return `[${addr}]:8080`;    // bare IPv6
    return /:\d+$/.test(addr) ? addr : addr + ':8080';
}

const app = {
    clusterSSE: null, refreshTimer: null, detailTimer: null, fallbackTimer: null,
    logAbort: null, currentTask: null, currentStream: 'stdout',
    activeJobId: null, activeAgentId: null, _skipPush: false, _dragSrcIdx: null,
    agents: [], status: null, jobs: [], capacityByEndpoint: {},
    clusters: [], activeCluster: 0,
    connectedEndpoint: null, _poolEndpoints: [], _poolIdx: 0,

    // ── Cluster config ─────────────────────────────

    getConfiguredEndpoint() {
        const c = this.clusters[this.activeCluster];
        const ep = c ? c.endpoint : 'localhost:8080';
        return ep.startsWith('http') ? ep : 'http://' + ep;
    },

    getEndpoint() { return this.connectedEndpoint || this.getConfiguredEndpoint(); },

    _toHex(buf) {
        return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
    },

    // signHeaders returns the auth header for a request. It signs
    // HMAC-SHA256(key, METHOD\nPATH\nsha256(body)) so the API key never travels
    // on the wire — only the signature does. Must match hop's pkg/httputil
    // scheme exactly. Async because it uses Web Crypto (SubtleCrypto).
    async signHeaders(method, url, body) {
        const key = this.clusters[this.activeCluster]?.apiKey;
        if (!key) return {};
        const enc = new TextEncoder();
        const path = new URL(url).pathname;
        let bodyBytes;
        if (body == null) bodyBytes = new Uint8Array(0);
        else if (typeof body === 'string') bodyBytes = enc.encode(body);
        else bodyBytes = new Uint8Array(body);
        const bodyHash = this._toHex(await crypto.subtle.digest('SHA-256', bodyBytes));
        const msg = `${method.toUpperCase()}\n${path}\n${bodyHash}`;
        const cryptoKey = await crypto.subtle.importKey(
            'raw', enc.encode(key), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
        const sig = await crypto.subtle.sign('HMAC', cryptoKey, enc.encode(msg));
        return { 'X-Hop-Auth': this._toHex(sig) };
    },

    loadClusters() {
        try {
            const stored = localStorage.getItem('hop-clusters');
            if (stored) this.clusters = JSON.parse(stored);
        } catch (e) { /* ignore */ }
        if (!this.clusters.length) this.clusters = [];
        const active = localStorage.getItem('hop-active-cluster');
        this.activeCluster = active !== null ? Math.min(Number(active), this.clusters.length - 1) : 0;
    },

    saveClusters() {
        localStorage.setItem('hop-clusters', JSON.stringify(this.clusters));
        localStorage.setItem('hop-active-cluster', String(this.activeCluster));
    },

    renderClusterMenu() {
        $('clusterMenu').innerHTML = this.clusters.map((c, i) =>
            `<button type="button" class="t-choice" aria-pressed="${i === this.activeCluster}" data-cluster="${i}" title="${this._esc(c.endpoint)}">${icon('dns')}<span class="t-nav-label">${this._esc(c.name)}</span></button>`
        ).join('') || '<span class="cluster-empty">No saved servers yet</span>';
        const cluster = this.clusters[this.activeCluster];
        $('clusterHeading').textContent = cluster?.name || 'Cluster overview';
        $('clusterDescription').textContent = cluster ? `Agents, capacity and workloads at ${cluster.endpoint}.` : 'Connect a cluster to see your agents, capacity and running jobs.';
        $('newJobButton').disabled = !cluster;
        const remove = $('removeClusterButton');
        remove.hidden = !cluster;
        if (cluster) {
            remove.dataset.removeCluster = String(this.activeCluster);
            remove.setAttribute('aria-label', `Remove ${cluster.name} from saved servers`);
        } else delete remove.dataset.removeCluster;
    },

    _resetState() {
        this.closeLogs();
        // Bump the generation so replies from in-flight requests to the OLD
        // cluster are dropped instead of repainting stale data after a switch.
        this._gen = (this._gen || 0) + 1;
        this.agents = [];
        this.status = null;
        this.jobs = [];
        this.capacityByEndpoint = {};
        this.connectedEndpoint = null;
        this._poolIdx = 0;
        this._renderReset();
    },

    // _renderReset immediately paints the cleared state, so switching to an
    // unreachable cluster shows "connecting" instead of the previous cluster's
    // data frozen on screen.
    _renderReset() {
        for (const id of ['agentCount', 'totalPlaced', 'totalJobs', 'leaderAddr']) $(id).textContent = '-';
        $('lastUpdate').textContent = '';
        $('statsContainer').classList.remove('settling');
        $('settleBadge').classList.remove('active');
        $('error').innerHTML = '';
        this.setSseStatus(true); // clear any stale SSE banner; connect will repopulate
        const message = this.clusters.length ? `${spinner}Connecting to cluster…` : 'Add a cluster to get started.';
        document.querySelector('#agentsTable tbody').innerHTML = `<tr><td colspan="8" class="empty">${message}</td></tr>`;
        document.querySelector('#jobsTable tbody').innerHTML = `<tr><td colspan="6" class="empty">${message}</td></tr>`;
        clearInterval(this.detailTimer);
        this.activeJobId = null;
        this.activeAgentId = null;
        $('agentDetailView').classList.add('hidden');
        $('agentsListView').classList.remove('hidden');
        $('jobDetailView').classList.add('hidden');
        $('jobsListView').classList.remove('hidden');
    },

    switchCluster(index) {
        if (index === this.activeCluster) return;
        this.activeCluster = index;
        this.saveClusters();
        this.renderClusterMenu();
        document.querySelector(`[data-cluster="${index}"]`)?.focus({ preventScroll: true });
        this._resetState();
        this.activeJobId = null;
        this._stopFallbackPoll();
        this._loadPool();
        this.connectSSE();
    },

    showAddCluster() { if (!$('clusterForm').open) $('clusterForm').showModal(); $('clusterName').focus(); },

    hideAddCluster() {
        $('clusterForm').close();
        $('clusterName').value = '';
        $('clusterEndpoint').value = '';
        $('clusterApiKey').value = '';
    },

    addClusterFromForm() {
        const name = $('clusterName').value.trim();
        const endpoint = $('clusterEndpoint').value.trim();
        const apiKey = $('clusterApiKey').value.trim();
        if (!name || !endpoint) return;
        const cluster = { name, endpoint };
        if (apiKey) cluster.apiKey = apiKey;
        this.clusters.push(cluster);
        this.activeCluster = this.clusters.length - 1;
        this.saveClusters();
        this.renderClusterMenu();
        this.hideAddCluster();
        this._resetState();
        this._poolEndpoints = [];
        this.connectSSE();
    },

    // prefillFromQuery handles deep links like
    // gui.gethop.org/?name=NODE&ip=IP — a node's setup page can hand over its
    // own address and we open the add-cluster form with it filled in. It is
    // never submitted for the operator, and the API key is deliberately NOT a
    // parameter: keys don't belong in URLs (history, logs, referrers), so that
    // one stays something you paste in yourself. Returns true if it opened.
    prefillFromQuery() {
        const q = new URLSearchParams(location.search);
        const name = (q.get('name') || '').trim();
        const ip = (q.get('ip') || '').trim();
        if (!name && !ip) return false;
        this.showAddCluster();
        $('clusterName').value = name;
        $('clusterEndpoint').value = ip ? withDefaultPort(ip) : '';
        $('clusterApiKey').focus();
        // Drop the params so a reload doesn't re-open the form and invite a duplicate.
        history.replaceState(null, '', location.pathname + location.hash);
        return true;
    },

    async removeCluster(index) {
        const cluster = this.clusters[index];
        if (!cluster || !await TactileDialog.confirm(`Remove cluster "${cluster.name}" from this browser? Its jobs keep running.`, { title: 'Remove cluster?', confirmLabel: 'Remove', cancelLabel: 'Cancel' })) return;
        index = this.clusters.indexOf(cluster);
        if (index < 0) return;
        const wasActive = index === this.activeCluster;
        this.clusters.splice(index, 1);
        if (this.activeCluster >= this.clusters.length) this.activeCluster = Math.max(0, this.clusters.length - 1);
        else if (index < this.activeCluster) this.activeCluster--;
        this.saveClusters();
        this.renderClusterMenu();
        document.querySelector(`[data-cluster="${this.activeCluster}"]`)?.focus({ preventScroll: true });
        if (wasActive) {
            this._resetState();
            this._stopFallbackPoll();
            if (this.clusters.length) { this._loadPool(); this.connectSSE(); }
            else { this.disconnectSSE(); this.setSseStatus(true); this.showAddCluster(); }
        }
    },

    // ── API ────────────────────────────────────────

    async fetchAPI(path, options = {}) {
        const url = this.getEndpoint() + path;
        const method = options.method || 'GET';
        const auth = await this.signHeaders(method, url, options.body);
        const headers = { ...auth, ...(options.headers || {}) };
        const resp = await fetch(url, { ...options, headers });
        if (!resp.ok) throw new Error(await httpErrorMessage(resp));
        return resp.status === 204 ? null : resp.json();
    },

    async fetchAgentCapacity(agentId, endpoint) {
        const gen = this._gen;
        try {
            const url = `${this.getEndpoint()}/v1/agents/${agentId}/capacity`;
            const resp = await fetch(url, { headers: await this.signHeaders('GET', url, null) });
            if (!resp.ok) return;
            const cap = await resp.json();
            if (gen !== this._gen) return; // cluster switched while in flight
            this.capacityByEndpoint[endpoint] = cap;
        } catch (e) { /* failed */ }
    },

    // ── SSE + failover pool ────────────────────────

    _buildEndpointList() {
        const configured = this.getConfiguredEndpoint();
        const seen = new Set([configured]);
        const list = [configured];
        for (const ep of this._poolEndpoints) {
            const norm = ep.startsWith('http') ? ep : 'http://' + ep;
            if (!seen.has(norm)) { seen.add(norm); list.push(norm); }
        }
        return list;
    },

    _updatePool() {
        this._poolEndpoints = this.agents.map(a => a.endpoint).filter(Boolean);
        const c = this.clusters[this.activeCluster];
        if (c) {
            try { localStorage.setItem(`hop-pool-${c.name}`, JSON.stringify(this._poolEndpoints)); }
            catch (e) { /* ignore */ }
        }
    },

    _loadPool() {
        this._poolEndpoints = [];
        const c = this.clusters[this.activeCluster];
        if (!c) return;
        try {
            const stored = localStorage.getItem(`hop-pool-${c.name}`);
            if (stored) this._poolEndpoints = JSON.parse(stored);
        } catch (e) { /* ignore */ }
    },

    _startFallbackPoll() {
        if (this.fallbackTimer) return;
        this.fallbackTimer = setInterval(() => this.refresh(), 10000);
    },

    _stopFallbackPoll() {
        if (this.fallbackTimer) { clearInterval(this.fallbackTimer); this.fallbackTimer = null; }
    },

    _tryNextEndpoint() {
        this._poolIdx++;
        const endpoints = this._buildEndpointList();
        const total = endpoints.length;
        this._startFallbackPoll();
        if (this._poolIdx >= total) {
            this.setSseStatus(false, `All ${total} agent(s) unreachable — retrying in 10s`);
            this._poolIdx = 0;
            setTimeout(() => this.connectSSE(), 10000);
        } else {
            const next = endpoints[this._poolIdx].replace(/^https?:\/\//, '');
            this.setSseStatus(false, `Trying ${next}... (${this._poolIdx + 1}/${total})`);
            setTimeout(() => this.connectSSE(), 1000);
        }
    },

    disconnectSSE() {
        if (this.clusterSSE) { this.clusterSSE.abort(); this.clusterSSE = null; }
    },

    setSseStatus(ok, msg) {
        const el = $('sseStatus');
        if (!el) return;
        el.className = ok ? 'sse-ok' : 'sse-error t-alert';
        el.dataset.tone = 'warning';
        el.textContent = ok ? '' : (msg || 'Live updates disconnected — retrying…');
        const connected = ok && !!this.connectedEndpoint;
        $('connectionState').classList.toggle('connected', connected);
        $('connectionState').innerHTML = connected ? 'Live updates connected' : this.clusters.length ? `${spinner}Connecting to cluster…` : 'No cluster connected';
    },

    connect() { this._loadPool(); this.connectSSE(); },

    async connectSSE() {
        this.disconnectSSE();
        if (!this.clusters.length) return;
        const endpoints = this._buildEndpointList();
        if (!endpoints.length) return;

        const endpoint = endpoints[this._poolIdx % endpoints.length];
        const abort = new AbortController();
        this.clusterSSE = abort;

        try {
            const eventsUrl = endpoint + '/v1/events';
            const resp = await fetch(eventsUrl, {
                headers: await this.signHeaders('GET', eventsUrl, null), signal: abort.signal
            });
            if (!resp.ok || !resp.body) {
                // Agent is reachable but returned an error — don't failover,
                // show the specific error and retry the same endpoint
                this.setSseStatus(false, await httpErrorMessage(resp));
                this._startFallbackPoll();
                setTimeout(() => this.connectSSE(), 10000);
                return;
            }

            this.connectedEndpoint = endpoint;
            this._poolIdx = 0;
            this.setSseStatus(true);
            this._stopFallbackPoll();
            this.refresh();

            const reader = resp.body.getReader();
            const decoder = new TextDecoder();
            let buf = '', event = '';
            while (true) {
                const { done, value } = await reader.read();
                if (done) throw new Error('SSE stream ended');
                buf += decoder.decode(value, { stream: true });
                const lines = buf.split('\n');
                buf = lines.pop();
                for (const line of lines) {
                    if (line.startsWith('event: ')) event = line.slice(7).trim();
                    else if (line.startsWith('data:') && event !== 'ping') {
                        clearTimeout(this.refreshTimer);
                        this.refreshTimer = setTimeout(() => this.refresh(), 500);
                    } else if (line === '') event = '';
                }
            }
        } catch (e) {
            if (!abort.signal.aborted) this._tryNextEndpoint();
        }
    },

    // ── Navigation ─────────────────────────────────

    showTab(name) {
        clearInterval(this.detailTimer);
        this.activeJobId = null;
        this.activeAgentId = null;
        document.querySelectorAll('.tab').forEach(t => {
            const selected = t.dataset.tab === name;
            t.classList.toggle('active', selected);
            t.setAttribute('aria-pressed', String(selected));
        });
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        $(`tab-${name}`).classList.add('active');
        if (name === 'jobs') {
            this.activeJobId = null;
            $('jobDetailView').classList.add('hidden');
            $('jobsListView').classList.remove('hidden');
        } else {
            $('agentDetailView').classList.add('hidden');
            $('agentsListView').classList.remove('hidden');
        }
        if (!this._skipPush) history.pushState(null, '', '#' + name);
    },

    navigateToHash() {
        const hash = location.hash || '#agents';
        this._skipPush = true;
        this.closeLogs();
        if (hash.startsWith('#jobs/')) this.openJobDetail(decodeURIComponent(hash.slice(6)));
        else if (hash.startsWith('#agents/')) this.openAgentDetail(decodeURIComponent(hash.slice(8)));
        else if (hash === '#jobs') this.showTab('jobs');
        else this.showTab('agents');
        this._skipPush = false;
    },

    // ── Data refresh ───────────────────────────────

    async refresh() {
        const gen = this._gen;
        try {
            $('error').innerHTML = '';
            const [status, jobs, agents, leaderInfo] = await Promise.all([
                this.fetchAPI('/v1/status'),
                this.fetchAPI('/v1/jobs'),
                this.fetchAPI('/v1/agents'),
                this.fetchAPI('/leader')
            ]);
            if (gen !== this._gen) return; // cluster switched while in flight — drop stale reply

            this.agents = agents;
            this.status = status;
            this.jobs = jobs;
            this._updatePool();

            $('agentCount').textContent = status.agents;
            $('totalPlaced').textContent = status.total_placed;
            $('totalJobs').textContent = status.jobs;
            $('leaderAddr').textContent = leaderInfo.leader || 'unknown';
            $('statsContainer').classList.toggle('settling', !!status.settling);
            $('settleBadge').classList.toggle('active', !!status.settling);

            for (const a of agents) this.fetchAgentCapacity(a.id, a.endpoint).then(() => {
                if (gen === this._gen) this.renderAgentsTable();
            });
            this.renderAgentsTable();
            this.renderJobsTable(jobs, status.placed || {}, status.agents);
            if (this.activeJobId) this.refreshJobDetail(this.activeJobId);
            if (this.activeAgentId) this.refreshAgentDetail(this.activeAgentId);

            $('lastUpdate').textContent = new Date().toLocaleTimeString();
        } catch (err) {
            if (gen !== this._gen) return; // stale failure from a previous cluster
            $('error').innerHTML = `<div class="warning t-alert" data-tone="warning">Waiting for cluster... (${this._esc(err.message)})</div>`;
        }
    },

    // ── Agents table ───────────────────────────────

    // meter renders used/total as a small bar + text (text always shown, color never alone)
    meter(used, total, text) {
        if (!total || used == null || isNaN(used)) return text;
        const pct = Math.min(100, (used / total) * 100);
        const sev = pct >= 90 ? ' crit' : pct >= 75 ? ' warn' : '';
        return `<span class="usage"><span class="meter t-progress${sev}"><span class="meter-fill" style="width:${pct.toFixed(1)}%"></span></span><span class="meter-text">${text}</span></span>`;
    },

    // taskStateLabel toont de startfase-voortgang achter de state: tijdens
    // "downloading" het percentage uit downloaded_bytes/image_size_bytes, of
    // alleen de bytes als de image-maat onbekend is — geen nep-percentage.
    taskStateLabel(t) {
        if (t.state !== 'downloading' || !t.downloaded_bytes) return t.state;
        if (t.image_size_bytes) {
            const pct = Math.min(100, Math.round((t.downloaded_bytes / t.image_size_bytes) * 100));
            return `${t.state} ${pct}%`;
        }
        return `${t.state} ${this.formatBytes(t.downloaded_bytes)}`;
    },

    // formatTemp toont de node-temperatuur uit de heartbeat (temp_milli_c,
    // milligraden — zelfde formaat als `hop agents`); 0/afwezig = geen sensor,
    // en dan hoort er een streepje te staan — geen nep-nul. Kleur is extra,
    // nooit de enige drager (zelfde principe als meter()).
    formatTemp(milliC) {
        if (!milliC) return '-';
        const c = milliC / 1000;
        const sev = c >= 85 ? ' crit' : c >= 70 ? ' warn' : '';
        return `<span class="temp${sev}">${c.toFixed(1)}°C</span>`;
    },

    // formatSystem tekent wat de kern en Hop zelf op een node gebruiken (uit
    // /v1/agents) in twee regels met de meter van de gui: de cpu van de kern
    // als balk met die van Hop als tekst, en het geheugen van de kern tegen
    // zijn RAM ("30 MB/40 MB") met dat van Hop erbij. Een veld zonder meting
    // ontbreekt (een oude agent): dan een streepje, zoals bij formatTemp.
    formatSystem(a) {
        const pct = v => `${Math.round(v)}%`;
        const hop = (v, fmt) => v != null ? ` · hop ${fmt(v)}` : '';
        const ram = a.kern_ram_bytes ? '/' + this.formatBytes(a.kern_ram_bytes) : '';
        const cpu = a.kern_cpu_percent == null ? '-' : this.meter(a.kern_cpu_percent, 100,
            `kern ${pct(a.kern_cpu_percent)}${hop(a.hop_cpu_percent, pct)}`);
        const mem = a.kern_mem_bytes == null ? '-' : this.meter(a.kern_mem_bytes, a.kern_ram_bytes,
            `${this.formatBytes(a.kern_mem_bytes)}${ram}${hop(a.hop_mem_bytes, b => this.formatBytes(b))}`);
        return cpu === '-' && mem === '-' ? '-' : `<div class="system-usage">${cpu}${mem}</div>`;
    },

    renderAgentsTable() {
        const tbody = document.querySelector('#agentsTable tbody');
        if (!this.agents.length) { tbody.innerHTML = '<tr><td colspan="8" class="empty">No agents</td></tr>'; return; }
        tbody.innerHTML = [...this.agents].sort((a, b) => a.id.localeCompare(b.id)).map(a => {
            const cap = this.capacityByEndpoint[a.endpoint];
            const cpu = cap ? this.meter(cap.cpu_used_shares, cap.cpu_cores * 1024,
                `${(cap.cpu_used_shares / 1024).toFixed(1)}/${cap.cpu_cores}`) : '-';
            const mem = cap ? this.meter(cap.memory_used_bytes, cap.memory_bytes,
                `${this.formatBytes(cap.memory_used_bytes)}/${this.formatBytes(cap.memory_bytes)}`) : '-';
            const tooltip = cap ? this.formatAttributes(cap.attributes) : '';
            const conn = a.endpoint === this.connectedEndpoint;
            return `<tr class="clickable" data-agent-id="${this._esc(a.id)}">
                <td data-label="ID"><button type="button" class="job-link t-choice" data-open-agent="${this._esc(a.id)}"${tooltip ? ` title="${this._esc(tooltip)}" data-tooltip="${this._esc(tooltip)}"` : ''}>${this._esc(a.id)}</button></td>
                <td data-label="Version"><span class="version">${this._esc(a.version || 'unknown')}</span></td>
                <td data-label="Endpoint"><code>${this._esc(a.endpoint)}</code>${conn ? ' <span class="connected-dot">●</span>' : ''}</td>
                <td data-label="CPU">${cpu}</td>
                <td data-label="Memory">${mem}</td>
                <td data-label="System">${this.formatSystem(a)}</td>
                <td data-label="Temp">${this.formatTemp(a.temp_milli_c)}</td>
                <td data-label="Tasks">${cap ? cap.tasks_running : '-'}</td>
            </tr>`;
        }).join('');
    },

    // flatTasks maakt van tasks_by_agent één lijst, per agent gesorteerd; de
    // systeemtaken (kern en Hop, state "system") staan bovenaan bij hun agent,
    // op pid: eerst de kern (0), dan Hop (1). De rest op core en dan op naam,
    // zodat taken die een core delen onder elkaar staan (zonder core achteraan).
    // memLimit is de noemer van het
    // geheugenprocent: memory_limit van de taak, of voor kern en Hop hun RAM
    // uit /v1/agents.
    flatTasks(byAgent) {
        const tasks = [];
        for (const [agentId, agentTasks] of Object.entries(byAgent || {})) {
            const agent = this.agents.find(a => a.id === agentId);
            const sysRam = { kern: agent?.kern_ram_bytes, hop: agent?.hop_ram_bytes };
            for (const t of agentTasks) tasks.push({ ...t, agentId, agentEndpoint: agent?.endpoint,
                memLimit: t.state === 'system' ? sysRam[t.job_name] : t.memory_limit });
        }
        const rank = t => t.state === 'system' ? (t.pid || 0) : Infinity;
        const core = t => t.core ?? Infinity;
        return tasks.sort((a, b) => a.agentId.localeCompare(b.agentId) || rank(a) - rank(b)
            || core(a) - core(b) || (a.job_name || '').localeCompare(b.job_name || '') || a.id.localeCompare(b.id));
    },

    // taskCore: de core waarop de taak draait (0 is de OS-core); met meer cores
    // het aantal extra erachter, "1 +2" voor de cores 1 tot en met 3. Zonder core "-".
    taskCore(t) {
        if (t.core == null) return '-';
        return t.cores > 1 ? `${t.core} +${t.cores - 1}` : String(t.core);
    },

    // taskCpu en taskMem: het procent met zijn noemer erbij, "1.0% of 1 core"
    // en "0.4% of 32 MB"; zonder bekende noemer (een oude agent) alleen het procent.
    taskCpu(t) {
        const of = t.cores ? ` of ${t.cores} core${t.cores === 1 ? '' : 's'}` : '';
        return this.meter(t.cpu_percent, 100, this.formatPercent(t.cpu_percent) + of);
    },

    taskMem(t) {
        const of = t.memLimit ? ` of ${this.formatBytes(t.memLimit)}` : '';
        return this.meter(t.mem_percent, 100, this.formatPercent(t.mem_percent) + of);
    },

    // taskCells: de cellen van een taakrij vanaf Core, gedeeld door de takenlijst
    // van een job en die van een agent. Een systeemtaak heeft geen logs.
    taskCells(t) {
        return `<td data-label="Core" class="task-core">${this._esc(this.taskCore(t))}</td>
                    <td data-label="CPU" class="task-cpu">${this.taskCpu(t)}</td>
                    <td data-label="Mem" class="task-mem">${this.taskMem(t)}</td>
                    <td data-label="Restarts" class="task-restarts">${t.restart_count || 0}</td>
                    <td data-label="State"><span class="t-tag t-supplement status task-state ${this._esc(t.state)}">${this._statusContent(this.taskStateLabel(t))}</span></td>
                    <td class="mobile-actions">${t.state === 'system' ? '' : `<button type="button" class="t-action" data-log-task="${this._esc(t.id)}" data-log-agent="${this._esc(t.agentId)}" data-log-endpoint="${this._esc(t.agentEndpoint || '')}">${icon('terminal')}Logs</button>`}</td>`;
    },

    // ── Agent detail ───────────────────────────────

    // Een agent openen gaat zoals een job openen: de lijst wijkt voor zijn
    // taken, die het detailritme (5 s) en refresh() bijhouden.
    async openAgentDetail(agentId) {
        clearInterval(this.detailTimer);
        this.activeJobId = null;
        this.activeAgentId = agentId;
        document.querySelectorAll('.tab').forEach(t => {
            const selected = t.dataset.tab === 'agents';
            t.classList.toggle('active', selected);
            t.setAttribute('aria-pressed', String(selected));
        });
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        $('tab-agents').classList.add('active');
        $('agentsListView').classList.add('hidden');
        $('agentDetailView').classList.remove('hidden');
        $('agentDetailName').textContent = agentId;
        if (!this._skipPush) history.pushState(null, '', '#agents/' + encodeURIComponent(agentId));

        document.querySelector('#agentTasksTable tbody').innerHTML = `<tr><td colspan="9" class="empty">${spinner}Loading tasks…</td></tr>`;
        await this.refreshAgentDetail(agentId);
        if (this.activeAgentId !== agentId) return;

        clearInterval(this.detailTimer);
        this.detailTimer = setInterval(() => { if (this.activeAgentId) this.refreshAgentDetail(this.activeAgentId); }, 5000);
    },

    // refreshAgentDetail haalt de taken van het cluster (/v1/tasks) en toont die
    // van deze agent: eerst kern en Hop (state "system"), dan de rest.
    async refreshAgentDetail(agentId) {
        const gen = this._gen;
        const isCurrent = () => gen === this._gen && this.activeAgentId === agentId;
        const agent = this.agents.find(a => a.id === agentId);
        const tags = agent ? [`version: ${agent.version || 'unknown'}`, agent.endpoint] : [];
        $('agentDetailInfo').innerHTML = tags.length ? `<div class="detail-tags">${tags.map(t => `<span class="detail-tag t-tag t-supplement">${this._esc(t)}</span>`).join(' ')}</div>` : '';
        const tbody = document.querySelector('#agentTasksTable tbody');
        try {
            const reply = await this.fetchAPI('/v1/tasks');
            if (!isCurrent()) return;
            if (reply?.unreachable?.includes(agentId)) {
                tbody.innerHTML = '<tr><td colspan="9" class="empty">The agent did not answer</td></tr>';
                return;
            }
            const tasks = this.flatTasks({ [agentId]: reply?.tasks_by_agent?.[agentId] || [] });
            tbody.innerHTML = tasks.length ? tasks.map(t => `<tr>
                    <td data-label="Task"><code>${this._esc(t.id.slice(0, 8))}</code></td>
                    <td data-label="Job"><code>${this._esc(t.job_name)}</code></td>
                    <td data-label="Ports">${this._esc(this.formatPorts(t.ports))}</td>
                    ${this.taskCells(t)}
                </tr>`).join('') : '<tr><td colspan="9" class="empty">No tasks</td></tr>';
        } catch (err) {
            if (!isCurrent()) return;
            tbody.innerHTML = `<tr><td colspan="9" class="empty">Failed to load tasks: ${this._esc(err.message)}</td></tr>`;
        }
    },

    closeAgentDetail() {
        clearInterval(this.detailTimer);
        this.activeAgentId = null;
        $('agentDetailView').classList.add('hidden');
        $('agentsListView').classList.remove('hidden');
        if (!this._skipPush) history.pushState(null, '', '#agents');
    },

    // ── Jobs table ─────────────────────────────────

    sortedByPriority(jobs) {
        return [...jobs].sort((a, b) => {
            const pa = a.priority != null ? a.priority : Infinity;
            const pb = b.priority != null ? b.priority : Infinity;
            return pa !== pb ? pa - pb : a.name.localeCompare(b.name);
        });
    },

    renderJobsTable(jobs, placedPerJob, agentCount) {
        const tbody = document.querySelector('#jobsTable tbody');
        const sorted = this.sortedByPriority(jobs);
        if (!sorted.length) { tbody.innerHTML = '<tr><td colspan="6" class="empty">No jobs</td></tr>'; return; }
        tbody.innerHTML = sorted.map((job, idx) => {
            const expected = job.count === -1 ? agentCount : (job.count || 1);
            const running = placedPerJob[job.name] || 0;
            const ok = running >= expected;
            const prio = job.priority != null ? `<span class="prio-badge">${job.priority}</span>` : '<span class="prio-badge">—</span>';
            const tip = this.formatJobTooltip(job);
            return `<tr class="clickable" draggable="true" data-job-id="${this._esc(job.name)}" data-drag-idx="${idx}"
                ondragstart="app.onDragStart(event,${idx})" ondragover="app.onDragOver(event)"
                ondragleave="app.onDragLeave(event)" ondrop="app.onDrop(event,${idx})" ondragend="app.onDragEnd(event)">
                <td class="mobile-hide"><span class="drag-handle" title="Drag to change priority">${icon('drag_indicator')}</span></td>
                <td data-label="Prio">${prio}</td>
                <td data-label="Name"><button type="button" class="job-link t-choice" data-open-job="${this._esc(job.name)}"${tip ? ` title="${this._esc(tip)}"` : ''}>${this._esc(job.name)}</button></td>
                <td data-label="Running">${running} / ${job.count === -1 ? 'all(' + expected + ')' : expected}</td>
                <td data-label="Status"><span class="t-tag t-supplement status ${ok ? 'running' : 'failed'}">${this._statusContent(ok ? 'OK' : 'DEGRADED')}</span></td>
                <td class="mobile-actions"><button type="button" class="t-action t-action--danger" data-delete-job="${this._esc(job.name)}">${icon('delete')}Delete</button></td>
            </tr>`;
        }).join('');
    },

    toggleNewJob() {
        if (!$('newJobForm').open && !this.clusters.length) { this.showAddCluster(); return; }
        if ($('newJobForm').open) $('newJobForm').close();
        else { $('newJobForm').showModal(); $('jobJson').focus(); }
    },

    async startJob() {
        const jsonStr = $('jobJson').value.trim();
        if (!jsonStr) { await notify('Enter job JSON'); return; }
        let job;
        try { job = JSON.parse(jsonStr); }
        catch (e) { await notify('Invalid JSON: ' + e.message); return; }
        try {
            await this.fetchAPI('/v1/jobs', {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(job)
            });
            $('jobJson').value = '';
            $('newJobForm').close();
            this.refresh();
        } catch (err) { await notify('Failed to start job: ' + err.message); }
    },

    async redeployJob(jobName) {
        if (!await TactileDialog.confirm(`Redeploy job ${jobName}? This triggers a rolling update.`, { title: 'Redeploy job?', tone: 'info', confirmLabel: 'Redeploy', cancelLabel: 'Cancel' })) return;
        try {
            const jobs = await this.fetchAPI('/v1/jobs');
            const job = jobs.find(j => j.name === jobName);
            if (!job) throw new Error('Job not found');
            await this.fetchAPI('/v1/jobs', {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(job)
            });
            this.refresh();
        } catch (err) { await notify('Failed to redeploy: ' + err.message); }
    },

    async deleteJob(jobName) {
        if (!await TactileDialog.confirm(`Delete job ${jobName}?`, { title: 'Delete job?', confirmLabel: 'Delete', cancelLabel: 'Cancel' })) return;
        try {
            await this.fetchAPI(`/v1/jobs/${jobName}`, { method: 'DELETE' });
            if (this.activeJobId === jobName) this.closeJobDetail();
            this.refresh();
        } catch (err) { await notify('Failed to delete job: ' + err.message); }
    },

    // ── Drag & drop priority ───────────────────────

    onDragStart(e, idx) {
        this._dragSrcIdx = idx;
        e.currentTarget.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
    },
    onDragOver(e) { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; e.currentTarget.classList.add('drag-over'); },
    onDragLeave(e) { e.currentTarget.classList.remove('drag-over'); },
    onDragEnd(e) {
        e.currentTarget.classList.remove('dragging');
        document.querySelectorAll('tr.drag-over').forEach(r => r.classList.remove('drag-over'));
        this._dragSrcIdx = null;
    },

    async onDrop(e, toIdx) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');
        const fromIdx = this._dragSrcIdx;
        if (fromIdx === null || fromIdx === toIdx) return;

        const sorted = this.sortedByPriority(this.jobs);
        const [moved] = sorted.splice(fromIdx, 1);
        sorted.splice(toIdx, 0, moved);
        sorted.forEach((job, i) => { job.priority = i; });

        this.renderJobsTable(this.jobs, this.status?.placed || {}, this.agents.length);

        try {
            await this.fetchAPI(`/v1/jobs/${moved.name}/priority`, {
                method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ priority: toIdx }),
            });
        } catch (err) { console.error('Priority update failed:', err); this.refresh(); }
    },

    // ── Job detail ─────────────────────────────────

    async openJobDetail(jobId) {
        this.activeAgentId = null;
        this.activeJobId = jobId;
        document.querySelectorAll('.tab').forEach(t => {
            const selected = t.dataset.tab === 'jobs';
            t.classList.toggle('active', selected);
            t.setAttribute('aria-pressed', String(selected));
        });
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        $('tab-jobs').classList.add('active');
        $('jobsListView').classList.add('hidden');
        $('jobDetailView').classList.remove('hidden');
        $('jobDetailName').textContent = jobId;
        $('jobDetailDelete').onclick = () => this.deleteJob(jobId);
        $('jobDetailRedeploy').onclick = () => this.redeployJob(jobId);
        if (!this._skipPush) history.pushState(null, '', '#jobs/' + encodeURIComponent(jobId));

        document.querySelector('#jobTasksTable tbody').innerHTML = `<tr><td colspan="9" class="empty">${spinner}Loading tasks…</td></tr>`;
        await this.refreshJobDetail(jobId);
        if (this.activeJobId !== jobId) return;

        clearInterval(this.detailTimer);
        this.detailTimer = setInterval(() => { if (this.activeJobId) this.refreshJobDetail(this.activeJobId); }, 5000);
    },

    _renderJobInfo(job) {
        const tags = [];
        if (job.image) tags.push(`image: ${job.image}`);
        if (job.driver) tags.push(job.driver);
        tags.push(`count: ${job.count === -1 ? 'all agents' : (job.count || 1)}`);
        if (job.update_policy) tags.push(`update: ${job.update_policy}`);
        if (job.cpu_shares) tags.push(`cpu: ${job.cpu_shares}`);
        if (job.memory_limit) tags.push(`mem: ${this.formatBytes(job.memory_limit)}`);
        tags.push(`ports: ${this.formatPorts(job.ports)}`);
        if (job.max_restarts != null) tags.push(`restarts: ${job.max_restarts === -1 ? '∞' : job.max_restarts}`);
        if (job.tags) for (const [k, v] of Object.entries(job.tags)) tags.push(`${k}=${v}`);
        if (job.affinity) for (const [k, v] of Object.entries(job.affinity)) tags.push(`affinity: ${k}=${v}`);

        let html = `<div class="detail-tags">${tags.map(t => `<span class="detail-tag t-tag t-supplement">${this._esc(t)}</span>`).join(' ')}</div>`;

        if (job.command) {
            html += `<details class="detail-section t-disclosure t-card"><summary>Command</summary><pre class="detail-pre">${this._esc(job.command)}</pre></details>`;
        }
        if (job.artifacts?.length) {
            const rows = job.artifacts.map(a => {
                const match = a.match ? Object.entries(a.match).map(([k,v]) => `${k}=${v}`).join(', ') : '';
                return `<tr><td data-label="URL"><code>${this._esc(a.url)}</code></td><td data-label="Match">${this._esc(match)}</td><td data-label="Filename">${this._esc(a.filename || '')}</td><td data-label="Extract">${this._esc(a.extract || 'binary')}</td></tr>`;
            }).join('');
            html += `<details class="detail-section t-disclosure t-card" open><summary>Artifacts</summary><table class="detail-table"><thead><tr><th>URL</th><th>Match</th><th>Filename</th><th>Extract</th></tr></thead><tbody>${rows}</tbody></table></details>`;
        }
        if (job.volumes && Object.keys(job.volumes).length) {
            const rows = Object.entries(job.volumes).map(([h, t]) =>
                `<tr><td data-label="Host"><code>${this._esc(h)}</code></td><td data-label="Task"><code>${this._esc(t)}</code></td></tr>`
            ).join('');
            html += `<details class="detail-section t-disclosure t-card" open><summary>Volumes</summary><table class="detail-table"><thead><tr><th>Host Path</th><th>Task Path</th></tr></thead><tbody>${rows}</tbody></table></details>`;
        }
        if (job.env && Object.keys(job.env).length) {
            const entries = Object.entries(job.env).sort();
            const rows = entries.map(([k, v]) =>
                `<tr><td data-label="Key"><code>${this._esc(k)}</code></td><td data-label="Value"><code>${this._esc(v)}</code></td></tr>`
            ).join('');
            html += `<details class="detail-section t-disclosure t-card"><summary>Environment (${entries.length})</summary><table class="detail-table"><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>${rows}</tbody></table></details>`;
        }
        return html;
    },

    async refreshJobDetail(jobId) {
        const gen = this._gen;
        const isCurrent = () => gen === this._gen && this.activeJobId === jobId;
        let job = this.jobs.find(j => j.name === jobId);
        if (!job) {
            // Deep link on a fresh page: jobs aren't fetched yet. Load them
            // before concluding the job is gone (closing would bounce the
            // user back to the list on every direct #jobs/<name> visit).
            try {
                const jobs = await this.fetchAPI('/v1/jobs');
                if (!isCurrent()) return; // view or cluster changed mid-flight
                this.jobs = jobs;
            } catch (e) { return; } // unreachable — keep "Loading…", refresh() retries
            job = this.jobs.find(j => j.name === jobId);
            if (!job) { this.closeJobDetail(); return; }
        }

        // Render info, preserving open/closed state of <details>
        const info = $('jobDetailInfo');
        const openState = {};
        info.querySelectorAll('details').forEach(d => {
            const key = d.querySelector('summary')?.textContent;
            if (key) openState[key] = d.open;
        });
        info.innerHTML = this._renderJobInfo(job);
        info.querySelectorAll('details').forEach(d => {
            const key = d.querySelector('summary')?.textContent;
            if (key && key in openState) d.open = openState[key];
        });

        // Status badge
        const placed = this.status?.placed || {};
        const expected = job.count === -1 ? (this.status?.agents || 0) : (job.count || 1);
        const running = placed[job.name] || 0;
        const ok = running >= expected;
        $('jobDetailStatus').innerHTML = this._statusContent(`${running}/${expected}`);
        $('jobDetailStatus').className = 't-tag t-supplement status ' + (ok ? 'running' : 'failed');

        // Tasks
        try {
            const js = await this.fetchAPI(`/v1/jobs/${jobId}/status`);
            if (!isCurrent()) return;
            const tasks = this.flatTasks(js?.tasks_by_agent);

            const tbody = document.querySelector('#jobTasksTable tbody');
            const existing = {};
            tbody.querySelectorAll('tr[data-task-id]').forEach(r => { existing[r.dataset.taskId] = r; });

            if (tasks.length && tasks.length === Object.keys(existing).length && tasks.every(t => existing[t.id])) {
                for (const t of tasks) {
                    const row = existing[t.id];
                    row.querySelector('.task-core').textContent = this.taskCore(t);
                    row.querySelector('.task-cpu').innerHTML = this.taskCpu(t);
                    row.querySelector('.task-mem').innerHTML = this.taskMem(t);
                    row.querySelector('.task-restarts').textContent = t.restart_count || 0;
                    const s = row.querySelector('.task-state');
                    s.className = 't-tag t-supplement status task-state ' + t.state;
                    s.innerHTML = this._statusContent(this.taskStateLabel(t));
                }
            } else {
                tbody.innerHTML = tasks.length ? tasks.map(t => `<tr data-task-id="${this._esc(t.id)}">
                    <td data-label="Task"><code>${this._esc(t.id.slice(0, 8))}</code></td>
                    <td data-label="Agent"><code>${this._esc(t.agentId)}</code></td>
                    <td data-label="Ports">${this._esc(this.formatPorts(t.ports))}</td>
                    ${this.taskCells(t)}
                </tr>`).join('') : '<tr><td colspan="9" class="empty">No tasks</td></tr>';
            }
        } catch (err) {
            if (!isCurrent()) return;
            document.querySelector('#jobTasksTable tbody').innerHTML =
                `<tr><td colspan="9" class="empty">Failed to load tasks: ${this._esc(err.message)}</td></tr>`;
        }
    },

    closeJobDetail() {
        clearInterval(this.detailTimer);
        this.activeJobId = null;
        $('jobDetailView').classList.add('hidden');
        $('jobsListView').classList.remove('hidden');
        if (!this._skipPush) history.pushState(null, '', '#jobs');
    },

    // ── Log viewer ─────────────────────────────────

    openLogs(taskId, agentId, agentEndpoint) {
        this.currentTask = { taskId, agentId, agentEndpoint };
        this.currentStream = 'stdout';
        $('logTaskId').textContent = taskId.slice(0, 8);
        $('logOutput').textContent = '';
        if (!$('logModal').open) $('logModal').showModal();
        $('btnStdout').classList.add('active');
        $('btnStderr').classList.remove('active');
        $('btnStdout').setAttribute('aria-pressed', 'true');
        $('btnStderr').setAttribute('aria-pressed', 'false');
        this.startLogStream();
    },

    switchStream(stream) {
        this.currentStream = stream;
        $('btnStdout').classList.toggle('active', stream === 'stdout');
        $('btnStderr').classList.toggle('active', stream === 'stderr');
        $('btnStdout').setAttribute('aria-pressed', String(stream === 'stdout'));
        $('btnStderr').setAttribute('aria-pressed', String(stream === 'stderr'));
        $('logOutput').textContent = '';
        this.startLogStream();
    },

    async startLogStream() {
        if (this.logAbort) this.logAbort.abort();
        const { taskId, agentId } = this.currentTask;
        const url = `${this.getEndpoint()}/v1/agents/${agentId}/logs/${taskId}/${this.currentStream}`;
        const abort = new AbortController();
        this.logAbort = abort;
        const output = $('logOutput');
        $('logStatus').innerHTML = `${spinner}Connecting…`;
        output.textContent += `Connecting via leader relay...\n`;

        try {
            const resp = await fetch(url, { headers: await this.signHeaders('GET', url, null), signal: abort.signal });
            if (abort.signal.aborted) return;
            if (!resp.ok || !resp.body) { $('logStatus').textContent = 'Connection failed'; output.textContent += `[Error: ${await httpErrorMessage(resp)}]\n`; return; }
            $('logStatus').textContent = 'Live output';
            output.textContent += '[Connected]\n';
            const reader = resp.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });
                const lines = buf.split('\n');
                buf = lines.pop();
                for (const line of lines) {
                    if (line.startsWith('data:')) output.textContent += line.slice(5) + '\n';
                }
                output.scrollTop = output.scrollHeight;
            }
            if (abort.signal.aborted) return;
            $('logStatus').textContent = 'Stream ended';
            output.textContent += '\n[Connection closed]\n';
        } catch (e) {
            if (!abort.signal.aborted) { $('logStatus').textContent = 'Connection failed'; output.textContent += `\n[Error: ${e.message}]\n`; }
        }
    },

    closeLogs() {
        if (this.logAbort) { this.logAbort.abort(); this.logAbort = null; }
        if ($('logModal').open) $('logModal').close();
        this.currentTask = null;
    },

    // ── Formatters ─────────────────────────────────

    formatBytes(bytes) {
        if (bytes == null) return '-';
        if (bytes === 0) return '0';
        for (const [unit, size] of [['GB', 1024 ** 3], ['MB', 1024 ** 2], ['KB', 1024]]) {
            if (bytes >= size) return `${(bytes / size).toFixed(unit === 'GB' ? 1 : 0)} ${unit}`;
        }
        return `${bytes} B`;
    },

    formatPercent(val) { return val != null ? val.toFixed(1) + '%' : '-'; },

    // formatPorts: de gepubliceerde poorten als "http 80, bench 9000", of "-";
    // platte tekst, de aanroeper escapet.
    formatPorts(ports) {
        if (!ports || !Object.keys(ports).length) return '-';
        return Object.entries(ports).map(([k, v]) => `${k} ${v}`).join(', ');
    },

    formatAttributes(attrs) {
        if (!attrs || !Object.keys(attrs).length) return '';
        return Object.keys(attrs).sort().map(k => `${k}=${attrs[k]}`).join('\n');
    },

    formatJobTooltip(job) {
        const parts = [];
        if (job.affinity && Object.keys(job.affinity).length)
            parts.push('Affinity: ' + Object.entries(job.affinity).sort().map(([k,v]) => `${k}=${v}`).join(', '));
        if (job.artifacts?.length) {
            for (const a of job.artifacts) {
                const match = a.match && Object.keys(a.match).length
                    ? Object.entries(a.match).sort().map(([k,v]) => `${k}=${v}`).join(',') + ' → ' : '';
                parts.push('Artifact: ' + match + a.url);
            }
        }
        if (job.image) parts.push('Image: ' + job.image);
        if (job.tags && Object.keys(job.tags).length)
            parts.push('Tags: ' + Object.entries(job.tags).sort().map(([k,v]) => `${k}=${v}`).join(', '));
        return parts.join('\n');
    },

    _statusContent(text) { return `<span class="t-status-dot" aria-hidden="true"></span><span>${this._esc(text)}</span>`; },

    _esc(value) { return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c])); },
};

// Keep native forms, keyboard navigation and dialogs usable after live rerenders.
$('clusterMenu').addEventListener('keydown', event => {
    const button = event.target.closest('[data-cluster]');
    if (!button) return;
    const index = Number(button.dataset.cluster), count = app.clusters.length;
    const next = { ArrowDown: (index + 1) % count, ArrowUp: (index + count - 1) % count, Home: 0, End: count - 1 }[event.key];
    if (next === undefined) return;
    event.preventDefault();
    app.switchCluster(next);
    document.querySelector(`[data-cluster="${next}"]`)?.focus({ preventScroll: true });
});
$('addClusterForm').addEventListener('submit', event => { event.preventDefault(); app.addClusterFromForm(); });
$('createJobForm').addEventListener('submit', async event => {
    event.preventDefault();
    const button = $('startJobButton');
    if (button.disabled) return;
    button.disabled = true;
    try { await app.startJob(); } finally { button.disabled = false; }
});
$('logModal').addEventListener('cancel', event => { event.preventDefault(); app.closeLogs(); });
$('logModal').addEventListener('close', () => app.closeLogs());
$('clusterForm').addEventListener('cancel', event => { event.preventDefault(); app.hideAddCluster(); });
document.addEventListener('click', event => {
    const button = event.target.closest('button');
    if (button?.dataset.cluster !== undefined) app.switchCluster(Number(button.dataset.cluster));
    else if (button?.dataset.removeCluster !== undefined) app.removeCluster(Number(button.dataset.removeCluster));
    else if (button?.dataset.deleteJob !== undefined) app.deleteJob(button.dataset.deleteJob);
    else if (button?.dataset.logTask !== undefined) app.openLogs(button.dataset.logTask, button.dataset.logAgent, button.dataset.logEndpoint);
    else if (button?.dataset.openJob !== undefined) app.openJobDetail(button.dataset.openJob);
    else if (button?.dataset.openAgent !== undefined) app.openAgentDetail(button.dataset.openAgent);
    else if (!button && !event.target.closest('input, a')) {
        const row = event.target.closest('tr[data-job-id]');
        if (row) app.openJobDetail(row.dataset.jobId);
        const agentRow = event.target.closest('tr[data-agent-id]');
        if (agentRow) app.openAgentDetail(agentRow.dataset.agentId);
    }
});
document.addEventListener('keydown', event => {
    if (event.key !== 'Escape' || event.defaultPrevented || document.querySelector('dialog[open]')) return;
    if (app.activeJobId) app.closeJobDetail();
    else if (app.activeAgentId) app.closeAgentDetail();
});
window.addEventListener('popstate', () => app.navigateToHash());

// Init
app.loadClusters();
app.renderClusterMenu();
if (app.clusters.length) {
    app.connect();
    if (location.hash) app.navigateToHash();
    else history.replaceState(null, '', '#agents');
}
if (!app.prefillFromQuery() && !app.clusters.length) app.showAddCluster();
