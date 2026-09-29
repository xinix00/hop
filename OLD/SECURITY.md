# Security Architecture

How the Hop Infrastructure Suite achieves production-grade security through simplicity.

## Design Philosophy

Traditional orchestrators solve multi-tenancy with complex software controls (RBAC, NetworkPolicy, service mesh). Hop takes a different approach: **physical separation over logical separation.**

```mermaid
graph TB
    subgraph "Traditional (Kubernetes)"
        direction TB
        K8S[Shared Cluster]
        K8S --> TA[Team A<br/>namespace-a]
        K8S --> TB[Team B<br/>namespace-b]
        K8S --> TC[Team C<br/>namespace-c]
        K8S -.->|"NetworkPolicy<br/>RBAC<br/>Quotas<br/>Service Mesh"| RISK["⚠ One misconfiguration<br/>breaks everything"]
    end

    subgraph "Hop Infrastructure"
        direction TB
        CA[Cluster A<br/>VLAN 10<br/>Team A]
        CB[Cluster B<br/>VLAN 20<br/>Team B]
        CC[Cluster C<br/>VLAN 30<br/>Team C]
        CA ~~~ CB ~~~ CC
        SAFE["Physically separated<br/>Nothing to misconfigure"]
    end
```

## Defense in Depth (4 Layers)

```mermaid
graph TB
    subgraph L1["Layer 1 — VLAN Isolation"]
        subgraph L2["Layer 2 — Network ACLs (Tailscale/UniFi/firewall)"]
            subgraph L3["Layer 3 — Cluster-per-group"]
                subgraph L4["Layer 4 — Process Isolation"]
                    P["chroot / sandbox<br/>cgroups / ulimit<br/>dedicated workdir"]
                end
            end
        end
    end

    style L1 fill:#1a1a2e,stroke:#e94560,color:#fff
    style L2 fill:#16213e,stroke:#e94560,color:#fff
    style L3 fill:#0f3460,stroke:#e94560,color:#fff
    style L4 fill:#533483,stroke:#e94560,color:#fff
```

Each layer is independent, proven, and explainable in one sentence:

| Layer | Technology | Proven since | One-sentence explanation |
|-------|-----------|-------------|--------------------------|
| 1 | VLAN | 1998 | Network traffic physically separated at the switch |
| 2 | Encrypted network | VPN/WireGuard | Encrypted access with site-to-site connectivity |
| 3 | Cluster-per-group | Architecture | Each team owns their cluster, nothing shared |
| 4 | chroot/sandbox/cgroups | 1982/2007 | OS-level process isolation and resource limits |

## Layer 1: VLAN Isolation

Each application group runs in its own VLAN. Layer 2 network isolation — the most battle-tested isolation primitive in networking.

```mermaid
graph LR
    subgraph VLAN10["VLAN 10 — Frontend"]
        F1[Node 1] --- F2[Node 2] --- F3[Node 3]
    end
    subgraph VLAN20["VLAN 20 — API"]
        A1[Node 4] --- A2[Node 5] --- A3[Node 6]
    end
    subgraph VLAN30["VLAN 30 — Data"]
        D1[Node 7] --- D2[Node 8] --- D3[Node 9]
    end

    VLAN10 -.-x|"blocked"| VLAN20
    VLAN20 -.-x|"blocked"| VLAN30
```

**What it provides:**
- Complete network separation between application groups
- No cross-group traffic possible at the network level
- No software misconfiguration can break this boundary
- Standard switch/router configuration, nothing custom

**Auditor conversation:**
> "How are your environments separated?"
> "Each cluster runs in its own VLAN."
> "Next question."

## Layer 2: Encrypted Network (Your Choice)

Layer 2 provides encrypted connectivity and access control. Multiple options, same security guarantees:

```mermaid
graph TB
    subgraph Options["Network Layer — Pick One"]
        direction LR
        TS["Tailscale<br/>WireGuard mesh<br/>Identity-based ACLs<br/>Zero-config"]
        UB["UniFi / Ubiquiti<br/>Site-to-site VPN<br/>VLAN across locations<br/>Hardware-backed"]
        WG["WireGuard (raw)<br/>Manual mesh<br/>Full control<br/>No vendor"]
    end

    subgraph Result["Same Result"]
        R["Encrypted traffic<br/>Access control<br/>Cross-site connectivity"]
    end

    TS --> Result
    UB --> Result
    WG --> Result
```

| Option | Strength | Best for |
|--------|----------|----------|
| **Tailscale** | Identity-based ACLs, zero-config, SSO | Cloud-native teams, remote access |
| **UniFi/Ubiquiti** | Hardware VPN, VLAN mesh across sites, no external dependency | On-premise, multi-site, full ownership |
| **WireGuard (raw)** | No vendor, full control | Teams that want maximum control |
| **IPsec/OpenVPN** | Legacy compatibility | Existing infrastructure |

**What any of these provide:**
- **Encryption**: All inter-node traffic encrypted
- **Access control**: Who/what can reach which cluster
- **Cross-site**: Connect VLANs across physical locations
- **Isolation**: Combined with VLAN = network-level separation

### Ubiquiti example (multi-site VLAN mesh)

```mermaid
graph LR
    subgraph Site_A["Site A — Amsterdam"]
        UDM_A["UniFi Gateway"]
        SW_A["UniFi Switch"]
        N1[Node 1<br/>VLAN 10]
        N2[Node 2<br/>VLAN 10]
        UDM_A --- SW_A
        SW_A --- N1
        SW_A --- N2
    end

    subgraph Site_B["Site B — Frankfurt"]
        UDM_B["UniFi Gateway"]
        SW_B["UniFi Switch"]
        N3[Node 3<br/>VLAN 10]
        N4[Node 4<br/>VLAN 10]
        UDM_B --- SW_B
        SW_B --- N3
        SW_B --- N4
    end

    UDM_A ===|"Site-to-site VPN<br/>VLAN 10 bridged"| UDM_B
```

Same VLAN, two sites, hardware-encrypted tunnel. Nodes 1-4 are in the same cluster — hop sees no difference between local and remote nodes.

### Tailscale example (identity-based)

```json
{
  "acls": [
    {"action": "accept", "src": ["tag:admin"],      "dst": ["tag:hop:*"]},
    {"action": "accept", "src": ["tag:deploy"],      "dst": ["tag:hop:8080"]},
    {"action": "accept", "src": ["tag:monitoring"],   "dst": ["tag:hop:9090"]}
  ]
}
```

**Key point:** Hop does not depend on any specific network solution. VLAN isolation works with any vendor or technology. Pick what fits your infrastructure.

## Layer 3: Cluster-per-Application-Group

Instead of isolating workloads within a shared cluster, each application group gets its own cluster.

```mermaid
graph TB
    subgraph "Shared cluster (anti-pattern)"
        SC[Single Cluster]
        SC --> NS1["namespace: frontend<br/>NetworkPolicy?<br/>ResourceQuota?<br/>RBAC roles?"]
        SC --> NS2["namespace: api<br/>NetworkPolicy?<br/>ResourceQuota?<br/>RBAC roles?"]
        SC --> NS3["namespace: data<br/>NetworkPolicy?<br/>ResourceQuota?<br/>RBAC roles?"]
    end

    subgraph "Separate clusters (Hop model)"
        C1["Frontend Cluster<br/>3 nodes, full access"]
        C2["API Cluster<br/>5 nodes, full access"]
        C3["Data Cluster<br/>3 nodes, full access"]
    end
```

**Why this is better than namespace isolation:**

| Shared cluster + namespaces | Separate clusters |
|-----------------------------|-------------------|
| 1 misconfigured NetworkPolicy = breach | Physically impossible to cross |
| RBAC complexity grows with teams | No RBAC needed — it's your cluster |
| Noisy neighbor (CPU/memory) | Dedicated resources |
| Single upgrade affects everyone | Independent upgrades |
| Complex audit trail | Simple: who accessed which cluster |

## Layer 4: Process Isolation

Each task runs with OS-level isolation:

```mermaid
graph TB
    subgraph Agent["Agent Node"]
        subgraph T1["Task A (chroot)"]
            P1["Process<br/>cgroups memory limit<br/>nice CPU priority<br/>dedicated workdir<br/>isolated filesystem"]
        end
        subgraph T2["Task B (chroot)"]
            P2["Process<br/>cgroups memory limit<br/>nice CPU priority<br/>dedicated workdir<br/>isolated filesystem"]
        end
    end
```

| Platform | Filesystem | Memory | CPU |
|----------|-----------|--------|-----|
| Linux | `chroot` jail | `cgroups v2` (OOM killer) | `nice` priority |
| macOS | `sandbox-exec` | `ulimit` | `nice` priority |

## Secrets Management

Hop does not store secrets. Secrets are injected at deploy time via external secrets managers.

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant Git as Git Repo
    participant OP as 1Password
    participant CLI as run apply
    participant ER as hop

    Dev->>Git: Push job template
    Dev->>CLI: ./deploy.sh myapp
    CLI->>OP: op read "op://Prod/myapp/db_password"
    OP-->>CLI: secret value
    CLI->>CLI: envsubst template → job.json
    CLI->>ER: run apply job.json
    CLI->>CLI: rm job.json
    Note over ER: Secrets live only<br/>in environment variables<br/>Never persisted to disk
```

**What this means for compliance:**
- No secrets at rest in the orchestrator
- Secrets rotation handled by the secrets manager
- Audit trail for secret access is in 1Password/Vault, not in Hop

## Attack Surface Comparison

```mermaid
graph LR
    subgraph Hop["Easy (5 binaries)"]
        ER[hop]
        ED[hopdns]
        EL[hoplb]
        EP[hopprom]
        LS[hoplockserver]
    end

    subgraph K8s["Kubernetes (15+ components)"]
        API[API Server]
        ETCD[etcd]
        SCHED[Scheduler]
        CM[Controller Manager]
        KUB[kubelet]
        KP[kube-proxy]
        CNI[CNI Plugin]
        CRI[Container Runtime]
        DNS[CoreDNS]
        ING[Ingress Controller]
        MS[Metrics Server]
        CERT[Cert Manager]
        ADM[Admission Controllers]
        SA[Service Accounts]
        dots[...]
    end
```

| Metric | Hop | Kubernetes |
|--------|------|------------|
| Components | 5 binaries | 15+ components |
| External dependencies | 3 Go libraries | Hundreds (CRI, CNI, CSI, ...) |
| Open ports | 0 (VPN/VLAN) | API server, etcd, kubelet, ... |
| CVEs per year | Minimal (small codebase) | 20+ (large attack surface) |
| Config parameters | <10 | 100+ |
| Can fully audit codebase | Yes | No (millions of lines) |

**The security advantage of simplicity:** a system you can fully understand is a system you can fully secure.

## Compliance Mapping

### SOC 2 Type II

| Trust Service Criteria | Hop Implementation |
|------------------------|---------------------|
| **CC6.1** Logical access | Network ACLs (Tailscale/UniFi/firewall) |
| **CC6.2** Access credentials | VPN device keys + 1Password |
| **CC6.3** Access removal | Device/key removal = instant revoke |
| **CC6.6** System boundaries | VLAN per cluster + Network ACLs (Tailscale/UniFi/firewall) |
| **CC6.7** Data transmission | WireGuard encryption (all traffic) |
| **CC7.1** Threat detection | hopprom + Prometheus alerting |
| **CC7.2** Monitoring | hopprom metrics, network/VPN access logs |
| **CC8.1** Change management | Git-based deploys, `run apply` from version control |
| **A1.2** Recovery | Auto-restart, leader failover, state persistence |

### ISO 27001

| Control | Hop Implementation |
|---------|---------------------|
| **A.9.1** Access control policy | Network ACLs (Tailscale/UniFi/firewall) + cluster-per-group |
| **A.9.2** User access management | VPN/network identity management |
| **A.10.1** Cryptographic controls | WireGuard encryption (all traffic) |
| **A.12.1** Operational procedures | `run apply` from git, repeatable deploys |
| **A.12.4** Logging and monitoring | hopprom + Prometheus + network/VPN logs |
| **A.13.1** Network security | VLAN isolation + encrypted network layer |
| **A.14.2** Secure development | Static binaries, minimal dependencies |
| **A.17.1** Continuity | Multi-node clusters, auto-failover |

### HIPAA

| Safeguard | Hop Implementation |
|-----------|---------------------|
| **Access control** | VPN identity + network ACLs |
| **Audit controls** | Network/VPN logs + hopprom metrics |
| **Integrity controls** | Encrypted transport (WireGuard) |
| **Transmission security** | All traffic encrypted by default |
| **Workstation security** | VPN/network device authorization |

### Gaps to Address

| Gap | Required for | Solution |
|-----|-------------|----------|
| Audit log (who deployed what) | SOC 2, ISO 27001 | Add logging to CLI (`run apply` logs to file/syslog) |
| Deploy approval workflow | SOC 2 (change management) | Git PR approval before `run apply` |
| Encryption at rest | HIPAA, PCI DSS | OS-level disk encryption (LUKS/FileVault) |
| Vulnerability scanning | All | External tooling (Trivy, Grype) |
| Penetration test report | SOC 2, ISO 27001 | Commission annually |
| Security policy documentation | All | Document your ISMS |

Most gaps are **documentation and process**, not technology.

## Security Monitoring

hopprom + Prometheus provides security-relevant alerting:

```yaml
groups:
  - name: security
    rules:
      # Unexpected agent (possible unauthorized node)
      - alert: UnknownAgent
        expr: hop_agents_total > expected_agent_count
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Unexpected agent count: {{ $value }}"

      # High failure rate (possible attack or misconfiguration)
      - alert: HighFailureRate
        expr: rate(hop_task_failures_total[5m]) > 1
        for: 5m
        labels:
          severity: warning

      # Agent down (possible infrastructure issue)
      - alert: AgentDown
        expr: hop_agents_healthy < hop_agents_total
        for: 2m
        labels:
          severity: critical
```

## Deployment Security Checklist

### Per cluster

- [ ] Dedicated VLAN configured
- [ ] Network ACLs (Tailscale/UniFi/firewall) defined and reviewed
- [ ] hoplockserver API key (`cluster.lock.api_key`) generated and secured
- [ ] Secrets injected via 1Password/Vault (never in job configs)
- [ ] Disk encryption enabled on all nodes (LUKS/FileVault)
- [ ] hopprom + Prometheus alerting configured
- [ ] Firewall rules: only VPN/VLAN traffic allowed

### Per application

- [ ] Static binary verified (`ldd` → "not a dynamic executable")
- [ ] Health check configured
- [ ] Resource limits set (CPU shares + memory limit)
- [ ] Artifact downloads authenticated (S3 auth / HTTP headers)
- [ ] Environment variables via secrets manager, not hardcoded

### Organizational

- [ ] Deploy process via git (PR + approval + `run apply`)
- [ ] Access reviews scheduled (VPN/network device audit)
- [ ] Incident response plan documented
- [ ] Backup/recovery procedure tested
- [ ] Penetration test scheduled annually

## Scaling Through Constant Complexity

Traditional orchestrators become harder to secure as they grow. More tenants = more RBAC rules, more NetworkPolicies, more audit complexity. Hop's cluster-per-group model means **security complexity is O(1), not O(n).**

```mermaid
graph LR
    subgraph Traditional["Traditional: Complexity grows with scale"]
        direction TB
        S1["10 teams"] -->|"10× RBAC rules<br/>10× NetworkPolicies<br/>10× audit scope"| C1["High complexity"]
        S2["100 teams"] -->|"100× RBAC rules<br/>100× NetworkPolicies<br/>100× audit scope"| C2["Unmanageable"]
    end

    subgraph Hop["Hop: Complexity stays constant"]
        direction TB
        E1["10 clusters"] -->|"Same config per cluster<br/>Same VLAN setup<br/>Same audit scope"| EC1["Low complexity"]
        E2["100 clusters"] -->|"Same config per cluster<br/>Same VLAN setup<br/>Same audit scope"| EC2["Low complexity"]
    end
```

**Why this works:**

| Aspect | Shared cluster (traditional) | Cluster-per-group (Hop) |
|--------|------------------------------|--------------------------|
| Adding a team | New RBAC roles, NetworkPolicies, quotas, audit rules | New VLAN + new cluster (identical config) |
| Security audit | Audit grows linearly with tenants | Audit one cluster = audit all clusters |
| Blast radius | One misconfiguration affects everyone | One cluster's issue stays in that cluster |
| Compliance scope | Entire shared cluster is in scope | Only the relevant cluster is in scope |

```mermaid
graph TB
    subgraph Scale["From 3 to 300 nodes"]
        direction LR
        subgraph Small["Small: 3 nodes"]
            CS1["Cluster A<br/>VLAN 10<br/>3 nodes"]
        end
        subgraph Medium["Medium: 30 nodes"]
            CM1["Cluster A<br/>VLAN 10<br/>5 nodes"]
            CM2["Cluster B<br/>VLAN 20<br/>5 nodes"]
            CM3["Cluster C<br/>VLAN 30<br/>5 nodes"]
            CM4["...3 more"]
        end
        subgraph Large["Large: 300 nodes"]
            CL1["Cluster A<br/>VLAN 10"]
            CL2["Cluster B<br/>VLAN 20"]
            CL3["..."]
            CL4["Cluster N<br/>VLAN N0"]
        end
    end

    style Small fill:#0f3460,stroke:#e94560,color:#fff
    style Medium fill:#0f3460,stroke:#e94560,color:#fff
    style Large fill:#0f3460,stroke:#e94560,color:#fff
```

**The key insight:** scaling from 3 to 300 nodes doesn't mean one cluster with 300 nodes. It means more clusters, each with the same simple, auditable configuration. The security posture of cluster #50 is identical to cluster #1 — same VLAN setup, same process isolation, same deployment checklist.

**For compliance, this is a superpower:** auditors don't need to understand a growing set of policies. They audit one cluster template, and that audit covers all clusters.

## Summary

Hop's security model is built on **proven, simple, auditable** layers:

| Layer | Technology | Age | Complexity |
|-------|-----------|-----|------------|
| Network | VLAN | 25+ years | Switch config |
| Encryption | VPN (WireGuard/IPsec/UniFi) | Proven | Standard infra |
| Isolation | Cluster-per-group | Architecture | Nothing to configure |
| Process | chroot/sandbox/cgroups | 40+ years | OS primitives |

No RBAC policies to misconfigure. No NetworkPolicies to audit. No service mesh certificates to rotate. Just layers of proven technology, each independently verifiable.

**The simplest system to secure is the smallest system to understand.**
