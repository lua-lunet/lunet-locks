# lease-sequencer environment ladder

The environment ladder for the diskless lease-sequencer: local ports (the
demo on loopback), one full-dress Lima VM with the complete diagnosis kit,
a colima aarch64 build proof, and a three-VM Lima cluster across three
kernel/network boundaries. Each rung records what ran, what was proven, and
the machine facts. Timings inside one physical host are indicative only;
the cross-datacentre numbers are the cloud rung's job.

## Baseline: the demo check on any single host

`run.sh`'s boot / join / increment / cadence / overlap criteria pass on the
host and inside the VMs. The kill-cycle leg completes its steal, identity
bump, reincarnation, and peer remap; the cycle's final re-stabilization
assertion fails: after the reincarnated node's re-admission walk begins, the
leader's renew stream stops (the walk never completes against the
future-era reincarnation — the multi-era learner-acquisition boundary, the
upstream fourth finding, see EXPERIMENTS.md). The one-cycle reincarnation +
steal window is the proof the environment drill uses; repeated cycles are
the E1 harness's job and its k≥2 leg is upstream-blocked.

## Rung 1 — one Lima VM, full-dress

Instance `lunet-locks-lab`: Debian trixie genericcloud-arm64
(`20260327-2429`, the same image ref as the host's existing Lima instance),
lima 2.2.0, vmType vz, 4 vCPU / 8 GiB / 30 GiB disk, kernel
`6.12.74+deb13+1-cloud-arm64`, Debian 13.4. Provisioned from the trixie apt
set (`ca-certificates curl libgcc-s1 libluajit-5.1-2 libstdc++6 libuv1`
plus the kit: chrony, tcpdump, iperf3, netcat-openbsd) and built from
source in the VM (rustup stable 1.98.1 — Debian's rustc 1.85 predates
let-chains).

NTP: chrony active, stratum 2, RMS offset 0.000000642 s (system time
0.000009166 s fast of NTP time).

The full drill (`vm_drill.sh`): six-node / three-DC cluster, join of ids
4/5/6, increment of 4 and 5, the stability-window cadence assertions, the
RTT probe, a rotated size-capped tcpdump of the UDP peer traffic
(`-i any -W 3 -C 5`, filter `udp and portrange 41101-41199`), a per-second
client-stats sampler, and the load client (`lease-load`, 1 contender +
2 getters) through a low-rate window and then a high-rate window. The load
client starts after the join reconfigurations complete — see findings.

Proven (all assertions green):

- cadence holds INSIDE the VM: renew 252-253 ms (window 150-400 ms),
  non-holder polls 480-503 ms, holder id 1, low and high rate alike;
- no two holders' lease windows overlap (45 grants in the low-rate window);
- the capture file grows: 831488 → 1003520 bytes across a 3 s window
  (1.5 MB `peer.pcap0` by drill end);
- the client stats file grows: 1821 → 2278 bytes (5 two-second window
  lines; ops `set ok=1`, `bump ok`, `get` with the contender's 2 expected
  errors while the lock was contested);
- RTT samples across the node client ports (`rtt_probe`, 20 per node,
  loopback-shaped and indicative only): p50 1.9-5.8 ms per node
  (e.g. dc1-node1 min=75 p50=1873 p90=5888 mean=2514 us) — the spread is
  the 4-vCPU VM scheduling the six nodes, the capture, and the load client
  together.

Findings baked into the drill:

- The join reconfigurations stall if the load client runs before they
  complete (observed twice: joins accepted, the leader's walk for the next
  reconfiguration never folds, increments refused for the full 90 s). The
  nodes' own embedded lease drivers are the boot-phase client traffic; the
  external load client starts once the cluster is stable.
- Debian's `tcpdump` (run via sudo) drops to the `tcpdump` user before
  opening the savefile; the drill runs it unprivileged against the
  `cap_net_raw,cap_net_admin=eip` file caps so it can write the capture.

Rung wall time: ~32 minutes (instance create → green drill).

## Rung 2 — colima aarch64 build proof

The local colima daemon as found: macOS Virtualization.Framework, aarch64,
docker server 29.2.1, runtime docker, mountType virtiofs, 4 vCPU / 8 GiB.
Left running exactly as found.

`make docker-build` passes on the daemon: the image builds and is
`linux/arm64` native. `docker/Dockerfile` gains the diagnosis kit in the
same plain multi-stage, no-BuildKit model:

- the proven cdylib stage stays byte-identical (rust 1.85, its own
  vendored closure);
- a new `demo` kit stage pins `rust:1.98-slim` and builds the demo crate's
  binaries (`lease-sequencer`, `lease-client`, `lease-load`) plus the
  std-only `rtt_probe` from the demo crate's own Cargo.lock vendored
  closure (`vendor-demo` in the prepared context — the two locks resolve
  different versions, so they cannot share one vendor directory). The
  separate pin exists because the demo crate uses let-chains (rustc 1.88+)
  while the cdylib stage stays on the proven 1.85;
- the runtime stage adds `chrony tcpdump iperf3 netcat-openbsd procps` and
  carries the demo binaries at `/app/demo/bin` with the demo's cluster
  descriptor at `/app/demo/cluster.jsonl`.

No-BuildKit proven explicitly: the prepared context builds end-to-end under
the legacy builder (`DOCKER_BUILDKIT=0`, docker 29.2.1, all 32 steps, no
BuildKit-only mounts/secrets/cache anywhere in the file) and under the
daemon's default path.

Functional proof: `make docker-simulation` (30 s) passes — three long-lived
service containers, dynamic TCP clients, kill/expire/takeover/renew cycles
across all three DCs, "simulation passed: no conflicting live holder
observed". Kit verified inside the image: `chronyc`, `tcpdump`, `iperf3`,
`nc` present and `rtt_probe` runs. One container-vs-VM fact: chronyd ships
in the image but cannot discipline the clock from inside a container (no
CAP_SYS_TIME) — containers ride the host kernel clock, so NTP belongs to
the host in container runs.

No timings are claimed from Docker, per the design doc's rule.

Rung wall time: ~10 minutes (Dockerfile + context changes, two builds,
simulation).

## Rung 3 — three Lima VMs

Instances `lunet-locks-node1/2/3`: the same Debian trixie arm64 image ref,
vmType vz, 2 vCPU / 2 GiB / 10 GiB each, kernel `6.12.74+deb13+1-cloud-arm64`,
lima 2.2.0, attached to the built-in `user-v2` named network (the documented
VM-to-VM path; each VM's default user-mode network is replaced by it), IPs
192.168.104.1 / .3 / .4, VM-to-VM ICMP RTT 0.294 ms. Kit per VM: the apt
runtime set, chrony (active), tcpdump with `cap_net_raw,cap_net_admin=eip`
file caps, iperf3, netcat; binaries extracted from the rung-2 image
(`docker create` + `docker cp`) — the image's contents proven functional
outside Docker.

NTP per VM at drill time: node1 stratum 4, system time 55.6 µs slow, RMS
0.0152 s; node2 stratum 3, 17.6 µs fast, RMS 0.0167 s; node3 stratum 3,
~0 µs slow, RMS 0.1378 s (RMS includes the boot history; live offsets are
the system-time figures). Cross-VM skew is microseconds — lease math safe.

Cluster: three genesis voters, one per VM, the descriptor carrying literal
IPv4s, each node binding its own VM's IP (`--client 0.0.0.0:4210N`), the
cluster forming across three VM kernel/network boundaries. Proof-of-life:
`lease-load` on node1's VM against all three client ports (1 contender +
2 getters, low rate), per-second stats sampling, per-VM tcpdump of the UDP
peer traffic. The load client starts after the holder is elected (the
rung-1 finding).

Proven before the cycle:

- cadence holds across the VM boundaries: renew 252 ms, non-holder polls
  433/465 ms, holder id 1;
- no two holders' lease windows overlap: PASS across 251 grants;
- capture grows on every VM through the whole drill (node1 1953792 →
  2342912, node2 970752 → 1376256, node3 966656 → 1191936 bytes);
- client stats grow (15996 bytes at window end; 2039 ops over the drill);
- RTT matrix, 15 samples per pair from every VM (indicative only; the
  0.294 ms ICMP floor is buried under the 5 ms tick loop and scheduler):
  p50 2.5-5.2 ms per node pair from each probe site.

The ONE kill/restart cycle (holder id 1 on node1's VM, load client live,
reference clock node2's VM):

- SIGKILL at ts=1788975305231; the client stats show the outage as an error
  burst (cumulative errors 2 → 15 → 31 → 45 across the kill→steal windows,
  p50 collapsing to ~3 ms as failures return fast) — this is the steal
  window from the client's side;
- node 2 steals: `grant node=2 op=steal` at ts=1788975310469 — kill→steal
  5238 ms — and serves a clean ~250 ms renew stream; client error rate 0
  from ts=1788975312667 through ts=1788975324669 under the new holder;
- the restarted node reincarnates: `own=16777217 incarnation=1` (was
  `own=1 incarnation=0`) at ts=1788975314626, and the peer accepts the
  resurrection (`remap old=1 new=16777217` at ts=1788975314677);
- the reconfiguration folds the bumped identity (the cluster advances to
  era 3 with it as a voter), and the leader succession then hands the
  leader role to the re-admitted identity (`leader change ... leader=
  16777217` at ts=1788975324952) whose process is still fenced at era 1 —
  `ReincarnationRefused` storms at the peers, the commit stream stops, and
  the load client wedges against the ghost leader from ts=1788975326669
  (steady ~23 errors per 2 s window to the end of the drill).

Stop-and-report (upstream, the fourth finding again, new manifestation):
the E1 first-cycle rejoin claim holds only when the killed node is a voting
NON-leader (the E1 harness's shape). Killing the holder/leader — the demo's
cycle shape — forces an election plus a reconfiguration that advance the
cluster past the reincarnation's reopen view; the re-admitted identity is
then elected leader while its process cannot evaluate the new era, wedging
the whole cluster. Observed on the host (`run.sh`, twice) and on the
three-VM genesis cluster (once). The steal window, the identity bump, and
the peer remap all complete; the post-rejoin leader succession is the
blocked leg.

VM end-state: all four created VMs (`lunet-locks-lab`, `node1`, `node2`,
`node3`) stopped and removed; the pre-existing `nextcloud-dev` instance
untouched (stopped as found); the colima daemon left running as found; the
`lunet-advisory-lock` image left in the daemon (reproducible with
`make docker-build`).

Rung wall time: ~25 minutes active (three instance creates, provisioning,
drill, cycle, teardown).

## Scaleway — permission-gated

No cloud instance was booted and none may be booted without the user's
explicit permission; the ladder stops here by design (the user's order:
local ports → one full-dress VM → cross-VM visibility → three VMs → then
and only then ask permission for Scaleway). When the gate opens, issue #14's
stages apply: Stage 3 (one node, baby step, zero running instances at its
end) precedes Stage 4 (three nodes, one per fr-par zone); the PAR zones are
x86_64-only so the cloud target is amd64 while the local ladders ran
aarch64; every node needs NTP as a hard dependency; `cluster.jsonl` carries
the nodes' literal private IPv4s; and nothing is left running — power-off
discipline end to end.
