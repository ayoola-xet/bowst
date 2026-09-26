# Deploying the Phase 1 soak test

This guide takes a fresh Linux server to a 72-hour market-data soak run of `bowst-md`, with the full session journaled for replay. No API keys or accounts are needed: the run uses only Binance's public market-data endpoints and places no orders.

## What this run proves, and what it does not

Phase 1 exit criteria (README §15) and how this run covers them:

| Criterion | Covered by this run? |
|---|---|
| 72 h continuous run with zero undetected gaps | **Yes.** Every gap is detected by sequence bridging, reported as `DOWN`, and resynchronized. The journal lets any incident be replayed exactly. |
| Book matches venue snapshots | **Yes.** Once a minute, one book (in turn) is rebuilt from a fresh snapshot and compared with the live book, 100 levels per side (ADR 0010). |
| Decode + book update (ADR 0011) | **Measured.** Every message is timed and reported every 10 s. The target (p99 ≤ 25 µs per message) applies to a dedicated, busy-polling core; a shared or sleeping machine overstates it. |

It shakes out connection-lifecycle, resync, verification and journal problems over days rather than seconds.

## 1. Legal check (before choosing where)

Read README §17, item 6. Where the server sits does not decide whether the business may use a venue: the trading entity's jurisdiction and the venue's terms do. For this run the tool uses only the public mirror (`data-stream.binance.vision`, `data-api.binance.vision`), which serves public data. Do not route around a geo-block with a VPN or proxy.

## 2. Server

| Item | Recommendation | Why |
|---|---|---|
| Region | AWS Tokyo (`ap-northeast-1`) | Binance's matching engine is hosted there, so this is where production will run. Any region works for this run. |
| Instance | 2 dedicated vCPUs, 4 GB RAM or more, x86-64 (for example `c7i.large`) | One core for the market-data thread, one for everything else. Burstable instances (`t3`/`t4g`) throttle under a sustained load and give misleading results. |
| Disk | 50 GB gp3 | See sizing below. |
| OS | Ubuntu 24.04 LTS | Any recent Linux with systemd works. |
| Network | Outbound HTTPS (443) only. Inbound SSH only, from your IP. | The tool serves nothing. |
| Clock | Keep the default time sync (Amazon Time Sync via `chrony`). | Journal wall-clock timestamps depend on it. |

**Cost:** an instance of this size is roughly USD 0.10 per hour, so a 72-hour run costs on the order of USD 10 plus disk. Check current pricing for your account.

**Disk sizing:** a real run with BTCUSDT, ETHUSDT and SOLUSDT wrote about 140 MB of journal per hour, roughly 50 MB per pair per hour, or 3.5 GB per pair over 72 hours. Volume rises with market activity, so budget 5 GB per pair and keep at least 30% of the disk free.

**Low-cost alternatives.** This run only reads public data, and latency figures from a shared or burstable machine are indicative only, so any always-on machine works:

- A free-tier cloud VM (for example Oracle Cloud's Always Free ARM instances), or a small VPS billed by the hour (Vultr, DigitalOcean, Hetzner), about USD 1–2 for 72 hours. Pick at least 2 GB of RAM, or add swap before building, because the optimized release build needs the memory.
- A computer you already have. On macOS, which has no systemd, run the tool in the foreground and keep the machine awake:

  ```sh
  mkdir -p ~/bowst-soak
  caffeinate -i ./target/release/bowst-md --symbols BTCUSDT,ETHUSDT,SOLUSDT \
    --seconds 259200 --journal ~/bowst-soak/journal --metrics 127.0.0.1:9184 \
    > ~/bowst-soak/soak.log 2>&1
  ```

  Keep it plugged in with the lid open, and use a directory in your home folder: macOS clears `/tmp`. Do not rebuild or `git pull` in that checkout during the run. Replacing a running binary can get the process killed; use a second clone for development.

Home network drops are fine: they exercise reconnection and resynchronization, which is part of what the run tests.

## 3. Build

On the server, as a normal user with `sudo`:

```sh
sudo apt-get update
sudo apt-get install -y build-essential git ca-certificates curl

# Rust. The repository pins the exact toolchain in rust-toolchain.toml.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"

git clone https://github.com/ayoola-xet/bowst.git
cd bowst
git checkout main          # or the release commit under test; note its hash in the run log
cargo build --release -p bowst-md
./target/release/bowst-md --help
```

The repository is private: clone with a read-only deploy key or a fine-grained token that has read access to this repository only. Never paste a token into the command line history of a shared machine.

Smoke test for 20 seconds before installing the service:

```sh
./target/release/bowst-md --symbols BTCUSDT,ETHUSDT --seconds 20 --journal /tmp/smoke
./target/release/bowst-md --replay /tmp/smoke
```

Both commands must exit with status 0, and the replay must report `0 records missing`.

## 4. Install as a service

```sh
sudo useradd --system --home /var/lib/bowst --shell /usr/sbin/nologin bowst
sudo install -d -o bowst -g bowst -m 0750 /var/lib/bowst /var/lib/bowst/journal
sudo install -m 0755 target/release/bowst-md /usr/local/bin/bowst-md
```

Create `/etc/systemd/system/bowst-md-soak.service`:

```ini
[Unit]
Description=bowst market-data soak run
Wants=network-online.target
After=network-online.target time-sync.target

[Service]
User=bowst
Group=bowst
# 72 hours. Choose the pairs to test; each must exist on Binance Spot.
ExecStart=/usr/local/bin/bowst-md --symbols BTCUSDT,ETHUSDT,SOLUSDT --seconds 259200 --journal /var/lib/bowst/journal --metrics 127.0.0.1:9184
# A soak run must not be restarted automatically: a restart would hide the failure being tested for.
Restart=no
LimitNOFILE=65536

# Hardening: the tool needs outbound network and its journal directory, nothing else.
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_INET AF_INET6
ReadWritePaths=/var/lib/bowst/journal

[Install]
WantedBy=multi-user.target
```

Start the service:

```sh
sudo systemctl daemon-reload
sudo systemctl start bowst-md-soak
```

The tool prints one board line per instrument per second, which is about 1 GB of text over 72 hours with three pairs. Make sure journald keeps enough: set `SystemMaxUse=2G` in `/etc/systemd/journald.conf`, then run `sudo systemctl restart systemd-journald` before you start the run.

## 5. During the run

With `--metrics`, Prometheus and Grafana can watch the run and alert on it; see `monitoring.md`. The commands below work without them.

```sh
systemctl status bowst-md-soak                       # still running?
journalctl -u bowst-md-soak -f                       # live board
journalctl -u bowst-md-soak | grep '\[status\]'      # every lifecycle event
du -sh /var/lib/bowst/journal && df -h /var/lib/bowst

# Each status line with the elapsed time of the board line before it:
journalctl -u bowst-md-soak -o cat | awk '/^---/{t=$2} /\[status\]/{print t, $0}'
```

On a machine without systemd, read `soak.log` with the same `grep` and `awk` commands.

Expected events:
- **Every 23 hours:** a planned disconnect and reconnect (`disconnected: connection reached its maximum age`). Binance closes connections at 24 hours, so the session reconnects first, on its own schedule. The seamless handover that removes this gap is a known follow-up (ADR 0008).
- **Occasional `disconnected: connection closed by peer`, then `connecting`, `connected` and every pair `live` again:** the network connection ended without a WebSocket close frame. This is typically a router, ISP or venue load balancer dropping a long-lived connection. Reconnection with fresh snapshots is the correct response.
- **Occasional `DOWN` followed by `live`:** a sequence gap that was detected and resynchronized. This is the system working. The runbook (`docs/runbooks/market-data.md`) explains each status.

Investigate any of these:
- An instrument stays `DOWN` for more than a minute.
- Repeated `snapshot failed` messages.
- `journal health` is anything other than `Ok`.
- Disk usage grows faster than the sizing above.

## 6. After the run: acceptance

```sh
journalctl -u bowst-md-soak > soak.log               # keep with the results
systemctl show bowst-md-soak -p ExecMainStatus       # must be 0
bowst-md --replay /var/lib/bowst/journal             # must exit 0
```

The run passes when:
- [ ] The process ran the full 72 hours and exited with status 0.
- [ ] The summary line shows `journal health: Ok` and `0 dropped`.
- [ ] Replay exits 0, reports `0 records missing`, and does not report a torn final record.
- [ ] Every `DOWN` in `soak.log` is followed by `live` for the same instrument, and each has an explanation (a detected gap, a reconnect or a venue incident).
- [ ] The summary reports 4 connections (a planned reconnect every 23 hours), plus one for each explained disconnect.
- [ ] The `verification:` summary line shows `0 mismatched`, and `passed` is close to one per minute of run time (about 4,300 over 72 hours, fewer for time spent reconnecting). Any mismatch fails the run; see the runbook.
- [ ] The `latency:` summary line is recorded with the results. On a dedicated, busy-polling core its p99 must be at most 25 µs (ADR 0011); on a shared or sleeping machine it is recorded as indicative only.

Record the commit hash, instance type, region, pairs, start and end times, and these results in the PR or issue that closes Phase 1.

## 7. Keeping the journal

Copy the journal off the server before deleting it. It becomes a regression fixture: any bug found later can be replayed against it.

```sh
tar -C /var/lib/bowst -czf bowst-soak-$(date +%F).tar.gz journal
```

Store it in private storage the team controls, such as an S3 bucket with public access blocked. It contains only public market data, but journals from later phases will hold order data, so treat every journal as private from the start.

Stop the instance when you are done. A stopped instance still bills for its disk.
