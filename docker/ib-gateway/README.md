# IB Gateway — Docker

Runs IB Gateway via [gnzsnz/ib-gateway-docker](https://github.com/gnzsnz/ib-gateway-docker) with file-based password secrets and persistent settings.

## Setup

```bash
cd docker/ib-gateway
cp .env.example .env
# Edit .env — set TWS_USERID to your IB username
mkdir -p secrets
echo "YOUR_IB_PASSWORD" > secrets/ib_password.txt
```

## Start

```bash
docker compose up -d
```

## Complete 2FA

On first login (and after extended disconnections), IB requires two-factor authentication.

**Option A — VNC** (requires `VNC_SERVER_PASSWORD` set in `.env`):
1. Connect a VNC client to `localhost:5900`
2. Complete the IBKR 2FA prompt in the Gateway GUI

**Option B — IBKR mobile app**: Approve the 2FA push notification from your IBKR mobile app.

Subsequent restarts reuse the saved session and skip 2FA.

## Verify

```bash
docker compose ps          # STATUS should show "healthy" after ~2 minutes
docker compose logs -f     # Watch login progress
```

## Switch to live trading

Edit `.env`:
```
TRADING_MODE=live
```

Then restart:
```bash
docker compose up -d
```

## Port mapping

SOCAT inside the container relays from 0.0.0.0 to IB Gateway's localhost-only API:

| Host port | Container port | SOCAT relays to | Purpose |
|-----------|---------------|-----------------|---------|
| 4001 | 4003 | 127.0.0.1:4001 | Live trading API |
| 4002 | 4004 | 127.0.0.1:4002 | Paper trading API |
| 5900 | 5900 | — | VNC (opt-in) |

## Monitoring

- **VNC**: `localhost:5900` (set `VNC_SERVER_PASSWORD` in `.env` to enable)
- **Logs**: `docker compose logs -f`

## Stop

```bash
docker compose down
```

Gateway settings (Jts directory) are persisted in a Docker volume and survive container restarts.

---

## Cloud Deployment (Hetzner VPS + Tailscale)

The same `docker-compose.yml` runs unmodified on a cloud VPS. Tailscale provides WireGuard-encrypted, identity-based access to the IB Gateway TCP socket — no public ports exposed.

### Overview

- **VPS**: Hetzner CX22 (2 vCPU, 4GB RAM, 40GB SSD) ~$4-6/mo, region `ash` (Ashburn, VA — closest to IB servers)
- **Network**: Tailscale mesh VPN with MagicDNS hostname `ib-gateway`
- **Security**: All IB/VNC/SSH ports blocked on public interface; accessible only via Tailscale
- **Canonical endpoint**: `ib-gateway:4001` (MagicDNS, survives IP changes)
- **Cost**: ~$4-6/mo (Hetzner) + $0 (Tailscale free tier)

### Architecture

```
              HETZNER VPS
            +----------------------------------------------+
            |  PUBLIC INTERFACE: all ports blocked by ufw   |
            |  (SSH only via Tailscale, break-glass via     |
            |   Hetzner web console)                        |
            |                                               |
            |  TAILSCALE INTERFACE (tailscale0: 100.x.y.z)  |
            |    MagicDNS hostname: ib-gateway               |
            |    ufw allows: 22, 4001, 5900 on tailscale0   |
            |    socat proxy: tailscale-ip:4001 -> 127.0.0.1:4001
            |    socat proxy: tailscale-ip:5900 -> 127.0.0.1:5900
            |                                               |
            |  DOCKER (unchanged docker-compose.yml)        |
            |    127.0.0.1:4001 -> container:4003 (SOCAT)   |
            |    127.0.0.1:4002 -> container:4004 (paper)   |
            |    127.0.0.1:5900 -> container:5900 (VNC)     |
            |    volume: ib-gateway-settings -> /Jts         |
            +-------------------+---------------------------+
                                |
                     WireGuard encrypted tunnel
                                |
         +----------------------+----------------------+
         |                      |                      |
  Cloud Service          Cloud Service          Local Dev
  (daily_update)         (research)             Machine
  clientId=1             clientId=10            clientId=20
  tag:mdw-client         tag:mdw-client         group:admins
```

### Port chain (end-to-end)

1. IB Gateway binds to `127.0.0.1:4001` inside the container
2. SOCAT in the container relays container port `4003` -> `127.0.0.1:4001`
3. Docker Compose maps host `127.0.0.1:4001` -> container `4003`
4. A socat systemd service on the VPS forwards `<tailscale-ip>:4001` -> `127.0.0.1:4001`
5. Tailscale exposes the VPS as `ib-gateway` via MagicDNS
6. Clients connect to `ib-gateway:4001` via Tailscale tunnel

Docker port bindings remain on `127.0.0.1` (not `0.0.0.0`). A socat systemd service bridges Tailscale traffic to localhost. (`tailscale serve --tcp` adds TLS which is incompatible with IB's raw TCP protocol.)

### Prerequisites

- Tailscale account (free tier, tailscale.com)
- Hetzner Cloud account
- `hcloud` CLI: `brew install hcloud`
- Tailscale on local machine: `brew install tailscale`

### T0: Tailscale account and ACL setup

1. Enable MagicDNS: Tailscale admin -> Settings -> DNS
2. Enable HTTPS certificates: Settings -> DNS -> HTTPS Certificates (required for `tailscale serve`)
3. Configure ACL policy (see Tailscale ACL Policy section below)
4. Generate a single-use preauth key for the VPS:
   - Tailscale admin -> Settings -> Keys -> Generate auth key
   - Reusable=No, Ephemeral=No, Tags=`tag:ib-gateway`, Expiry=1 hour
   - Save without shell history exposure:
     ```bash
     cat > /tmp/ts-authkey.txt  # paste key, then Ctrl-D
     chmod 600 /tmp/ts-authkey.txt
     ```

### T1: Provision Hetzner VPS

```bash
hcloud server create \
  --name mdw-ib-gateway \
  --type cx22 \
  --image ubuntu-24.04 \
  --location ash \
  --ssh-key <your-ssh-key-name>
```

### T2: Harden VPS and install Tailscale

SSH into the VPS (`ssh root@<PUBLIC_IP>`):

```bash
# Create non-root admin user
adduser mdw --disabled-password --gecos ""
usermod -aG sudo mdw
echo "mdw ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/mdw
mkdir -p /home/mdw/.ssh
cp ~/.ssh/authorized_keys /home/mdw/.ssh/
chown -R mdw:mdw /home/mdw/.ssh
chmod 700 /home/mdw/.ssh && chmod 600 /home/mdw/.ssh/authorized_keys

# Harden SSH
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sshd -t && systemctl restart ssh

# Unattended security updates
apt-get update && apt-get install -y unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades

# Install Docker
curl -fsSL https://get.docker.com | sh
systemctl enable docker
usermod -aG docker mdw

# Install Tailscale
curl -fsSL https://tailscale.com/install.sh | sh

# Authenticate (transfer authkey file first: scp /tmp/ts-authkey.txt root@<PUBLIC_IP>:/tmp/)
export TS_AUTH_KEY=$(cat /tmp/ts-authkey.txt)
tailscale up --auth-key="$TS_AUTH_KEY" --hostname=ib-gateway --advertise-tags=tag:ib-gateway
unset TS_AUTH_KEY
rm /tmp/ts-authkey.txt

# Verify MagicDNS hostname
tailscale status

# TCP forwarding via socat systemd services
# (tailscale serve --tcp adds TLS, incompatible with IB's raw TCP protocol)
TSIP=$(tailscale ip -4)

sudo tee /etc/systemd/system/ib-gateway-proxy.service > /dev/null <<SOCAT
[Unit]
Description=IB Gateway TCP proxy (Tailscale -> localhost)
After=network.target tailscaled.service docker.service
[Service]
Type=simple
ExecStart=/usr/bin/socat TCP-LISTEN:4001,bind=${TSIP},reuseaddr,fork TCP:127.0.0.1:4001
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
SOCAT

sudo tee /etc/systemd/system/ib-gateway-vnc-proxy.service > /dev/null <<SOCAT
[Unit]
Description=IB Gateway VNC proxy (Tailscale -> localhost)
After=network.target tailscaled.service docker.service
[Service]
Type=simple
ExecStart=/usr/bin/socat TCP-LISTEN:5900,bind=${TSIP},reuseaddr,fork TCP:127.0.0.1:5900
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
SOCAT

sudo systemctl daemon-reload
sudo systemctl enable --now ib-gateway-proxy ib-gateway-vnc-proxy

# Firewall: block everything except Tailscale
ufw default deny incoming
ufw default allow outgoing
ufw allow in on tailscale0 to any port 22
ufw allow in on tailscale0 to any port 4001
ufw allow in on tailscale0 to any port 5900
ufw enable

reboot
```

After reboot, SSH via Tailscale and verify:

```bash
ssh mdw@ib-gateway
tailscale status                    # Connected
sudo systemctl status ib-gateway-proxy  # socat proxy active
sudo ufw status                     # tailscale0-only rules
```

Revoke the preauth key in Tailscale admin immediately after this step.

**Break-glass access**: If Tailscale is down, use the Hetzner web console (browser-based VNC) from the Hetzner Cloud dashboard.

### T3: Deploy Docker Compose

From local machine:

```bash
ssh mdw@ib-gateway 'mkdir -p ~/ib-gateway'
scp docker/ib-gateway/docker-compose.yml mdw@ib-gateway:~/ib-gateway/
scp docker/ib-gateway/.env.example mdw@ib-gateway:~/ib-gateway/.env
```

On the VPS (`ssh mdw@ib-gateway`):

```bash
cd ~/ib-gateway
mkdir -p secrets

# Edit .env: set TWS_USERID, TRADING_MODE=live, READ_ONLY_API=yes
# Create password secret
echo "<password>" > secrets/ib_password.txt
chmod 600 secrets/ib_password.txt

docker compose up -d
```

Wait ~2 minutes, then verify:

```bash
docker compose ps         # Should show "healthy"
docker compose logs --tail=50
```

For 2FA: approve via IBKR mobile app, or set `VNC_SERVER_PASSWORD` in `.env` and connect via `vnc://ib-gateway:5900`.

### T4: Verify from local dev machine

```bash
source ~/market-warehouse/.venv/bin/activate

tailscale status                          # Should show ib-gateway
nc -z ib-gateway 4001                     # TCP connectivity

python3 -c "
from ib_async import IB
ib = IB()
ib.connect('ib-gateway', 4001, clientId=99)
print('Server time:', ib.reqCurrentTime())
ib.disconnect()
"

python scripts/livewire_ingest.py historical --host ib-gateway --tickers AAPL
```

Negative test from a machine not on Tailscale: `nc -z <PUBLIC_IP> 4001` should fail.

### T5: Enroll clients

For each new client (cloud service or dev machine):

1. Generate a preauth key in Tailscale admin (Tags=`tag:mdw-client`, Reusable=No, Expiry=1 hour)
2. Install Tailscale on the client
3. Authenticate:
   ```bash
   export TS_AUTH_KEY=$(cat /path/to/key.txt)
   tailscale up --auth-key="$TS_AUTH_KEY" --hostname=<client-name> --advertise-tags=tag:mdw-client
   unset TS_AUTH_KEY
   rm /path/to/key.txt
   ```
4. Configure scripts:
   ```bash
   export MDW_IB_HOST=ib-gateway
   export MDW_IB_PORT=4001
   ```
5. Verify ACL enforcement:
   ```bash
   nc -z ib-gateway 4001   # Should succeed
   nc -z ib-gateway 5900   # Should FAIL (not admin)
   nc -z ib-gateway 22     # Should FAIL (not admin)
   ```
6. Revoke the preauth key in Tailscale admin

### T6: Cutover daily_update

Cold cutover only — IB allows one active session per login. Running two gateways causes session displacement.

```bash
# 1. Stop local gateway
~/ibc/bin/stop-secure-ibc-service.sh
# or: cd docker/ib-gateway && docker compose down

# 2. Verify cloud gateway
python3 -c "
from ib_async import IB
ib = IB()
ib.connect('ib-gateway', 4001, clientId=99)
print('Server time:', ib.reqCurrentTime())
ib.disconnect()
"

# 3. Test daily update
MDW_IB_HOST=ib-gateway python scripts/livewire_ingest.py daily --dry-run
MDW_IB_HOST=ib-gateway python scripts/livewire_ingest.py daily

# 4. Make permanent: set MDW_IB_HOST=ib-gateway in ~/market-warehouse/.env
```

### T7: Client ID allocation

| clientId | Purpose | Access |
|----------|---------|--------|
| 0 | Reserved (future write-path) | Read-write (future) |
| 1-9 | Production services (daily_update, backfill) | Read-only |
| 10-19 | Research / backtesting | Read-only |
| 20-31 | Local dev / ad-hoc | Read-only |

`READ_ONLY_API=yes` enforces read-only globally. Future write-path: separate gateway instance on a different port with `READ_ONLY_API=no`.

### Tailscale ACL policy

Configure in Tailscale admin console (Access Controls):

```jsonc
{
  "tagOwners": {
    "tag:ib-gateway": ["autogroup:admin"],
    "tag:mdw-client": ["autogroup:admin"]
  },
  "acls": [
    // Data warehouse clients -> IB Gateway API only
    { "action": "accept", "src": ["tag:mdw-client"], "dst": ["tag:ib-gateway:4001"] },
    // Admins -> VNC for 2FA and SSH for management
    { "action": "accept", "src": ["group:admins"], "dst": ["tag:ib-gateway:22,5900"] }
  ]
}
```

### Rollback to local gateway

```bash
unset MDW_IB_HOST   # Falls back to 127.0.0.1 default
~/ibc/bin/start-secure-ibc-service.sh

python3 -c "
from ib_async import IB
ib = IB()
ib.connect('127.0.0.1', 4001, clientId=99)
print('Server time:', ib.reqCurrentTime())
ib.disconnect()
"
```

### 2FA reauth runbook

If IB Gateway enters a 2FA loop (repeated restarts in logs):

1. Check logs: `ssh mdw@ib-gateway 'cd ~/ib-gateway && docker compose logs --tail=100'`
2. Set `VNC_SERVER_PASSWORD` in `.env` on VPS
3. Restart: `docker compose restart`
4. Connect VNC via Tailscale: `vnc://ib-gateway:5900`
5. Complete 2FA manually
6. Remove VNC password from `.env` and restart

### Volume backup

Use Hetzner server snapshots (no downtime):

```bash
hcloud server create-image --type snapshot --description "ib-gateway-$(date +%Y%m%d)" mdw-ib-gateway
```

Schedule weekly via cron or Hetzner API automation.

### Appendix: iptables DNAT fallback

If `tailscale serve --tcp` is not available in your Tailscale version:

```bash
iptables -t nat -A PREROUTING -i tailscale0 -p tcp --dport 4001 -j DNAT --to-destination 127.0.0.1:4001
iptables -t nat -A PREROUTING -i tailscale0 -p tcp --dport 5900 -j DNAT --to-destination 127.0.0.1:5900

sysctl -w net.ipv4.conf.tailscale0.route_localnet=1
echo "net.ipv4.conf.tailscale0.route_localnet=1" >> /etc/sysctl.conf

apt-get install -y iptables-persistent
netfilter-persistent save
```
