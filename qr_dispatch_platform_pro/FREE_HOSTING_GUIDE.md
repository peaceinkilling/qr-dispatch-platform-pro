# Free Hosting Guide (No Monthly Subscription)

This project can run 24/7 with **no routine paid subscription** using:

- Oracle Cloud **Always Free** VM
- DuckDNS free subdomain
- Caddy (free HTTPS/auto TLS)

---

## 1) Accounts to create (all free)

1. Oracle Cloud account (Always Free tier)
2. DuckDNS account (for free fixed domain, e.g. `mycht.duckdns.org`)

No paid plan required.

---

## 2) Create the free VM

On Oracle Cloud:

- Create Compute instance (Ubuntu 22.04/24.04)
- Shape: Always Free eligible (2 OCPU / 1-2 GB RAM is enough)
- Open ports in security list:
  - `22` (SSH)
  - `80` (HTTP)
  - `443` (HTTPS)

---

## 3) SSH and install system packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip caddy
```

---

## 4) Upload project

Upload full folder `qr_dispatch_platform_pro` to server.

Then:

```bash
cd /path/to/qr_dispatch_platform_pro
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

---

## 5) App environment

Edit `.env`:

```env
PUBLIC_BASE_URL=https://your-subdomain.duckdns.org
SESSION_SECRET=put-a-long-random-string-here
ADMIN_USERNAME=cht_mgt
ADMIN_PASSWORD=Security@123
```

If you already have a production DB, upload `data/dispatch.db`.

---

## 6) Run app as service (gunicorn)

Create service file:

```bash
sudo nano /etc/systemd/system/qr-dispatch.service
```

Paste:

```ini
[Unit]
Description=QR Dispatch Platform
After=network.target

[Service]
User=www-data
WorkingDirectory=/path/to/qr_dispatch_platform_pro
EnvironmentFile=/path/to/qr_dispatch_platform_pro/.env
ExecStart=/path/to/qr_dispatch_platform_pro/.venv/bin/gunicorn app:app -k uvicorn.workers.UvicornWorker -w 2 -b 127.0.0.1:8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable qr-dispatch
sudo systemctl start qr-dispatch
sudo systemctl status qr-dispatch
```

---

## 7) DuckDNS setup

Create a DuckDNS domain like `yourname.duckdns.org` and note your DuckDNS token.

Create updater script:

```bash
mkdir -p ~/duckdns
cat > ~/duckdns/update.sh <<'EOF'
#!/usr/bin/env bash
echo url="https://www.duckdns.org/update?domains=YOUR_DOMAIN&token=YOUR_TOKEN&ip=" | curl -k -o ~/duckdns/duck.log -K -
EOF
chmod 700 ~/duckdns/update.sh
```

Cron every 5 minutes:

```bash
(crontab -l 2>/dev/null; echo "*/5 * * * * ~/duckdns/update.sh >/dev/null 2>&1") | crontab -
```

---

## 8) Caddy reverse proxy with HTTPS

Edit Caddyfile:

```bash
sudo nano /etc/caddy/Caddyfile
```

Use:

```caddy
your-subdomain.duckdns.org {
    reverse_proxy 127.0.0.1:8000
}
```

Reload:

```bash
sudo systemctl restart caddy
sudo systemctl status caddy
```

Caddy will automatically issue and renew SSL certs.

---

## 9) Verify

1. Open `https://your-subdomain.duckdns.org/admin/login`
2. Login with admin credentials
3. Open dashboard, create/share a CHT
4. Scan QR and verify public page opens without login

---

## 10) Important behavior in this codebase

- Public can access only:
  - `/dispatch/{token}`
  - `/qr/{token}.png`
  - `/static/*`
- Dashboard and admin routes require login
- QR links use `PUBLIC_BASE_URL` (so set it correctly before generating final QRs)

