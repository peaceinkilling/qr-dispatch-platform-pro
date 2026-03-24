# Railway Deployment (Exact Steps)

This guide is for deploying this project on Railway Hobby plan.

---

## A) What to upload to GitHub

Upload the full `qr_dispatch_platform_pro` project, including:

- `app.py`
- `templates/`
- `static/`
- `requirements.txt`
- `Procfile`
- `railway.json`
- `README.md`

Do **not** upload:
- `.env` (already ignored)
- `.venv/` (already ignored)
- `data/` (now ignored; local DB stays local)

---

## B) Create GitHub repo and push

From project folder:

```bash
git init
git add .
git commit -m "Prepare app for Railway deployment"
git branch -M main
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```

If repo is already initialized, just:

```bash
git add .
git commit -m "Prepare app for Railway deployment"
git push
```

---

## C) Railway setup

1. Open Railway -> New Project -> Deploy from GitHub repo.
2. Select this repo.
3. Railway detects Python app and uses `Procfile`.

---

## D) Add persistent volume (important for SQLite)

In Railway project:

1. Open service settings.
2. Add Volume.
3. Mount path: `/data`

This keeps your database across redeploys/restarts.

---

## E) Add environment variables

In Railway service variables, add:

- `PUBLIC_BASE_URL=https://<your-app>.up.railway.app` (or your custom domain)
- `DISPATCH_DB_PATH=/data/dispatch.db`
- `SESSION_SECRET=<long-random-secret>`
- `ADMIN_USERNAME=cht_mgt`
- `ADMIN_PASSWORD=Security@123` (or your own password)

---

## F) Deploy and verify

1. Trigger deploy (or it auto-deploys after env updates).
2. Wait until status is healthy.
3. Open:
   - `https://<your-app>.up.railway.app/healthz` -> should return `{"ok": true}`
   - `https://<your-app>.up.railway.app/admin/login`
4. Login and create a CHT.
5. Open share/QR and test scan from phone.

---

## G) Custom domain (optional)

If you add your own domain in Railway:
1. Add domain in Railway settings.
2. Update DNS records as instructed by Railway.
3. Update `PUBLIC_BASE_URL` to your custom domain.
4. Redeploy once.

