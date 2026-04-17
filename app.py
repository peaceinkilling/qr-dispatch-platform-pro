
from __future__ import annotations

import functools
import io
import json
import random
import secrets
import sqlite3
import smtplib
import hashlib
from contextlib import closing
import csv
from datetime import date, datetime, timedelta, time
from pathlib import Path
from typing import Any, Optional
from email.message import EmailMessage

NATURE_ROMAN = {"1": "I", "2": "II", "3": "III", "4": "IV"}


def nature_to_roman(value: str) -> str:
    """Convert stored '1,3,4' to display 'I, III, IV'."""
    if not value:
        return ""
    parts = [p.strip() for p in (value or "").split(",") if p.strip()]
    def sort_key(p: str) -> int:
        return int(p) if p.isdigit() else 999

    return ", ".join(NATURE_ROMAN.get(p, p) for p in sorted(parts, key=sort_key))

import qrcode
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

import os
from urllib.parse import quote

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(
    os.environ.get(
        "DISPATCH_DB_PATH",
        str(BASE_DIR / "data" / "dispatch.db"),
    )
)


def _resolve_writable_db_path() -> Path:
    """
    Prefer DISPATCH_DB_PATH when the directory is writable (e.g. Railway volume at /data).
    If /data is missing or not writable, fall back to ./data under the app so the service
    still boots and healthchecks pass (data may reset on redeploy without a volume).
    """
    global DB_PATH
    chosen = Path(DB_PATH)
    try:
        chosen.parent.mkdir(parents=True, exist_ok=True)
        test = chosen.parent / ".write_test"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        return chosen
    except OSError:
        fallback = BASE_DIR / "data" / "dispatch.db"
        fallback.parent.mkdir(parents=True, exist_ok=True)
        DB_PATH = fallback
        return fallback

PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
# OpenStreetMap-based driving routes (OSRM). Set OSRM_ROUTING=false to use straight-line distance only.
OSRM_ROUTING_ENABLED = os.environ.get("OSRM_ROUTING", "true").strip().lower() in ("1", "true", "yes", "on")
OSRM_BASE_URL = os.environ.get("OSRM_BASE_URL", "https://router.project-osrm.org").strip().rstrip("/")
# Snap depot/station to nearest drivable edge before routing (reduces odd detours from off-network pins).
OSRM_SNAP_ENDPOINTS = os.environ.get("OSRM_SNAP_ENDPOINTS", "true").strip().lower() in ("1", "true", "yes", "on")

# Funds portal (admin-only UI + extra PIN). Change via FUNDS_PORTAL_PIN in production.
FUNDS_PORTAL_PIN = os.environ.get("FUNDS_PORTAL_PIN", "212141").strip()
FUNDS_ANNUAL_TABLE = "fund_annual_budget"
DISPATCH_TABLE = "dispatches"
LOCATIONS_TABLE = "locations"
PIN_CODES_TABLE = "pincodes"
DELETED_DISPATCHES_TABLE = "deleted_dispatches"
TRUSTED_ADMIN_DEVICES_TABLE = "trusted_admin_devices"
OSRM_ROUTE_CACHE_TABLE = "osrm_route_cache"

app = FastAPI(title="Dispatch QR Platform", version="3.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _jinja_tojson(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


templates.env.filters["tojson"] = _jinja_tojson

# ---- Admin authentication (simple session cookie) ----
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "cht_mgt")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Security@123")
ADMIN_2FA_EMAIL = os.environ.get("ADMIN_2FA_EMAIL", "peaceinkilling@gmail.com").strip()
ADMIN_REQUIRE_2FA = os.environ.get("ADMIN_REQUIRE_2FA", "false").strip().lower() in ("1", "true", "yes", "on")
ADMIN_ENTRY_TOKEN = os.environ.get(
    "ADMIN_ENTRY_TOKEN",
    "XgZt0dfEZYvptf4i-clhxsjz9SsK2hyk0vWRJX30K60",
).strip()

SMTP_HOST = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587").strip() or "587")
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "").strip()
SMTP_FROM_EMAIL = os.environ.get("SMTP_FROM_EMAIL", SMTP_USERNAME or ADMIN_2FA_EMAIL).strip()
ADMIN_TRUSTED_DEVICE_MAX_AGE_DAYS = int(
    os.environ.get("ADMIN_TRUSTED_DEVICE_MAX_AGE_DAYS", "1825").strip() or "1825"
)

# Must be stable across workers/processes. Random per-worker secrets break sessions on multi-worker hosts.
SESSION_SECRET = os.environ.get(
    "SESSION_SECRET",
    "O0nfjD9qFAsbsyWv45quyj3VyIya3yPzvbmnApoqFqc-vMVNOVkzBotfYSGf5neA",
)
SESSION_COOKIE_SECURE = PUBLIC_BASE_URL.startswith("https://") if PUBLIC_BASE_URL else False

class AdminGateMiddleware(BaseHTTPMiddleware):
    """
    Public pages:
      - /dispatch/{token}
      - /qr/{token}.png
      - /static/*
    Protected pages (admin only):
      - /
      - /admin/*
      - /export/*
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        is_public = (
            path.startswith("/dispatch/")
            or path.startswith("/qr/")
            or path.startswith("/static/")
            or path in ("/admin/login", "/admin/logout", "/admin/2fa", "/healthz")
        )

        if is_public:
            return await call_next(request)

        is_admin_protected = path == "/" or path.startswith("/admin") or path.startswith("/export")
        if not is_admin_protected:
            return await call_next(request)

        try:
            # Access session via scope to avoid AssertionError when SessionMiddleware
            # is not yet attached on this middleware hop.
            session = request.scope.get("session") or {}
            is_admin_authenticated = session.get("admin_authenticated") is True
            if not is_admin_authenticated:
                device_token = request.cookies.get("admin_device_token", "")
                if device_token and _is_trusted_device(device_token):
                    is_admin_authenticated = True
                    session["admin_authenticated"] = True
        except Exception:
            # Corrupt/old session cookie should never crash the app.
            # Treat it as logged out and force clean login.
            is_admin_authenticated = False

        if is_admin_authenticated:
            # Logged-in admin allowed pages:
            # - dashboard
            # - create new CHT
            # - share from dashboard
            # - mark delivered
            allowed_admin = (
                path == "/"
                or path.startswith("/admin/new")
                or path.startswith("/admin/edit")
                or path.startswith("/admin/delete")
                or path.startswith("/admin/share")
                or path.startswith("/admin/status/")
                or path.startswith("/admin/funds")
                or path.startswith("/export/")
                or path in ("/admin/login", "/admin/logout")
            )
            if not allowed_admin:
                return RedirectResponse(url="/", status_code=307)
            return await call_next(request)

        # Redirect unauthenticated admin navigation to login.
        next_path = path
        if request.url.query:
            next_path = f"{next_path}?{request.url.query}"
        login_url = _admin_login_url(next_path)
        return RedirectResponse(url=login_url, status_code=307)


app.add_middleware(AdminGateMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="admin_session",
    https_only=SESSION_COOKIE_SECURE,
    same_site="lax",
)


def _admin_login_url(next_path: str = "/admin/new") -> str:
    base = "/admin/login"
    query_parts = []
    if next_path:
        query_parts.append(f"next={quote(next_path, safe='')}")
    if ADMIN_ENTRY_TOKEN:
        query_parts.append(f"entry={quote(ADMIN_ENTRY_TOKEN, safe='')}")
    if query_parts:
        return f"{base}?{'&'.join(query_parts)}"
    return base


def get_public_base_url(request: Request) -> str:
    """
    Base URL used to generate QR links and share URLs.
    Prefer PUBLIC_BASE_URL so QR codes remain correct behind reverse proxies.
    """
    return PUBLIC_BASE_URL or str(request.base_url).rstrip("/")


def _normalize_otp(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def _hash_token(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _send_login_codes_email(one_time_code: str, device_code: str) -> tuple[bool, str]:
    """
    Sends two codes:
    - one-time code: current browser session only
    - device code: trust current device for long-term access
    """
    missing = []
    if not SMTP_HOST:
        missing.append("SMTP_HOST")
    if not SMTP_FROM_EMAIL:
        missing.append("SMTP_FROM_EMAIL")
    if not ADMIN_2FA_EMAIL:
        missing.append("ADMIN_2FA_EMAIL")
    if SMTP_USERNAME and not SMTP_PASSWORD:
        missing.append("SMTP_PASSWORD")
    if missing:
        return False, f"Email service is not configured. Missing: {', '.join(missing)}"
    msg = EmailMessage()
    msg["Subject"] = "CHT Admin Login Verification Codes"
    msg["From"] = SMTP_FROM_EMAIL
    msg["To"] = ADMIN_2FA_EMAIL
    msg.set_content(
        (
            "Use one of these login codes:\n\n"
            f"One-time access code: {one_time_code}\n"
            f"Trusted device code: {device_code}\n\n"
            "One-time code: grants access for this login only.\n"
            "Trusted device code: approves this device for future logins.\n\n"
            "Codes expire in 10 minutes."
        )
    )
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.starttls()
            if SMTP_USERNAME:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        return True, ""
    except Exception:
        return False, "Failed to send verification email."


def _register_trusted_device(token: str, user_agent: str) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {TRUSTED_ADMIN_DEVICES_TABLE}
            (token_hash, device_label, created_at, last_used_at, revoked_at)
            VALUES (?, ?, ?, ?, NULL)
            """,
            (_hash_token(token), (user_agent or "")[:240], now, now),
        )
        conn.commit()


def _is_trusted_device(token: str) -> bool:
    if not token:
        return False
    token_hash = _hash_token(token)
    with closing(get_conn()) as conn:
        row = conn.execute(
            f"""
            SELECT id FROM {TRUSTED_ADMIN_DEVICES_TABLE}
            WHERE token_hash = ? AND revoked_at IS NULL
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            return False
        conn.execute(
            f"UPDATE {TRUSTED_ADMIN_DEVICES_TABLE} SET last_used_at = ? WHERE token_hash = ?",
            (datetime.utcnow().isoformat(timespec="seconds"), token_hash),
        )
        conn.commit()
    return True


@app.get("/healthz")
def healthz():
    return {"ok": True}

STATUS_META = {
    "On Route": {"label": "On Route", "pill": "status-green", "dot": "bg-emerald-400"},
    "Delayed": {"label": "Delayed", "pill": "status-amber", "dot": "bg-amber-400"},
    "Delivered": {"label": "Delivered", "pill": "status-blue", "dot": "bg-sky-400"},
}

# Statuses allowed from the admin UI forms.
# "Delayed" is derived automatically from ETA and never set directly by the forms.
VALID_STATUSES = {"On Route", "Delivered"}

# Default CHT capacity (kg). Each dispatch can override this via `cht_capacity_weight_kg`.
CAPACITY_WEIGHT_KG = 5000.0

# Depot: Banar, Rajasthan (PIN 342027, near Jodhpur).
DEPOT_PINCODE = "342027"
DEPOT_LAT = 26.3412647  # fallback if pincode lookup fails (aligned with map pin)
DEPOT_LNG = 73.1539759
DEPOT_NAME = "Banar"


def _resolve_depot() -> None:
    """Resolve depot coordinates from pincode at startup."""
    global DEPOT_LAT, DEPOT_LNG, DEPOT_NAME
    pin = lookup_pincode(DEPOT_PINCODE)
    if pin:
        DEPOT_LAT = pin["lat"]
        DEPOT_LNG = pin["lng"]
        DEPOT_NAME = pin["place_name"]


def _apply_depot_env_override() -> None:
    """Optional fine-tuning of depot GPS (e.g. align with Google Maps pin) via DEPOT_LAT / DEPOT_LNG."""
    global DEPOT_LAT, DEPOT_LNG
    try:
        lat_s = os.environ.get("DEPOT_LAT")
        lng_s = os.environ.get("DEPOT_LNG")
        if lat_s:
            DEPOT_LAT = float(lat_s)
        if lng_s:
            DEPOT_LNG = float(lng_s)
    except (TypeError, ValueError):
        pass


# GPS lookup states for the public "Station/Location" view.
LOCATION_STATES = {
    "RJ": "Rajasthan",
    "MP": "Madhya Pradesh",
    "MH": "Maharashtra",
    "GJ": "Gujarat",
}

# In-memory cache for fast destination -> GPS lookup.
LOCATIONS_BY_TOKEN: dict[str, list[dict]] = {}
LOCATIONS_ALL: list[dict] = []


def _norm_text(value: str) -> str:
    return " ".join((value or "").strip().lower().replace(",", " ").split())


def load_locations_cache() -> None:
    """
    Loads all district/city GPS rows into memory so public pages can render quickly.
    """
    global LOCATIONS_BY_TOKEN, LOCATIONS_ALL
    LOCATIONS_BY_TOKEN = {}
    LOCATIONS_ALL = []
    with closing(get_conn()) as conn:
        try:
            rows = conn.execute(
                f"SELECT state_code, state_name, district_name, lat, lng FROM {LOCATIONS_TABLE}"
            ).fetchall()
        except sqlite3.OperationalError:
            return

    for r in rows:
        loc = {
            "state_code": r["state_code"],
            "state_name": r["state_name"],
            "district_name": r["district_name"],
            "lat": float(r["lat"]),
            "lng": float(r["lng"]),
        }
        LOCATIONS_ALL.append(loc)
        tokens = _norm_text(loc["district_name"]).split()
        # Index by up to first 3 tokens (handles most multi-word district names).
        for t in tokens[:3]:
            LOCATIONS_BY_TOKEN.setdefault(t, []).append(loc)


def lookup_location(destination: str) -> Optional[dict]:
    """
    Best-effort mapping from free-form destination text to a known district/city with GPS.
    Returns None if no confident match is found.
    """
    dest_norm = _norm_text(destination)
    if not dest_norm:
        return None

    dest_tokens = dest_norm.split()
    dest_tokens_set = set(dest_tokens)
    dest_last_token = dest_tokens[-1] if dest_tokens else ""
    candidates: list[dict] = []
    for t in dest_tokens[-5:]:
        candidates.extend(LOCATIONS_BY_TOKEN.get(t, []))
    if not candidates:
        candidates = LOCATIONS_ALL[:]

    # Hard safety: only allow GPS lookup from the India states we support.
    # This prevents bad/mismatched destination texts from resolving into Pakistan coordinates.
    allowed_states = {s.lower() for s in PINCODE_STATES}
    candidates = [
        loc
        for loc in candidates
        if str(loc.get("state_name", "")).strip().lower() in allowed_states
    ] or []

    best = None
    best_score = -1
    for loc in candidates:
        loc_name_norm = _norm_text(loc["district_name"])
        if not loc_name_norm:
            continue

        loc_tokens_set = set(loc_name_norm.split())
        common = dest_tokens_set & loc_tokens_set
        common_count = len(common)

        score = 0
        # Exact match is the strongest signal.
        if loc_name_norm == dest_norm:
            score = 100
        # If the location is a single-token name, matching that token is acceptable.
        elif len(loc_tokens_set) == 1 and common_count == 1:
            score = 8
        # Multi-token names require at least two matching tokens.
        elif common_count >= 2:
            score = common_count
        # Additional safe hint: last token matches the location tokens (helps with "HQ"/"Road" suffixes).
        elif common_count == 1 and dest_last_token in loc_tokens_set:
            score = 3

        if score == 0:
            continue

        if score > best_score:
            best_score = score
            best = loc

    # Prevent wrong/irrelevant matches from generating bad GPS pins.
    return best if best_score >= 3 else None


PINCODE_STATES = {"Rajasthan", "Madhya Pradesh", "Maharashtra", "Gujarat"}
PINCODE_STATES_LOWER = {s.lower() for s in PINCODE_STATES} | {
    # Also skip other common Indian state tokens so the PIN->place parser
    # never picks a state word as the city.
    "uttar pradesh", "haryana", "punjab", "delhi", "bihar", "jharkhand",
    "chhattisgarh", "karnataka", "tamil nadu", "telangana", "andhra pradesh",
    "kerala", "odisha", "west bengal", "assam", "goa", "himachal pradesh",
    "jammu and kashmir", "uttarakhand",
}
PINCODES_BY_PIN: dict[str, dict] = {}
PINCODES_GEOCODE_FAILED: set[str] = set()


def load_pincodes_cache() -> None:
    """
    Loads PINCODE -> GPS rows into memory so public pages can resolve quickly.
    """
    global PINCODES_BY_PIN
    PINCODES_BY_PIN = {}
    with closing(get_conn()) as conn:
        try:
            rows = conn.execute(
                f"""
                SELECT pincode, place_name, state_name, lat, lng
                FROM {PIN_CODES_TABLE}
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                """
            ).fetchall()
        except sqlite3.OperationalError:
            return

    for r in rows:
        pin = str(r["pincode"]).strip()
        if not pin:
            continue
        try:
            lat = float(r["lat"])
            lng = float(r["lng"])
        except (TypeError, ValueError):
            continue
        PINCODES_BY_PIN[pin] = {
            "pincode": pin,
            "place_name": r["place_name"],
            "state_name": r["state_name"],
            "lat": lat,
            "lng": lng,
        }


def normalize_pincode(value: str) -> str:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(digits) != 6:
        return ""
    return digits


def lookup_pincode_for_place(name: str) -> Optional[dict]:
    """
    Best-effort: match free-text place name to a known PIN row (for city → PIN hints on forms).
    """
    q = _norm_text(name)
    if len(q) < 2:
        return None
    exact: list[dict] = []
    partial: list[tuple[int, dict]] = []
    for pd in PINCODES_BY_PIN.values():
        pn = _norm_text(pd.get("place_name") or "")
        if not pn:
            continue
        if pn == q:
            exact.append(pd)
        elif q in pn or (len(pn) >= 4 and pn in q):
            partial.append((len(pn), pd))
    if exact:
        return exact[0]
    if partial:
        partial.sort(key=lambda x: (-x[0], x[1].get("pincode", "")))
        return partial[0][1]
    return None


def lookup_pincode_suggestions(name: str, limit: int = 8) -> list[dict]:
    """
    Return multiple station/place suggestions for predictive typing.
    Matches by place, state, and pincode fragments.
    """
    q_raw = (name or "").strip()
    q_norm = _norm_text(q_raw)
    q_digits = "".join(ch for ch in q_raw if ch.isdigit())
    if len(q_norm) < 2 and len(q_digits) < 2:
        return []

    scored: list[tuple[int, str, dict]] = []
    seen: set[tuple[str, str]] = set()
    for pd in PINCODES_BY_PIN.values():
        place = (pd.get("place_name") or "").strip()
        state = (pd.get("state_name") or "").strip()
        pin = (pd.get("pincode") or "").strip()
        if not place or not pin:
            continue

        key = (place.casefold(), pin)
        if key in seen:
            continue
        seen.add(key)

        pn = _norm_text(place)
        sn = _norm_text(state)
        score = 0

        if q_norm:
            if pn == q_norm:
                score = max(score, 120)
            elif pn.startswith(q_norm):
                score = max(score, 100)
            elif q_norm in pn:
                score = max(score, 80)
            elif sn.startswith(q_norm):
                score = max(score, 60)
            elif q_norm in sn:
                score = max(score, 45)

        if q_digits:
            if pin.startswith(q_digits):
                score = max(score, 95)
            elif q_digits in pin:
                score = max(score, 70)

        if score <= 0:
            continue
        scored.append((score, pin, pd))

    scored.sort(key=lambda x: (-x[0], x[1]))
    return [x[2] for x in scored[: max(1, min(limit, 20))]]


def lookup_pincode(pincode: str) -> Optional[dict]:
    pin = normalize_pincode(pincode)
    if not pin:
        return None
    cached = PINCODES_BY_PIN.get(pin)
    if cached:
        # Already resolved (lat/lng validated in cache load).
        return cached

    # Lazy geocode fallback to fill missing/NULL pincodes.
    # This keeps map markings working even when the DB has incomplete PIN data.
    if pin in PINCODES_GEOCODE_FAILED:
        return None

    resolved = _geocode_pincode_nominatim(pin)
    if not resolved:
        PINCODES_GEOCODE_FAILED.add(pin)
        return None

    # Update cache immediately for the current process.
    PINCODES_BY_PIN[pin] = resolved
    return resolved


def _geocode_pincode_nominatim(pin: str) -> Optional[dict]:
    """
    Resolve a 6-digit Indian pincode -> {pincode, place_name, state_name, lat, lng}.
    Uses OpenStreetMap Nominatim with caching into the local SQLite DB.
    """
    import json as _json
    import urllib.parse as _urlparse
    import urllib.request as _urlrequest

    query = f"{pin}, India"
    url = (
        "https://nominatim.openstreetmap.org/search"
        "?format=json&limit=1&countrycodes=IN"
        "&q=" + _urlparse.quote(query)
    )

    req = _urlrequest.Request(
        url,
        headers={
            # Avoid 403/429 by identifying a user agent.
            "User-Agent": "DispatchQR/1.0 (server-side geocoding for pincode lookup)",
            "Accept-Language": "en",
        },
        method="GET",
    )

    try:
        with _urlrequest.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", "ignore")
    except Exception:
        return None

    try:
        data = _json.loads(body)
    except Exception:
        return None

    if not isinstance(data, list) or not data:
        return None

    first = data[0]
    try:
        lat = float(first.get("lat"))
        lng = float(first.get("lon"))
    except (TypeError, ValueError):
        return None

    display = str(first.get("display_name") or "").strip()
    # Nominatim often returns the PIN itself as the first token (e.g. "344001, Barmer, Rajasthan, India").
    # Walk the tokens and pick the first meaningful human-readable name, skipping the PIN + stray digits.
    place_name = pin
    if display:
        for raw in display.split(","):
            tok = raw.strip()
            if not tok:
                continue
            if tok == pin or tok.isdigit():
                continue
            low = tok.lower()
            if low in ("india",) or low in PINCODE_STATES_LOWER:
                continue
            place_name = tok
            break

    # Best-effort state extraction from Nominatim display_name.
    state_name = ""
    lower = display.lower()
    for st in PINCODE_STATES:
        if st.lower() in lower:
            state_name = st
            break

    # If we couldn't extract a supported state, still store it so lat/lng resolution works.
    # (We load pincodes without state filtering.)
    state_name = state_name or "India"

    resolved = {
        "pincode": pin,
        "place_name": place_name,
        "state_name": state_name,
        "lat": lat,
        "lng": lng,
    }

    # Persist so we don't geocode repeatedly.
    try:
        with closing(get_conn()) as conn:
            conn.execute(
                f"""
                INSERT INTO {PIN_CODES_TABLE} (pincode, place_name, state_name, lat, lng)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(pincode) DO UPDATE SET
                  place_name = excluded.place_name,
                  state_name = excluded.state_name,
                  lat = excluded.lat,
                  lng = excluded.lng
                """,
                (pin, resolved["place_name"], resolved["state_name"], resolved["lat"], resolved["lng"]),
            )
            conn.commit()
    except Exception:
        # Even if DB persistence fails, keep runtime working.
        pass

    return resolved


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Haversine distance in kilometers between two lat/lng points.
    """
    from math import asin, cos, radians, sin, sqrt

    R = 6371.0
    d_lat = radians(lat2 - lat1)
    d_lng = radians(lng2 - lng1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


@functools.lru_cache(maxsize=1024)
def _osrm_nearest_cached(key: tuple[float, float]) -> Optional[tuple[float, float]]:
    """Snap a coordinate to the nearest point on the OSRM driving network."""
    if not OSRM_ROUTING_ENABLED:
        return None
    import urllib.error
    import urllib.request

    lat, lng = key
    url = f"{OSRM_BASE_URL}/nearest/v1/driving/{lng},{lat}?number=1"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "DispatchQR/1.0 (OSRM nearest road snap)"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = json.loads(resp.read().decode("utf-8", "ignore"))
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return None
    if body.get("code") != "Ok":
        return None
    wps = body.get("waypoints") or []
    if not wps:
        return None
    loc = wps[0].get("location")
    if not isinstance(loc, (list, tuple)) or len(loc) < 2:
        return None
    lng_r, lat_r = float(loc[0]), float(loc[1])
    return (lat_r, lng_r)


def osrm_snap_latlng(lat: float, lng: float) -> tuple[float, float]:
    if not OSRM_SNAP_ENDPOINTS:
        return (lat, lng)
    got = _osrm_nearest_cached((round(lat, 5), round(lng, 5)))
    if got:
        return got
    return (lat, lng)


def _osrm_route_cache_key(key: tuple[float, float, float, float]) -> str:
    lat1, lng1, lat2, lng2 = key
    return f"{lat1:.4f}:{lng1:.4f}:{lat2:.4f}:{lng2:.4f}"


def _osrm_route_db_get(key: tuple[float, float, float, float]) -> Optional[tuple[float, tuple[tuple[float, float], ...]]]:
    try:
        with closing(get_conn()) as conn:
            row = conn.execute(
                f"SELECT distance_km, latlngs_json FROM {OSRM_ROUTE_CACHE_TABLE} WHERE key = ?",
                (_osrm_route_cache_key(key),),
            ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    try:
        pts_raw = json.loads(row["latlngs_json"])
    except (TypeError, ValueError):
        return None
    pts: list[tuple[float, float]] = []
    for p in pts_raw or []:
        if isinstance(p, (list, tuple)) and len(p) >= 2:
            try:
                pts.append((float(p[0]), float(p[1])))
            except (TypeError, ValueError):
                continue
    if len(pts) < 2:
        return None
    return (float(row["distance_km"] or 0.0), tuple(pts))


def _osrm_route_db_put(key: tuple[float, float, float, float], dist_km: float, pts: tuple[tuple[float, float], ...]) -> None:
    try:
        with closing(get_conn()) as conn:
            conn.execute(
                f"""
                INSERT INTO {OSRM_ROUTE_CACHE_TABLE} (key, distance_km, latlngs_json, cached_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  distance_km = excluded.distance_km,
                  latlngs_json = excluded.latlngs_json,
                  cached_at = excluded.cached_at
                """,
                (
                    _osrm_route_cache_key(key),
                    float(dist_km),
                    json.dumps([[float(a), float(b)] for a, b in pts]),
                    datetime.utcnow().isoformat(timespec="seconds"),
                ),
            )
            conn.commit()
    except sqlite3.Error:
        # Cache persistence is best-effort — do not fail the request.
        pass


@functools.lru_cache(maxsize=768)
def _osrm_driving_route_cached(key: tuple[float, float, float, float]) -> Optional[tuple[float, tuple[tuple[float, float], ...]]]:
    """
    Call OSRM once per rounded depot/destination pair.
    Checks persistent DB cache first, then hits OSRM, then persists on success.
    Returns (distance_km, ((lat,lng), ...)) or None if routing fails.
    """
    # 1) Persistent cross-restart cache.
    cached = _osrm_route_db_get(key)
    if cached:
        return cached

    if not OSRM_ROUTING_ENABLED:
        return None
    import urllib.error
    import urllib.request

    lat1, lng1, lat2, lng2 = key
    url = (
        f"{OSRM_BASE_URL}/route/v1/driving/"
        f"{lng1},{lat1};{lng2},{lat2}"
        f"?overview=full&geometries=geojson&steps=false&alternatives=true&continue_straight=true"
    )
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "DispatchQR/1.0 (OSRM driving route; openstreetmap.org data)"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8", "ignore"))
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return None
    if body.get("code") != "Ok" or not body.get("routes"):
        return None
    routes = body.get("routes") or []
    if not routes:
        return None
    # Pick the fastest route by duration (fallback to distance if needed).
    r0 = min(
        routes,
        key=lambda r: (
            float(r.get("duration") or 1e18),
            float(r.get("distance") or 1e18),
        ),
    )
    dist_m = float(r0.get("distance") or 0.0)
    geom = r0.get("geometry") or {}
    coords = geom.get("coordinates") or []
    if dist_m <= 0 or len(coords) < 2:
        return None
    # GeoJSON is [lng, lat]; Leaflet wants [lat, lng]
    latlngs: list[tuple[float, float]] = []
    for c in coords:
        if not isinstance(c, (list, tuple)) or len(c) < 2:
            continue
        lng_a, lat_a = float(c[0]), float(c[1])
        latlngs.append((lat_a, lng_a))
    if len(latlngs) < 2:
        return None

    result = (dist_m / 1000.0, tuple(latlngs))
    # 2) Persist for the next restart and for peer processes.
    _osrm_route_db_put(key, result[0], result[1])
    return result


def osrm_driving_route(
    lat1: float, lng1: float, lat2: float, lng2: float
) -> Optional[dict[str, Any]]:
    """
    Driving distance and path along roads (OSRM / OpenStreetMap), or None on failure.
    Uses snapped road endpoints for the router, then anchors the polyline to the true depot/station pins.
    """
    slat1, slng1 = osrm_snap_latlng(lat1, lng1)
    slat2, slng2 = osrm_snap_latlng(lat2, lng2)
    k = (
        round(slat1, 4),
        round(slng1, 4),
        round(slat2, 4),
        round(slng2, 4),
    )
    got = _osrm_driving_route_cached(k)
    if not got:
        return None
    dist_km, pts_t = got
    pts: list[tuple[float, float]] = list(pts_t)

    def _near_m(a: tuple[float, float], b: tuple[float, float]) -> float:
        return haversine_km(a[0], a[1], b[0], b[1]) * 1000.0

    # Anchor to true Banar / station coordinates for map display (short connectors if needed).
    if pts:
        if _near_m((lat1, lng1), pts[0]) > 120:
            pts.insert(0, (lat1, lng1))
        if _near_m((lat2, lng2), pts[-1]) > 120:
            pts.append((lat2, lng2))
        if abs(pts[0][0] - lat1) > 1e-5 or abs(pts[0][1] - lng1) > 1e-5:
            pts[0] = (lat1, lng1)
        if abs(pts[-1][0] - lat2) > 1e-5 or abs(pts[-1][1] - lng2) > 1e-5:
            pts[-1] = (lat2, lng2)
    return {
        "distance_km": dist_km,
        "latlngs": [[lat, lng] for lat, lng in pts],
    }


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def parse_dt(value: str) -> Optional[datetime]:
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _validate_dispatch_timeline_or_400(dispatch_date: str, eta_date: str) -> None:
    dispatch_dt = parse_dt(dispatch_date or "")
    eta_dt = parse_dt(eta_date or "")
    if not dispatch_dt:
        raise HTTPException(status_code=400, detail="Invalid dispatch date/time.")
    if not eta_dt:
        raise HTTPException(status_code=400, detail="Invalid ETA date/time.")
    # Business rule: entry is created only after CHT has dispatched.
    if dispatch_dt > datetime.now():
        raise HTTPException(status_code=400, detail="Dispatch date/time cannot be in the future.")
    if eta_dt < dispatch_dt:
        raise HTTPException(status_code=400, detail="ETA cannot be earlier than dispatch date/time.")


def _parse_optional_inr_amount(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace("₹", "")
    if not s:
        return None
    try:
        v = float(s)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid CHT cost amount.")
    if v < 0:
        raise HTTPException(status_code=400, detail="CHT cost cannot be negative.")
    return v


def human_eta(value: str) -> str:
    dt = parse_dt(value)
    if not dt:
        return value
    # Treat "00:00" timestamps (often used for date-only inputs) as 12:00 PM for display.
    # This avoids showing a misleading "time remaining" starting from 12 AM.
    if dt.time() == time(0, 0) and dt.microsecond == 0:
        dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    delta = dt - datetime.now()
    if delta.total_seconds() <= 0:
        return "Due / arrived"
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    if days:
        return f"{days}d {hours}h left"
    if hours:
        return f"{hours}h {minutes}m left"
    return f"{minutes}m left"


def eta_deadline_dt(eta_date_str: str) -> Optional[datetime]:
    """Deadline for delay comparisons: end-of-day of the ETA date."""
    dt = parse_dt(eta_date_str or "")
    if not dt:
        return None
    # Always wait until the end of the ETA day before marking delayed.
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def compute_eta_urgency(item: dict) -> dict:
    """
    Rules (not delivered): overdue past deadline; ≤24h to deadline = urgent; ≤72h = soon.
    Delivered: no operational badge.
    """
    status = (item.get("status") or "").strip()
    if status in ("Delivered", "Delayed"):
        return {"level": "delivered", "label": "", "badge_class": ""}

    deadline = eta_deadline_dt(item.get("eta_date") or "")
    if not deadline:
        return {"level": "unknown", "label": "", "badge_class": ""}

    now = datetime.now()
    if now > deadline:
        return {
            "level": "overdue",
            "label": "Overdue",
            "badge_class": "eta-badge eta-badge--overdue",
        }
    delta = deadline - now
    if delta <= timedelta(hours=24):
        return {
            "level": "urgent",
            "label": "Due ≤24h",
            "badge_class": "eta-badge eta-badge--urgent",
        }
    if delta <= timedelta(hours=72):
        return {
            "level": "soon",
            "label": "Due soon",
            "badge_class": "eta-badge eta-badge--soon",
        }
    return {"level": "ok", "label": "", "badge_class": ""}


def fmt_display(value: str) -> str:
    dt = parse_dt(value)
    if not dt:
        return value
    # For date-only inputs, keep it clean and premium.
    if dt.time().hour == 0 and dt.time().minute == 0:
        return dt.strftime("%d %b %Y")
    return dt.strftime("%d %b %Y • %H:%M")


def normalize_mobile(value: str) -> str:
    # WhatsApp "wa.me" expects digits only with country code (no +).
    return "".join(ch for ch in (value or "") if ch.isdigit())


def hydrate_row(
    row: sqlite3.Row,
    *,
    public: bool,
    fetch_road_route: bool = False,
    include_funds: bool = False,
) -> dict:
    item = dict(row)
    raw_status = (item.get("status") or "").strip()
    effective_status = raw_status
    if raw_status == "Awaiting":
        # Back-compat: older rows may still be stored as "Awaiting".
        # Convert them to either "On Route" or "Delayed" based on ETA.
        deadline = eta_deadline_dt(item.get("eta_date") or "")
        now = datetime.now()
        effective_status = "Delayed" if deadline and now > deadline else "On Route"

    # Unknown statuses should never leak into the UI.
    if effective_status not in STATUS_META:
        effective_status = "On Route"

    item["status"] = effective_status
    item["status_meta"] = STATUS_META.get(effective_status, STATUS_META["On Route"])

    item["dispatch_display"] = fmt_display(item["dispatch_date"])
    item["eta_display"] = fmt_display(item["eta_date"])
    item["eta_remaining"] = human_eta(item["eta_date"])
    item["eta_urgency"] = compute_eta_urgency(item)

    try:
        weight = float(item.get("total_weight_kg") or 0)
    except (TypeError, ValueError):
        weight = 0.0
    item["weight_display"] = f"{weight:g} kg"
    capacity = 0.0
    try:
        capacity = float(item.get("cht_capacity_weight_kg") or 0)
    except (TypeError, ValueError):
        capacity = 0.0

    if capacity <= 0:
        capacity = CAPACITY_WEIGHT_KG

    if capacity > 0:
        load_percent = max(0.0, min(100.0, (weight / capacity) * 100.0))
    else:
        load_percent = 0.0

    item["load_percent"] = int(round(load_percent))
    item["capacity_weight_display"] = f"{capacity:g} kg"

    item["nature_of_items_display"] = nature_to_roman(item.get("nature_of_items") or "")

    item["driver_mobile_digits"] = normalize_mobile(item.get("driver_mobile"))
    item["driver_mobile_tel"] = (item.get("driver_mobile") or "").replace(" ", "")
    item["driver_mobile_tel_href"] = f"tel:{item['driver_mobile_tel']}"

    if public:
        # Never pass internal-only/confidential values to public templates.
        item.pop("internal_notes", None)
        item.pop("cht_cost_amount", None)
    elif not include_funds:
        # CHT cost is fund-head only (not main dashboard / overview / share UI).
        item.pop("cht_cost_amount", None)

    # Station/Location resolution (dispatch-level, safe for both admin and public).
    # "to" display: district name only (not detailed location). Priority: locations district > destination.
    pin = lookup_pincode(item.get("destination_pincode") or "")
    if pin:
        item["station_name"] = pin["place_name"]
        item["station_state_name"] = pin["state_name"]
        # For pincode: destination is typically district (e.g. Jaisalmer, Indore). Use it for "to" display.
        item["station_district"] = item.get("destination") or pin["place_name"]
        item["station_lat"] = pin["lat"]
        item["station_lng"] = pin["lng"]
    else:
        loc = lookup_location(item.get("destination") or "")
        if loc:
            item["station_name"] = loc["district_name"]
            item["station_state_name"] = loc["state_name"]
            item["station_district"] = loc["district_name"]
            item["station_lat"] = loc["lat"]
            item["station_lng"] = loc["lng"]
        else:
            item["station_name"] = item.get("destination")
            item["station_state_name"] = ""
            item["station_district"] = item.get("destination")
            item["station_lat"] = None
            item["station_lng"] = None
    item["to_display"] = item.get("station_district") or item.get("destination") or "—"

    item["route_latlngs"] = None
    item["distance_mode"] = "none"

    if item.get("station_lat") is not None and item.get("station_lng") is not None:
        item["station_coords_display"] = f"{item['station_lat']:.4f}, {item['station_lng']:.4f}"
        slat = float(item["station_lat"])
        slng = float(item["station_lng"])
        road: Optional[dict[str, Any]] = None
        if fetch_road_route:
            try:
                road = osrm_driving_route(DEPOT_LAT, DEPOT_LNG, slat, slng)
            except Exception:
                road = None
        if road:
            item["distance_km"] = road["distance_km"]
            item["distance_display"] = f"{road['distance_km']:.0f} km"
            item["distance_mode"] = "road"
            item["route_latlngs"] = road.get("latlngs")
        else:
            try:
                dist = haversine_km(DEPOT_LAT, DEPOT_LNG, slat, slng)
                item["distance_km"] = dist
                item["distance_display"] = f"~{dist:.0f} km"
                item["distance_mode"] = "air"
            except Exception:
                item["distance_display"] = ""
                item["distance_km"] = None
                item["distance_mode"] = "none"
    else:
        item["station_coords_display"] = ""
        item["distance_display"] = ""
        item["distance_km"] = None
    return item


def init_db() -> None:
    _resolve_writable_db_path()
    with closing(get_conn()) as conn:
        # Create table if missing, then add any missing columns (simple migration).
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dispatches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_token TEXT NOT NULL UNIQUE,
                vehicle_number TEXT NOT NULL,
                dispatch_date TEXT NOT NULL,
                destination TEXT NOT NULL,
                destination_pincode TEXT DEFAULT '',
                icn_number TEXT DEFAULT '',
                driver_name TEXT NOT NULL,
                driver_mobile TEXT NOT NULL,
                package_count INTEGER NOT NULL,
                package_weight_kg REAL NOT NULL DEFAULT 0,
                total_weight_kg REAL NOT NULL DEFAULT 0,
                cht_capacity_weight_kg REAL NOT NULL DEFAULT 5000.0,
                eta_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'On Route',
                internal_notes TEXT DEFAULT '',
                nature_of_items TEXT DEFAULT '',
                cht_cost_amount REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

        # Locations table stores district/city GPS used for the public Station/Location view.
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {LOCATIONS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_code TEXT NOT NULL,
                state_name TEXT NOT NULL,
                district_name TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                UNIQUE(state_code, district_name)
            );
            """
        )

        # PINCODE -> GPS mapping used for station/location resolution.
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {PIN_CODES_TABLE} (
                pincode TEXT PRIMARY KEY,
                place_name TEXT NOT NULL,
                state_name TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL
            );
            """
        )
        # Ensure depot (Banar, PIN 342027) is always resolvable; keep coords in sync with depot map pin.
        conn.execute(
            f"""
            INSERT INTO {PIN_CODES_TABLE} (pincode, place_name, state_name, lat, lng)
            VALUES ('342027', 'Banar', 'Rajasthan', 26.3412647, 73.1539759)
            ON CONFLICT(pincode) DO UPDATE SET
              place_name = excluded.place_name,
              state_name = excluded.state_name,
              lat = excluded.lat,
              lng = excluded.lng
            """
        )

        # Ensure key destination pincodes used by demo/setup are correct.
        # This prevents wrong station GPS pins (which look like "Pakistan" on the schematic/fallback map).
        # Also fills missing pincodes so markings appear for every CHT with a valid destination_pincode.
        pincode_fixes = [
            # Rajasthan
            ("302001", "Jaipur", "Rajasthan", 26.9124, 75.7873),
            ("301001", "Alwar", "Rajasthan", 27.5530, 76.6346),
            ("305001", "Ajmer", "Rajasthan", 26.4499, 74.6399),
            ("305601", "Nasirabad", "Rajasthan", 26.3076, 74.7336),
            ("307501", "Mount Abu", "Rajasthan", 24.5926, 72.7156),
            ("313001", "Udaipur", "Rajasthan", 24.5854, 73.7125),
            ("324001", "Kota", "Rajasthan", 25.2138, 75.8564),
            ("332001", "Sikar", "Rajasthan", 27.6094, 75.1399),
            ("334001", "Bikaner", "Rajasthan", 28.0222, 73.3119),
            ("344001", "Barmer", "Rajasthan", 25.7521, 71.3966),
            ("344032", "Utarlai", "Rajasthan", 25.8010, 71.5190),
            ("345001", "Jaisalmer", "Rajasthan", 26.9156, 70.9076),
            ("345023", "Pokhran", "Rajasthan", 26.9180, 71.9152),
            # Gujarat
            ("361001", "Jamnagar", "Gujarat", 22.4707, 70.0577),
            ("370001", "Bhuj", "Gujarat", 23.2420, 69.6669),
            ("380001", "Ahmedabad", "Gujarat", 23.2156, 72.7961),
            ("382421", "Gandhinagar", "Gujarat", 23.2156, 72.6369),
            ("390001", "Vadodara", "Gujarat", 22.3072, 73.1812),
            ("395001", "Surat", "Gujarat", 21.1702, 72.8311),
            # Madhya Pradesh
            ("452001", "Indore", "Madhya Pradesh", 22.7196, 75.8577),
            ("462001", "Bhopal", "Madhya Pradesh", 23.2599, 77.4126),
            ("474001", "Gwalior", "Madhya Pradesh", 26.2183, 78.1828),
            # Maharashtra
            ("411001", "Pune", "Maharashtra", 18.5204, 73.8567),
            ("431001", "Aurangabad", "Maharashtra", 19.8762, 75.3433),
            ("440001", "Nagpur", "Maharashtra", 21.1458, 79.0882),
            # Uttar Pradesh (demo)
            ("284001", "Jhansi", "Uttar Pradesh", 25.4484, 78.5685),
            ("284501", "Babina", "Uttar Pradesh", 25.2430, 78.4620),
        ]
        conn.executemany(
            f"""
            INSERT INTO {PIN_CODES_TABLE} (pincode, place_name, state_name, lat, lng)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(pincode) DO UPDATE SET
              place_name = excluded.place_name,
              state_name = excluded.state_name,
              lat = excluded.lat,
              lng = excluded.lng
            """,
            pincode_fixes,
        )

        # Log of deleted CHTs for record keeping.
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {DELETED_DISPATCHES_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_token TEXT NOT NULL,
                vehicle_number TEXT NOT NULL,
                dispatch_date TEXT NOT NULL,
                destination TEXT NOT NULL,
                destination_pincode TEXT DEFAULT '',
                icn_number TEXT DEFAULT '',
                driver_name TEXT NOT NULL,
                driver_mobile TEXT NOT NULL,
                package_count INTEGER NOT NULL,
                total_weight_kg REAL NOT NULL,
                deleted_at TEXT NOT NULL,
                remarks TEXT NOT NULL DEFAULT ''
            );
            """
        )
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {TRUSTED_ADMIN_DEVICES_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_hash TEXT NOT NULL UNIQUE,
                device_label TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                revoked_at TEXT DEFAULT NULL
            );
            """
        )

        existing_cols = {
            r["name"] for r in conn.execute("PRAGMA table_info(dispatches)").fetchall()
        }
        added_icn = "icn_number" not in existing_cols
        added_weight = "total_weight_kg" not in existing_cols
        added_package_weight = "package_weight_kg" not in existing_cols
        added_capacity_weight = "cht_capacity_weight_kg" not in existing_cols
        added_pincode = "destination_pincode" not in existing_cols
        added_nature = "nature_of_items" not in existing_cols
        if added_icn:
            conn.execute("ALTER TABLE dispatches ADD COLUMN icn_number TEXT DEFAULT ''")
        if added_weight:
            conn.execute(
                "ALTER TABLE dispatches ADD COLUMN total_weight_kg REAL NOT NULL DEFAULT 0"
            )
        if added_package_weight:
            conn.execute(
                "ALTER TABLE dispatches ADD COLUMN package_weight_kg REAL NOT NULL DEFAULT 0"
            )
        if added_capacity_weight:
            conn.execute(
                "ALTER TABLE dispatches ADD COLUMN cht_capacity_weight_kg REAL NOT NULL DEFAULT 5000.0"
            )
        if added_pincode:
            conn.execute(
                "ALTER TABLE dispatches ADD COLUMN destination_pincode TEXT DEFAULT ''"
            )
        if added_nature:
            conn.execute("ALTER TABLE dispatches ADD COLUMN nature_of_items TEXT DEFAULT ''")
        if "cht_cost_amount" not in existing_cols:
            conn.execute("ALTER TABLE dispatches ADD COLUMN cht_cost_amount REAL")

        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {FUNDS_ANNUAL_TABLE} (
                year INTEGER PRIMARY KEY,
                allocated_amount REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            """
        )

        # Persistent OSRM route cache so road polylines survive restarts
        # and every new dispatch contributes back to the shared cache.
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {OSRM_ROUTE_CACHE_TABLE} (
                key TEXT PRIMARY KEY,
                distance_km REAL NOT NULL,
                latlngs_json TEXT NOT NULL,
                cached_at TEXT NOT NULL
            );
            """
        )
        now_iso_seed = datetime.utcnow().isoformat(timespec="seconds")
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {FUNDS_ANNUAL_TABLE} (year, allocated_amount, updated_at)
            VALUES (?, 0, ?)
            """,
            (datetime.utcnow().year, now_iso_seed),
        )

        # Fixed public_token per CHT so QR codes are stable (no change on refresh).
        # Demo timelines: staggered dispatch dates and distance-aware ETA day ranges from Banar (342027).
        # Road distances on maps/QR still come from OSM coordinates + OSRM (not these day ranges).
        demo_seed_base = [
            {"public_token": "cht-gj08au8678", "vehicle_number": "GJ 08 AU 8678", "destination": "Jaisalmer", "destination_pincode": "345001", "icn_number": "ICN-CTH-226-10001", "driver_name": "Sattar Khan", "driver_mobile": "+91 98765 43210", "package_count": 95, "total_weight_kg": 4120.0, "nature_of_items": "1"},
            {"public_token": "cht-rj09ns1101", "vehicle_number": "RJ 09 NS 1101", "destination": "Nasirabad", "destination_pincode": "305601", "icn_number": "ICN-CTH-226-10002", "driver_name": "Ramesh Yadav", "driver_mobile": "+91 99887 76655", "package_count": 84, "total_weight_kg": 3760.0, "nature_of_items": "2"},
            {"public_token": "cht-rj11bm2202", "vehicle_number": "RJ 11 BM 2202", "destination": "Barmer", "destination_pincode": "344001", "icn_number": "ICN-CTH-226-10003", "driver_name": "Imran Ali", "driver_mobile": "+91 91234 56780", "package_count": 79, "total_weight_kg": 3555.0, "nature_of_items": "3"},
            {"public_token": "cht-gj10jm3303", "vehicle_number": "GJ 10 JM 3303", "destination": "Jamnagar", "destination_pincode": "361001", "icn_number": "ICN-CTH-226-10004", "driver_name": "Nafees Khan", "driver_mobile": "+91 90012 34567", "package_count": 88, "total_weight_kg": 3960.0, "nature_of_items": "1,3"},
            {"public_token": "cht-gj05mn1357", "vehicle_number": "GJ 05 MN 1357", "destination": "Vadodara", "destination_pincode": "390001", "icn_number": "ICN-CTH-226-10005", "driver_name": "Prakash Joshi", "driver_mobile": "+91 98765 77777", "package_count": 87, "total_weight_kg": 3915.0, "nature_of_items": "1,2"},
            {"public_token": "cht-rj18aj4404", "vehicle_number": "RJ 18 AJ 4404", "destination": "Ajmer", "destination_pincode": "305001", "icn_number": "ICN-CTH-226-10006", "driver_name": "Vikram Singh", "driver_mobile": "+91 98765 11111", "package_count": 92, "total_weight_kg": 4140.0, "nature_of_items": "2,4"},
            {"public_token": "cht-rj14ma5505", "vehicle_number": "RJ 14 MA 5505", "destination": "Mount Abu", "destination_pincode": "307501", "icn_number": "ICN-CTH-226-10007", "driver_name": "Mahendra Sharma", "driver_mobile": "+91 98765 22222", "package_count": 71, "total_weight_kg": 3185.0, "nature_of_items": "1,2,3"},
            {"public_token": "cht-rj22pk6606", "vehicle_number": "RJ 22 PK 6606", "destination": "Pokhran", "destination_pincode": "345023", "icn_number": "ICN-CTH-226-10008", "driver_name": "Rajesh Patel", "driver_mobile": "+91 98765 33333", "package_count": 83, "total_weight_kg": 3735.0, "nature_of_items": "4"},
            {"public_token": "cht-rj13cd5678", "vehicle_number": "RJ 13 CD 5678", "destination": "Udaipur", "destination_pincode": "313001", "icn_number": "ICN-CTH-226-10009", "driver_name": "Anil Verma", "driver_mobile": "+91 98765 44444", "package_count": 91, "total_weight_kg": 4095.0, "nature_of_items": "1,4"},
            {"public_token": "cht-gj12bh7707", "vehicle_number": "GJ 12 BH 7707", "destination": "Bhuj", "destination_pincode": "370001", "icn_number": "ICN-CTH-226-10010", "driver_name": "Suresh Deshmukh", "driver_mobile": "+91 98765 55555", "package_count": 86, "total_weight_kg": 3870.0, "nature_of_items": "2"},
            {"public_token": "cht-rj06al8808", "vehicle_number": "RJ 06 AL 8808", "destination": "Alwar", "destination_pincode": "301001", "icn_number": "ICN-CTH-226-10011", "driver_name": "Deepak Meena", "driver_mobile": "+91 98765 66666", "package_count": 90, "total_weight_kg": 4050.0, "nature_of_items": "3,4"},
            {"public_token": "cht-rj24ut9909", "vehicle_number": "RJ 24 UT 9909", "destination": "Utarlai", "destination_pincode": "344032", "icn_number": "ICN-CTH-226-10012", "driver_name": "Mohan Lal", "driver_mobile": "+91 98765 88888", "package_count": 80, "total_weight_kg": 3600.0, "nature_of_items": "2,3"},
            {"public_token": "cht-rj14kc9021", "vehicle_number": "RJ 14 KC 9021", "destination": "Ahmedabad", "destination_pincode": "380001", "icn_number": "ICN-CTH-226-10013", "driver_name": "Sanjay Rao", "driver_mobile": "+91 98765 99999", "package_count": 99, "total_weight_kg": 4455.0, "nature_of_items": "1,3,4"},
            {"public_token": "cht-mp09gh3456", "vehicle_number": "MP 09 GH 3456", "destination": "Bhopal", "destination_pincode": "462001", "icn_number": "ICN-CTH-226-10014", "driver_name": "Arvind Saxena", "driver_mobile": "+91 98260 33445", "package_count": 96, "total_weight_kg": 4320.0, "nature_of_items": "1,4"},
            {"public_token": "cht-gj18gn2020", "vehicle_number": "GJ 18 GN 2020", "destination": "Gandhinagar", "destination_pincode": "382421", "icn_number": "ICN-CTH-226-10015", "driver_name": "Hitesh Modi", "driver_mobile": "+91 98250 55667", "package_count": 81, "total_weight_kg": 3645.0, "nature_of_items": "2,3"},
            {"public_token": "cht-up76bb3030", "vehicle_number": "UP 76 BB 3030", "destination": "Babina", "destination_pincode": "284501", "icn_number": "ICN-CTH-226-10016", "driver_name": "Ravi Yadav", "driver_mobile": "+91 98370 77889", "package_count": 88, "total_weight_kg": 3960.0, "nature_of_items": "3"},
            {"public_token": "cht-mp07gw4040", "vehicle_number": "MP 07 GW 4040", "destination": "Gwalior", "destination_pincode": "474001", "icn_number": "ICN-CTH-226-10017", "driver_name": "Pankaj Tomar", "driver_mobile": "+91 97520 99001", "package_count": 93, "total_weight_kg": 4185.0, "nature_of_items": "1,2"},
            {"public_token": "cht-up93jh5050", "vehicle_number": "UP 93 JH 5050", "destination": "Jhansi", "destination_pincode": "284001", "icn_number": "ICN-CTH-226-10018", "driver_name": "Amit Dubey", "driver_mobile": "+91 94500 22334", "package_count": 85, "total_weight_kg": 3825.0, "nature_of_items": "4"},
            {"public_token": "cht-rj02kl2468", "vehicle_number": "RJ 02 KL 2468", "destination": "Kota", "destination_pincode": "324001", "icn_number": "ICN-CTH-226-10019", "driver_name": "Lokesh Gurjar", "driver_mobile": "+91 94600 44556", "package_count": 94, "total_weight_kg": 4230.0, "nature_of_items": "3,4"},
            {"public_token": "cht-rj01ab1234", "vehicle_number": "RJ 01 AB 1234", "destination": "Jaipur", "destination_pincode": "302001", "icn_number": "ICN-CTH-226-10020", "driver_name": "Harish Choudhary", "driver_mobile": "+91 94140 66778", "package_count": 101, "total_weight_kg": 4545.0, "nature_of_items": "1,2,4"},
            {"public_token": "cht-rj23sk6060", "vehicle_number": "RJ 23 SK 6060", "destination": "Sikar", "destination_pincode": "332001", "icn_number": "ICN-CTH-226-10021", "driver_name": "Bhagirath Singh", "driver_mobile": "+91 99280 88990", "package_count": 89, "total_weight_kg": 4005.0, "nature_of_items": "2"},
            {"public_token": "cht-rj08op8642", "vehicle_number": "RJ 08 OP 8642", "destination": "Bikaner", "destination_pincode": "334001", "icn_number": "ICN-CTH-226-10022", "driver_name": "Om Prakash", "driver_mobile": "+91 98290 10112", "package_count": 77, "total_weight_kg": 3465.0, "nature_of_items": "1,3"},
            {"public_token": "cht-gj03ef9012", "vehicle_number": "GJ 03 EF 9012", "destination": "Surat", "destination_pincode": "395001", "icn_number": "ICN-CTH-226-10023", "driver_name": "Dinesh Shah", "driver_mobile": "+91 98251 31415", "package_count": 86, "total_weight_kg": 3870.0, "nature_of_items": "3,4"},
            {"public_token": "cht-mp11in1616", "vehicle_number": "MP 11 IN 1616", "destination": "Indore", "destination_pincode": "452001", "icn_number": "ICN-CTH-226-10024", "driver_name": "Manish Joshi", "driver_mobile": "+91 93000 16171", "package_count": 91, "total_weight_kg": 4095.0, "nature_of_items": "1,2,3"},
        ]
        rng = random.Random(datetime.utcnow().date().isoformat())
        n_demo = len(demo_seed_base)
        delivered_n = max(3, min(n_demo // 3, 8))
        delayed_n = max(2, n_demo // 9)
        onroute_n = max(1, n_demo - delivered_n - delayed_n)
        status_pool = (
            ["Delivered"] * delivered_n
            + ["Delayed"] * delayed_n
            + ["On Route"] * onroute_n
        )
        rng.shuffle(status_pool)
        today_d = datetime.utcnow().date()
        demo_seed = []
        for idx, (base_item, seed_status) in enumerate(zip(demo_seed_base, status_pool)):
            pin = (base_item.get("destination_pincode") or "").strip()
            # Road-transit day ranges from Banar (342027) — rough, distance-aware for believable ETAs.
            if pin in ("345001", "344001", "344032", "345023", "305601"):
                dmin, dmax = 2, 5
            elif pin[:2] in ("30", "31", "32", "33"):
                dmin, dmax = 3, 7
            elif pin.startswith("38") or pin.startswith("39") or pin in ("361001", "370001", "382421"):
                dmin, dmax = 5, 10
            elif pin.startswith("46") or pin.startswith("45") or pin.startswith("47"):
                dmin, dmax = 7, 14
            elif pin.startswith("284"):
                dmin, dmax = 8, 16
            else:
                dmin, dmax = 4, 10
            transit = rng.randint(dmin, dmax)

            if seed_status == "Delivered":
                # Completed recently — ETA in the past, dispatched a transit-window earlier.
                eta_dt = today_d - timedelta(days=rng.randint(1, 18))
                dispatch_dt = eta_dt - timedelta(days=transit + rng.randint(0, 3))
            elif seed_status == "Delayed":
                # ETA just slipped into the past; still in transit.
                eta_dt = today_d - timedelta(days=rng.randint(1, 5))
                dispatch_dt = eta_dt - timedelta(days=transit + rng.randint(0, 2))
            else:
                # On Route — three realistic buckets:
                #   A) Just dispatched (today / yesterday), arriving next few days/weeks
                #   B) Dispatched this week, mid-transit, ETA this week or next
                #   C) Dispatched 1–2 weeks back, long-haul, ETA further out
                bucket = rng.random()
                if bucket < 0.30:
                    dispatch_dt = today_d - timedelta(days=rng.randint(0, 2))
                    eta_dt = dispatch_dt + timedelta(days=transit + rng.randint(0, 3))
                elif bucket < 0.70:
                    dispatch_dt = today_d - timedelta(days=rng.randint(3, 6))
                    eta_dt = dispatch_dt + timedelta(days=transit + rng.randint(0, 3))
                else:
                    dispatch_dt = today_d - timedelta(days=rng.randint(7, 13))
                    eta_dt = dispatch_dt + timedelta(days=transit + rng.randint(2, 6))
                # On-route rows must still have ETA strictly in the future.
                if eta_dt <= today_d:
                    eta_dt = today_d + timedelta(days=rng.randint(2, max(3, transit)))

            # Safety: ETA must be after dispatch and dispatch not in the future.
            if dispatch_dt > today_d:
                dispatch_dt = today_d
            if eta_dt <= dispatch_dt:
                eta_dt = dispatch_dt + timedelta(days=max(2, transit))

            seed_item = dict(base_item)
            seed_item["dispatch_date"] = dispatch_dt.isoformat()
            seed_item["eta"] = eta_dt.isoformat()
            seed_item["status"] = seed_status
            demo_seed.append(seed_item)

        count = conn.execute(f"SELECT COUNT(*) FROM {DISPATCH_TABLE}").fetchone()[0]
        existing_vehicles = {
            r[0]
            for r in conn.execute(f"SELECT vehicle_number FROM {DISPATCH_TABLE}").fetchall()
        }
        if count == 0:
            for item in demo_seed:
                token = item.get("public_token") or secrets.token_urlsafe(12)
                conn.execute(
                    """
                    INSERT INTO dispatches
                    (public_token, vehicle_number, dispatch_date, destination, destination_pincode, driver_name, driver_mobile,
                     package_count, total_weight_kg, eta_date, icn_number, status, internal_notes, nature_of_items, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        token,
                        item["vehicle_number"],
                        item["dispatch_date"],
                        item["destination"],
                        item.get("destination_pincode") or "",
                        item["driver_name"],
                        item["driver_mobile"],
                        item["package_count"],
                        item["total_weight_kg"],
                        item["eta"],
                        item["icn_number"],
                        item["status"],
                        item.get("remarks") or "",
                        item.get("nature_of_items") or "",
                        datetime.utcnow().isoformat(timespec="seconds"),
                        datetime.utcnow().isoformat(timespec="seconds"),
                    ),
                )
        else:
            # Add missing demo CHTs for existing DBs (stable vehicle_number + token list)
            for item in demo_seed:
                if item["vehicle_number"] in existing_vehicles:
                    continue
                token = item.get("public_token") or secrets.token_urlsafe(12)
                conn.execute(
                    """
                    INSERT INTO dispatches
                    (public_token, vehicle_number, dispatch_date, destination, destination_pincode, driver_name, driver_mobile,
                     package_count, total_weight_kg, eta_date, icn_number, status, internal_notes, nature_of_items, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        token,
                        item["vehicle_number"],
                        item["dispatch_date"],
                        item["destination"],
                        item.get("destination_pincode") or "",
                        item["driver_name"],
                        item["driver_mobile"],
                        item["package_count"],
                        item["total_weight_kg"],
                        item["eta"],
                        item["icn_number"],
                        item["status"],
                        item.get("remarks") or "",
                        item.get("nature_of_items") or "",
                        datetime.utcnow().isoformat(timespec="seconds"),
                        datetime.utcnow().isoformat(timespec="seconds"),
                    ),
                )
                existing_vehicles.add(item["vehicle_number"])
        if (added_icn or added_weight) and count > 0:
            # Populate new fields for existing demo records where values are blank/default.
            ids = [
                r["id"]
                for r in conn.execute(
                    f"SELECT id FROM {DISPATCH_TABLE} ORDER BY id DESC LIMIT ?",
                    (len(demo_seed),),
                ).fetchall()
            ][::-1]

            rows = conn.execute(
                f"SELECT id, icn_number, total_weight_kg FROM {DISPATCH_TABLE} WHERE id IN ({','.join(['?']*len(ids))})",
                tuple(ids),
            ).fetchall()
            row_by_id = {r["id"]: r for r in rows}

            for idx, pk in enumerate(ids):
                seed_item = demo_seed[idx] if idx < len(demo_seed) else None
                if not seed_item:
                    break
                r = row_by_id.get(pk)
                if not r:
                    continue
                # Only overwrite if still blank/default to avoid touching real user data.
                icn_blank = (r["icn_number"] or "").strip() == ""
                weight_default = float(r["total_weight_kg"] or 0) == 0.0
                if (added_icn and icn_blank) or (added_weight and weight_default):
                    conn.execute(
                        f"UPDATE {DISPATCH_TABLE} SET icn_number = ?, total_weight_kg = ? WHERE id = ?",
                        (seed_item["icn_number"], seed_item["total_weight_kg"], pk),
                    )

        # Migrate existing demo rows to fixed public_token (stable QR codes) and better utilization.
        for s in demo_seed:
            tok = s.get("public_token")
            if tok:
                conn.execute(
                    f"UPDATE {DISPATCH_TABLE} SET public_token = ? WHERE vehicle_number = ? AND public_token != ?",
                    (tok, s["vehicle_number"], tok),
                )
                conn.execute(
                    f"""
                    UPDATE {DISPATCH_TABLE}
                    SET package_count = ?,
                        total_weight_kg = ?,
                        dispatch_date = ?,
                        eta_date = ?,
                        status = ?,
                        icn_number = ?,
                        updated_at = ?
                    WHERE vehicle_number = ?
                    """,
                    (
                        s["package_count"],
                        s["total_weight_kg"],
                        s["dispatch_date"],
                        s["eta"],
                        s["status"],
                        s["icn_number"],
                        datetime.utcnow().isoformat(timespec="seconds"),
                        s["vehicle_number"],
                    ),
                )

        # Safe migration for demo rows: update "destination" to clean district/city names.
        demo_vehicles = [s["vehicle_number"] for s in demo_seed]
        conn.execute(
            f"""
            UPDATE {DISPATCH_TABLE}
            SET
                destination = CASE vehicle_number
                    WHEN 'GJ 08 AU 8678' THEN 'Jaisalmer'
                    WHEN 'RJ 09 NS 1101' THEN 'Nasirabad'
                    WHEN 'RJ 11 BM 2202' THEN 'Barmer'
                    WHEN 'GJ 10 JM 3303' THEN 'Jamnagar'
                    WHEN 'GJ 05 MN 1357' THEN 'Vadodara'
                    WHEN 'RJ 18 AJ 4404' THEN 'Ajmer'
                    WHEN 'RJ 14 MA 5505' THEN 'Mount Abu'
                    WHEN 'RJ 22 PK 6606' THEN 'Pokhran'
                    WHEN 'RJ 13 CD 5678' THEN 'Udaipur'
                    WHEN 'GJ 12 BH 7707' THEN 'Bhuj'
                    WHEN 'RJ 06 AL 8808' THEN 'Alwar'
                    WHEN 'RJ 24 UT 9909' THEN 'Utarlai'
                    WHEN 'RJ 14 KC 9021' THEN 'Ahmedabad'
                    WHEN 'MP 09 GH 3456' THEN 'Bhopal'
                    WHEN 'GJ 18 GN 2020' THEN 'Gandhinagar'
                    WHEN 'UP 76 BB 3030' THEN 'Babina'
                    WHEN 'MP 07 GW 4040' THEN 'Gwalior'
                    WHEN 'UP 93 JH 5050' THEN 'Jhansi'
                    WHEN 'RJ 02 KL 2468' THEN 'Kota'
                    WHEN 'RJ 01 AB 1234' THEN 'Jaipur'
                    WHEN 'RJ 23 SK 6060' THEN 'Sikar'
                    WHEN 'RJ 08 OP 8642' THEN 'Bikaner'
                    WHEN 'GJ 03 EF 9012' THEN 'Surat'
                    WHEN 'MP 11 IN 1616' THEN 'Indore'
                    ELSE destination
                END,
                destination_pincode = CASE vehicle_number
                    WHEN 'GJ 08 AU 8678' THEN '345001'
                    WHEN 'RJ 09 NS 1101' THEN '305601'
                    WHEN 'RJ 11 BM 2202' THEN '344001'
                    WHEN 'GJ 10 JM 3303' THEN '361001'
                    WHEN 'GJ 05 MN 1357' THEN '390001'
                    WHEN 'RJ 18 AJ 4404' THEN '305001'
                    WHEN 'RJ 14 MA 5505' THEN '307501'
                    WHEN 'RJ 22 PK 6606' THEN '345023'
                    WHEN 'RJ 13 CD 5678' THEN '313001'
                    WHEN 'GJ 12 BH 7707' THEN '370001'
                    WHEN 'RJ 06 AL 8808' THEN '301001'
                    WHEN 'RJ 24 UT 9909' THEN '344032'
                    WHEN 'RJ 14 KC 9021' THEN '380001'
                    WHEN 'MP 09 GH 3456' THEN '462001'
                    WHEN 'GJ 18 GN 2020' THEN '382421'
                    WHEN 'UP 76 BB 3030' THEN '284501'
                    WHEN 'MP 07 GW 4040' THEN '474001'
                    WHEN 'UP 93 JH 5050' THEN '284001'
                    WHEN 'RJ 02 KL 2468' THEN '324001'
                    WHEN 'RJ 01 AB 1234' THEN '302001'
                    WHEN 'RJ 23 SK 6060' THEN '332001'
                    WHEN 'RJ 08 OP 8642' THEN '334001'
                    WHEN 'GJ 03 EF 9012' THEN '395001'
                    WHEN 'MP 11 IN 1616' THEN '452001'
                    ELSE destination_pincode
                END
            WHERE vehicle_number IN ({','.join(repr(v) for v in demo_vehicles)})
            """
        )

        # Populate sample "type of stores" (nature_of_items) for demo rows only when blank.
        # This keeps existing user-created data intact.
        nature_cases = "\n".join(
            f"WHEN {repr(s['vehicle_number'])} THEN {repr(s.get('nature_of_items') or '')}"
            for s in demo_seed
        )
        demo_vehicle_list = ",".join(repr(s["vehicle_number"]) for s in demo_seed)
        conn.execute(
            f"""
            UPDATE {DISPATCH_TABLE}
            SET nature_of_items = CASE vehicle_number
            {nature_cases}
            ELSE nature_of_items
            END
            WHERE vehicle_number IN ({demo_vehicle_list})
              AND (nature_of_items IS NULL OR TRIM(nature_of_items) = '')
            """
        )
        conn.execute(
            f"DELETE FROM {DISPATCH_TABLE} WHERE vehicle_number = ?",
            ("JK 02 HP 1010",),
        )
        # Remove legacy Jodhpur demo row (too close to depot); replaced by Sikar in demo_seed_base.
        conn.execute(
            f"DELETE FROM {DISPATCH_TABLE} WHERE vehicle_number = ?",
            ("RJ 03 JD 6060",),
        )
        conn.commit()


STATUS_SYNC_INTERVAL_SECONDS = 300
_LAST_STATUS_SYNC_AT: Optional[datetime] = None


def _normalize_future_dispatch_dates() -> None:
    """
    Safety cleanup: dispatches should never be stored in the future.
    If older data contains future dispatch timestamps, clamp them to "now".
    """
    now = datetime.now().replace(microsecond=0)
    now_iso = now.isoformat(sep=" ", timespec="seconds")
    with closing(get_conn()) as conn:
        rows = conn.execute(
            f"SELECT id, dispatch_date, eta_date FROM {DISPATCH_TABLE}"
        ).fetchall()
        changed = False
        for r in rows:
            ddt = parse_dt(r["dispatch_date"] or "")
            if not ddt or ddt <= now:
                continue
            eta = parse_dt(r["eta_date"] or "")
            # Preserve a valid sequence: ETA should not be earlier than dispatch.
            eta_out = now_iso
            if eta and eta >= now:
                eta_out = eta.isoformat(sep=" ", timespec="seconds")
            conn.execute(
                f"UPDATE {DISPATCH_TABLE} SET dispatch_date = ?, eta_date = ?, updated_at = ? WHERE id = ?",
                (now_iso, eta_out, now_iso, r["id"]),
            )
            changed = True
        if changed:
            conn.commit()


def sync_delayed_statuses() -> None:
    """
    Keep dispatch `status` consistent with ETA.

    - If not Delivered and ETA day has ended => mark as Delayed
    - Otherwise => mark as On Route

    This also guarantees there are no lingering "Awaiting" rows.
    """
    global _LAST_STATUS_SYNC_AT
    now = datetime.now()
    _normalize_future_dispatch_dates()
    # Always keep the statuses correct for the dashboard/public pages.
    # (We still keep the variable in case you want to tune later.)
    if STATUS_SYNC_INTERVAL_SECONDS and _LAST_STATUS_SYNC_AT is not None:
        if (now - _LAST_STATUS_SYNC_AT).total_seconds() < STATUS_SYNC_INTERVAL_SECONDS:
            return
    with closing(get_conn()) as conn:
        rows = conn.execute(
            f"SELECT id, status, eta_date FROM {DISPATCH_TABLE} WHERE status != 'Delivered'"
        ).fetchall()
        updates: list[tuple[str, str]] = []
        for r in rows:
            deadline = eta_deadline_dt(r["eta_date"])
            new_status = "Delayed" if deadline and now > deadline else "On Route"
            if r["status"] != new_status:
                updates.append((new_status, r["id"]))

        if updates:
            now_iso = now.isoformat(timespec="seconds")
            for new_status, row_id in updates:
                conn.execute(
                    f"UPDATE {DISPATCH_TABLE} SET status = ?, updated_at = ? WHERE id = ?",
                    (new_status, now_iso, row_id),
                )
            conn.commit()
    _LAST_STATUS_SYNC_AT = now


@app.on_event("startup")
def startup() -> None:
    init_db()
    load_locations_cache()
    load_pincodes_cache()
    _resolve_depot()
    _apply_depot_env_override()
    sync_delayed_statuses()


DISPATCH_SORT_ORDER_MAP = {
    "dispatch_desc": "datetime(dispatch_date) DESC, id DESC",
    "dispatch_asc": "datetime(dispatch_date) ASC, id ASC",
    "eta_desc": "datetime(eta_date) DESC, id DESC",
    "eta_asc": "datetime(eta_date) ASC, id ASC",
    "status": (
        "CASE status WHEN 'Delayed' THEN 1 WHEN 'On Route' THEN 2 WHEN 'Delivered' THEN 3 ELSE 9 END, "
        "datetime(dispatch_date) DESC, id DESC"
    ),
    "vehicle_asc": "LOWER(vehicle_number) ASC, id DESC",
    "weight_desc": "total_weight_kg DESC, id DESC",
    "weight_asc": "total_weight_kg ASC, id DESC",
    "updated_desc": "datetime(updated_at) DESC, id DESC",
}


def query_dispatches_hydrated(
    conn: sqlite3.Connection,
    *,
    q: Optional[str] = None,
    status: Optional[str] = None,
    destination: Optional[str] = None,
    vehicle_number: Optional[str] = None,
    dispatch_date: Optional[str] = None,
    dispatch_date_from: Optional[str] = None,
    dispatch_date_to: Optional[str] = None,
    distance_range: Optional[str] = None,
    weight_min: Optional[str] = None,
    weight_max: Optional[str] = None,
    sort: Optional[str] = None,
) -> list[dict]:
    sql = f"SELECT * FROM {DISPATCH_TABLE} WHERE 1=1"
    params: list[object] = []
    if q:
        sql += (
            " AND ("
            "LOWER(vehicle_number) LIKE LOWER(?) OR "
            "LOWER(COALESCE(destination,'')) LIKE LOWER(?) OR "
            "LOWER(driver_name) LIKE LOWER(?) OR "
            "LOWER(COALESCE(driver_mobile,'')) LIKE LOWER(?) OR "
            "LOWER(COALESCE(icn_number,'')) LIKE LOWER(?) OR "
            "LOWER(COALESCE(destination_pincode,'')) LIKE LOWER(?)"
            ")"
        )
        like = f"%{q}%"
        params.extend([like, like, like, like, like, like])
    if destination:
        sql += " AND LOWER(COALESCE(destination,'')) LIKE LOWER(?)"
        params.append(f"%{destination}%")
    if vehicle_number:
        sql += " AND LOWER(vehicle_number) LIKE LOWER(?)"
        params.append(f"%{vehicle_number}%")
    if dispatch_date:
        sql += " AND dispatch_date = ?"
        params.append(dispatch_date)
    if dispatch_date_from:
        sql += " AND dispatch_date >= ?"
        params.append(dispatch_date_from)
    if dispatch_date_to:
        sql += " AND dispatch_date <= ?"
        params.append(dispatch_date_to)
    if status and status != "All":
        sql += " AND status = ?"
        params.append(status)

    if weight_min:
        try:
            wmin = float(weight_min)
            sql += " AND total_weight_kg >= ?"
            params.append(wmin)
        except (TypeError, ValueError):
            pass
    if weight_max:
        try:
            wmax = float(weight_max)
            sql += " AND total_weight_kg <= ?"
            params.append(wmax)
        except (TypeError, ValueError):
            pass

    sort_key = (sort or "dispatch_desc").strip()
    sql += " ORDER BY " + DISPATCH_SORT_ORDER_MAP.get(
        sort_key, DISPATCH_SORT_ORDER_MAP["dispatch_desc"]
    )
    rows = [hydrate_row(r, public=False) for r in conn.execute(sql, params).fetchall()]

    if distance_range and distance_range != "All":

        def in_range(d: dict) -> bool:
            km = d.get("distance_km")
            if km is None:
                return False
            try:
                km_f = float(km)
            except Exception:
                return False

            if distance_range == "0-50":
                return km_f <= 50
            if distance_range == "50-150":
                return 50 < km_f <= 150
            if distance_range == "150+":
                return km_f > 150
            return True

        rows = [r for r in rows if in_range(r)]
    return rows


@app.get("/truck-image")
def truck_image():
    image_path = BASE_DIR / "fdeca998-e75f-4fd6-8189-b96d8652d0dd-removebg-preview.png"
    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Truck image not found")
    return FileResponse(str(image_path), media_type="image/png")


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request, next: str = "/admin/new", entry: str = ""):
    if ADMIN_ENTRY_TOKEN and entry != ADMIN_ENTRY_TOKEN:
        raise HTTPException(status_code=404, detail="Not found")
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "error": "",
            "next": next,
            "entry": entry,
        },
    )


@app.post("/admin/login")
def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/admin/new"),
    entry: str = Form(""),
):
    if ADMIN_ENTRY_TOKEN and entry != ADMIN_ENTRY_TOKEN:
        raise HTTPException(status_code=404, detail="Not found")

    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        if not ADMIN_REQUIRE_2FA:
            request.session["admin_authenticated"] = True
            request.session["admin_user"] = username
            request.session["admin_2fa_pending"] = False
            return RedirectResponse(url=next or "/admin/new", status_code=303)

        one_time_code = f"{random.randint(0, 999999):06d}"
        device_code = f"{random.randint(0, 999999):06d}"
        sent, msg = _send_login_codes_email(one_time_code, device_code)
        request.session["admin_authenticated"] = False
        request.session["admin_user"] = username
        request.session["admin_2fa_pending"] = True
        request.session["admin_2fa_next"] = next or "/admin/new"
        request.session["admin_2fa_code_one_time"] = one_time_code
        request.session["admin_2fa_code_device"] = device_code
        request.session["admin_2fa_expires_at"] = (
            datetime.utcnow() + timedelta(minutes=10)
        ).isoformat(timespec="seconds")
        request.session["admin_2fa_email_status"] = "sent" if sent else (msg or "failed")
        return RedirectResponse(url="/admin/2fa", status_code=303)

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "error": "Invalid credentials.",
            "next": next,
            "entry": entry,
        },
        status_code=401,
    )


@app.get("/admin/2fa", response_class=HTMLResponse)
def admin_2fa_page(request: Request):
    if not ADMIN_REQUIRE_2FA:
        return RedirectResponse(url="/admin/new", status_code=303)
    if request.session.get("admin_authenticated") is True:
        return RedirectResponse(url="/admin/new", status_code=303)
    if request.session.get("admin_2fa_pending") is not True:
        return RedirectResponse(url="/admin/login", status_code=303)
    return templates.TemplateResponse(
        request,
        "admin_2fa.html",
        {
            "request": request,
            "error": "",
            "email_status": request.session.get("admin_2fa_email_status", ""),
            "admin_2fa_email": ADMIN_2FA_EMAIL,
        },
    )


@app.post("/admin/2fa")
def admin_2fa_submit(request: Request, otp_code: str = Form("")):
    if not ADMIN_REQUIRE_2FA:
        return RedirectResponse(url="/admin/new", status_code=303)
    if request.session.get("admin_2fa_pending") is not True:
        return RedirectResponse(url=_admin_login_url(), status_code=303)

    code = _normalize_otp(otp_code)
    expires_at_raw = request.session.get("admin_2fa_expires_at", "")
    one_time_code = request.session.get("admin_2fa_code_one_time", "")
    device_code = request.session.get("admin_2fa_code_device", "")
    expired = False
    try:
        exp_dt = datetime.fromisoformat(expires_at_raw) if expires_at_raw else None
        expired = bool(exp_dt and datetime.utcnow() > exp_dt)
    except Exception:
        expired = True

    if expired:
        request.session["admin_2fa_pending"] = False
        request.session.pop("admin_2fa_code_one_time", None)
        request.session.pop("admin_2fa_code_device", None)
        request.session.pop("admin_2fa_expires_at", None)
        return templates.TemplateResponse(
            request,
            "admin_2fa.html",
            {
                "request": request,
                "error": "Code expired. Please login again.",
                "email_status": request.session.get("admin_2fa_email_status", ""),
                "admin_2fa_email": ADMIN_2FA_EMAIL,
            },
            status_code=401,
        )

    mode = ""
    if code and code == str(one_time_code):
        mode = "one_time"
    elif code and code == str(device_code):
        mode = "device"

    if not mode:
        return templates.TemplateResponse(
            request,
            "admin_2fa.html",
            {
                "request": request,
                "error": "Invalid verification code.",
                "email_status": request.session.get("admin_2fa_email_status", ""),
                "admin_2fa_email": ADMIN_2FA_EMAIL,
            },
            status_code=401,
        )

    next_url = request.session.get("admin_2fa_next") or "/admin/new"
    request.session["admin_authenticated"] = True
    request.session["admin_2fa_pending"] = False
    request.session.pop("admin_2fa_next", None)
    request.session.pop("admin_2fa_code_one_time", None)
    request.session.pop("admin_2fa_code_device", None)
    request.session.pop("admin_2fa_expires_at", None)

    response = RedirectResponse(url=next_url, status_code=303)
    if mode == "device":
        device_token = secrets.token_urlsafe(32)
        _register_trusted_device(device_token, request.headers.get("user-agent", ""))
        response.set_cookie(
            "admin_device_token",
            device_token,
            max_age=60 * 60 * 24 * max(1, ADMIN_TRUSTED_DEVICE_MAX_AGE_DAYS),
            httponly=True,
            secure=SESSION_COOKIE_SECURE,
            samesite="lax",
            path="/",
        )
    return response


@app.get("/admin/logout", response_class=HTMLResponse)
def admin_logout(request: Request):
    try:
        request.session.clear()
    except Exception:
        pass
    response = RedirectResponse(url=_admin_login_url(), status_code=303)
    response.delete_cookie("admin_device_token", path="/")
    return response


@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    q: Optional[str] = None,
    status: Optional[str] = None,
    destination: Optional[str] = None,
    vehicle_number: Optional[str] = None,
    dispatch_date: Optional[str] = None,
    dispatch_date_from: Optional[str] = None,
    dispatch_date_to: Optional[str] = None,
    distance_range: Optional[str] = None,
    weight_min: Optional[str] = None,
    weight_max: Optional[str] = None,
    sort: Optional[str] = None,
):
    try:
        sync_delayed_statuses()
        sort_key = (sort or "dispatch_desc").strip()
        with closing(get_conn()) as conn:
            rows = query_dispatches_hydrated(
                conn,
                q=q,
                status=status,
                destination=destination,
                vehicle_number=vehicle_number,
                dispatch_date=dispatch_date,
                dispatch_date_from=dispatch_date_from,
                dispatch_date_to=dispatch_date_to,
                distance_range=distance_range,
                weight_min=weight_min,
                weight_max=weight_max,
                sort=sort_key,
            )
            stats = {
                "all": conn.execute(f"SELECT COUNT(*) FROM {DISPATCH_TABLE}").fetchone()[0],
                "on_route": conn.execute(
                    f"SELECT COUNT(*) FROM {DISPATCH_TABLE} WHERE status='On Route'"
                ).fetchone()[0],
                "delayed": conn.execute(
                    f"SELECT COUNT(*) FROM {DISPATCH_TABLE} WHERE status='Delayed'"
                ).fetchone()[0],
                "delivered": conn.execute(
                    f"SELECT COUNT(*) FROM {DISPATCH_TABLE} WHERE status='Delivered'"
                ).fetchone()[0],
            }
        hero = rows[0] if rows else None
        map_marker_count = sum(
            1
            for r in rows
            if r.get("station_lat") is not None and r.get("station_lng") is not None
        )
        return templates.TemplateResponse(
            request,
            "dashboard_ui.html",
            {
                "request": request,
                "dispatches": rows,
                "map_marker_count": map_marker_count,
                "q": q or "",
                "status": status or "All",
                "destination": destination or "",
                "vehicle_number": vehicle_number or "",
                "dispatch_date": dispatch_date or "",
                "dispatch_date_from": dispatch_date_from or "",
                "dispatch_date_to": dispatch_date_to or "",
                "distance_range": distance_range or "All",
                "weight_min": weight_min or "",
                "weight_max": weight_max or "",
                "stats": stats,
                "hero": hero,
                "depot_lat": DEPOT_LAT,
                "depot_lng": DEPOT_LNG,
                "depot_name": DEPOT_NAME,
                "sort": sort_key,
            },
        )
    except Exception:
        # Keep admin usable even if dashboard hydration hits transient DB issues.
        return RedirectResponse(url="/admin/new", status_code=303)


@app.get("/export/dispatches.csv")
def export_dispatches_csv(
    request: Request,
    q: Optional[str] = None,
    status: Optional[str] = None,
    destination: Optional[str] = None,
    vehicle_number: Optional[str] = None,
    dispatch_date: Optional[str] = None,
    dispatch_date_from: Optional[str] = None,
    dispatch_date_to: Optional[str] = None,
    distance_range: Optional[str] = None,
    weight_min: Optional[str] = None,
    weight_max: Optional[str] = None,
    sort: Optional[str] = None,
):
    """Same filters as the dashboard; downloads CSV for the current result set."""
    sort_key = (sort or "dispatch_desc").strip()
    with closing(get_conn()) as conn:
        rows = query_dispatches_hydrated(
            conn,
            q=q,
            status=status,
            destination=destination,
            vehicle_number=vehicle_number,
            dispatch_date=dispatch_date,
            dispatch_date_from=dispatch_date_from,
            dispatch_date_to=dispatch_date_to,
            distance_range=distance_range,
            weight_min=weight_min,
            weight_max=weight_max,
            sort=sort_key,
        )

    base = get_public_base_url(request)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "public_token",
            "vehicle_number",
            "status",
            "destination",
            "destination_pincode",
            "dispatch_date",
            "eta_date",
            "driver_name",
            "driver_mobile",
            "package_count",
            "total_weight_kg",
            "load_percent",
            "icn_number",
            "nature_of_items",
            "nature_display",
            "distance_km",
            "eta_urgency",
            "public_url",
            "updated_at",
        ]
    )
    for d in rows:
        eu = d.get("eta_urgency") or {}
        urgency = eu.get("label") or eu.get("level", "")
        w.writerow(
            [
                d.get("public_token", ""),
                d.get("vehicle_number", ""),
                d.get("status", ""),
                d.get("destination", ""),
                d.get("destination_pincode", ""),
                d.get("dispatch_date", ""),
                d.get("eta_date", ""),
                d.get("driver_name", ""),
                d.get("driver_mobile", ""),
                d.get("package_count", ""),
                d.get("total_weight_kg", ""),
                d.get("load_percent", ""),
                d.get("icn_number", ""),
                d.get("nature_of_items", ""),
                d.get("nature_of_items_display", ""),
                d.get("distance_km", ""),
                urgency,
                base + "/dispatch/" + str(d.get("public_token", "")),
                d.get("updated_at", ""),
            ]
        )
    data = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([data]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="cht-dispatches-filtered.csv"',
        },
    )


@app.get("/dispatch/{token}", response_class=HTMLResponse)
def dispatch_detail(request: Request, token: str):
    sync_delayed_statuses()
    with closing(get_conn()) as conn:
        row = conn.execute(
            f"SELECT * FROM {DISPATCH_TABLE} WHERE public_token = ?",
            (token,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    item = hydrate_row(row, public=True, fetch_road_route=True)
    base = get_public_base_url(request)
    qr_url = base + f"/qr/{token}.png"
    share_url = base + f"/dispatch/{token}"
    return templates.TemplateResponse(
        request,
        "detail.html",
        {
            "request": request,
            "item": item,
            "qr_url": qr_url,
            "share_url": share_url,
            "depot_lat": DEPOT_LAT,
            "depot_lng": DEPOT_LNG,
            "depot_name": DEPOT_NAME,
            "depot_pincode": DEPOT_PINCODE,
        },
    )


@app.get("/admin/new", response_class=HTMLResponse)
def new_consignment_form(request: Request):
    return templates.TemplateResponse(request, "new.html", {"request": request})


def _parse_nature_form(n1: Optional[str], n2: Optional[str], n3: Optional[str], n4: Optional[str]) -> str:
    parts = [v for v in [n1, n2, n3, n4] if v and str(v).strip() in ("1", "2", "3", "4")]
    return ",".join(sorted(set(parts), key=lambda x: int(x)))


def _prefetch_destination_route_async(destination_pincode: str) -> None:
    """
    Fire-and-forget: resolve the PIN (triggering Nominatim if needed) and pre-fetch
    the OSRM road route from the depot so the public/detail page renders instantly
    and survives server restarts (persistent cache).

    Runs in a daemon thread — it never blocks the HTTP response or raises.
    """
    import threading

    pin = normalize_pincode(destination_pincode or "")
    if not pin:
        return

    def _worker() -> None:
        try:
            resolved = lookup_pincode(pin)
            if not resolved:
                return
            lat = float(resolved.get("lat") or 0.0)
            lng = float(resolved.get("lng") or 0.0)
            if lat == 0.0 and lng == 0.0:
                return
            # Warms both the persistent OSRM cache and the process-local LRU.
            osrm_driving_route(DEPOT_LAT, DEPOT_LNG, lat, lng)
        except Exception:
            # Prefetch is strictly best-effort — failures never surface to users.
            return

    threading.Thread(target=_worker, daemon=True, name=f"route-prefetch-{pin}").start()


@app.post("/admin/new")
def create_consignment(
    vehicle_number: str = Form(...),
    dispatch_date: str = Form(...),
    destination: str = Form(""),
    destination_pincode: str = Form(""),
    icn_number: str = Form(...),
    driver_name: str = Form(...),
    driver_mobile: str = Form(...),
    package_count: int = Form(...),
    package_weight_kg: float = Form(0.0),
    total_weight_kg: float = Form(0.0),
    cht_capacity_weight_kg: float = Form(CAPACITY_WEIGHT_KG),
    eta_date: str = Form(...),
    status: str = Form("On Route"),
    internal_notes: str = Form(""),
    nature_1: Optional[str] = Form(None),
    nature_2: Optional[str] = Form(None),
    nature_3: Optional[str] = Form(None),
    nature_4: Optional[str] = Form(None),
    cht_cost_amount: Optional[str] = Form(None),
):
    status = status if status in VALID_STATUSES else "On Route"
    _validate_dispatch_timeline_or_400(dispatch_date, eta_date)
    cht_cost_val = _parse_optional_inr_amount(cht_cost_amount)
    token = secrets.token_urlsafe(12)
    now = datetime.utcnow().isoformat(timespec="seconds")

    # Total weight is always taken from the form (weighbridge / manifest). Packages vary in size;
    # we do not derive total from "weight per package" × count.

    # If PIN code is provided, normalize it; fill city name only when city field is empty (user can override).
    pin = lookup_pincode(destination_pincode or "")
    if pin:
        destination_pincode = pin["pincode"]
        if not (destination or "").strip():
            destination = pin["place_name"]

    nature_of_items = _parse_nature_form(nature_1, nature_2, nature_3, nature_4)

    with closing(get_conn()) as conn:
        conn.execute(
            """
            INSERT INTO dispatches
            (public_token, vehicle_number, dispatch_date, destination, destination_pincode, icn_number, driver_name, driver_mobile,
             package_count, package_weight_kg, total_weight_kg, cht_capacity_weight_kg, eta_date, status, internal_notes, nature_of_items, cht_cost_amount, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                token,
                vehicle_number,
                dispatch_date,
                destination,
                normalize_pincode(destination_pincode),
                icn_number,
                driver_name,
                driver_mobile,
                package_count,
                package_weight_kg,
                total_weight_kg,
                cht_capacity_weight_kg,
                eta_date,
                status,
                internal_notes,
                nature_of_items,
                cht_cost_val,
                now,
                now,
            ),
        )
        conn.commit()
    # Kick off eager internet-backed resolution + route fetch so the detail/QR
    # page serves a road route immediately (survives restarts via DB cache).
    _prefetch_destination_route_async(destination_pincode)
    return RedirectResponse(url=f"/admin/share/{token}?toast=created", status_code=303)


@app.get("/admin/edit/{token}", response_class=HTMLResponse)
def edit_dispatch_form(request: Request, token: str):
    with closing(get_conn()) as conn:
        row = conn.execute(
            f"SELECT * FROM {DISPATCH_TABLE} WHERE public_token = ?",
            (token,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    item = hydrate_row(row, public=False, include_funds=True)
    return templates.TemplateResponse(
        request,
        "edit.html",
        {"request": request, "item": item, "token": token, "statuses": list(VALID_STATUSES)},
    )


@app.post("/admin/edit/{token}")
def update_dispatch(
    token: str,
    vehicle_number: str = Form(...),
    dispatch_date: str = Form(...),
    destination: str = Form(""),
    destination_pincode: str = Form(""),
    icn_number: str = Form(...),
    driver_name: str = Form(...),
    driver_mobile: str = Form(...),
    package_count: int = Form(...),
    package_weight_kg: float = Form(0.0),
    total_weight_kg: float = Form(0.0),
    cht_capacity_weight_kg: float = Form(CAPACITY_WEIGHT_KG),
    eta_date: str = Form(...),
    status: str = Form("On Route"),
    internal_notes: str = Form(""),
    nature_1: Optional[str] = Form(None),
    nature_2: Optional[str] = Form(None),
    nature_3: Optional[str] = Form(None),
    nature_4: Optional[str] = Form(None),
    cht_cost_amount: Optional[str] = Form(None),
):
    status = status if status in VALID_STATUSES else "On Route"
    _validate_dispatch_timeline_or_400(dispatch_date, eta_date)
    now = datetime.utcnow().isoformat(timespec="seconds")
    cht_cost_val = _parse_optional_inr_amount(cht_cost_amount)

    # Total weight from form only (not derived from per-package × count).

    pin = lookup_pincode(destination_pincode or "")
    if pin:
        destination_pincode = pin["pincode"]
        if not (destination or "").strip():
            destination = pin["place_name"]

    nature_of_items = _parse_nature_form(nature_1, nature_2, nature_3, nature_4)

    with closing(get_conn()) as conn:
        exists = conn.execute(
            f"SELECT id FROM {DISPATCH_TABLE} WHERE public_token = ?",
            (token,),
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Dispatch not found")
        conn.execute(
            f"""
            UPDATE {DISPATCH_TABLE}
            SET vehicle_number = ?, dispatch_date = ?, destination = ?, destination_pincode = ?, icn_number = ?, driver_name = ?, driver_mobile = ?,
                package_count = ?, package_weight_kg = ?, total_weight_kg = ?, cht_capacity_weight_kg = ?, eta_date = ?, status = ?, internal_notes = ?, nature_of_items = ?, cht_cost_amount = ?, updated_at = ?
            WHERE public_token = ?
            """,
            (
                vehicle_number,
                dispatch_date,
                destination,
                normalize_pincode(destination_pincode),
                icn_number,
                driver_name,
                driver_mobile,
                package_count,
                package_weight_kg,
                total_weight_kg,
                cht_capacity_weight_kg,
                eta_date,
                status,
                internal_notes,
                nature_of_items,
                cht_cost_val,
                now,
                token,
            ),
        )
        conn.commit()
    # Refresh pincode/route cache whenever a dispatch is edited (destination may
    # have changed). Runs in a daemon thread — never blocks the redirect.
    _prefetch_destination_route_async(destination_pincode)
    return RedirectResponse(url=f"/admin/edit/{token}?toast=updated", status_code=303)


def _funds_year_window(y: int) -> tuple[str, str]:
    return (f"{y}-01-01", f"{y + 1}-01-01")


def _clamp_report_year(y: Optional[int]) -> int:
    cur = datetime.now().year
    if y is None:
        return cur
    if y < 2000 or y > 2100:
        return cur
    return y


@app.get("/admin/funds")
def funds_entry(request: Request):
    if not request.session.get("admin_authenticated"):
        return RedirectResponse(url=_admin_login_url("/admin/funds"), status_code=307)
    if request.session.get("funds_portal_ok"):
        return RedirectResponse(url="/admin/funds/dashboard", status_code=307)
    return RedirectResponse(url="/admin/funds/pin", status_code=307)


@app.get("/admin/funds/pin", response_class=HTMLResponse)
def funds_pin_get(request: Request):
    if not request.session.get("admin_authenticated"):
        return RedirectResponse(url=_admin_login_url(str(request.url)), status_code=307)
    next_url = request.query_params.get("next") or "/admin/funds/dashboard"
    if not str(next_url).startswith("/admin/funds"):
        next_url = "/admin/funds/dashboard"
    err = request.query_params.get("err")
    err_msg = "Invalid PIN. Try again." if err == "1" else ""
    return templates.TemplateResponse(
        request,
        "funds_pin.html",
        {"request": request, "next": next_url, "error": err_msg},
    )


@app.post("/admin/funds/pin")
def funds_pin_post(request: Request, pin: str = Form(""), next: str = Form("/admin/funds/dashboard")):
    if not request.session.get("admin_authenticated"):
        return RedirectResponse(url=_admin_login_url("/admin/funds/pin"), status_code=307)
    next_url = (next or "/admin/funds/dashboard").strip()
    if not next_url.startswith("/admin/funds"):
        next_url = "/admin/funds/dashboard"
    if (pin or "").strip() == FUNDS_PORTAL_PIN:
        request.session["funds_portal_ok"] = True
        return RedirectResponse(url=next_url, status_code=303)
    return RedirectResponse(url=f"/admin/funds/pin?next={quote(next_url)}&err=1", status_code=303)


@app.post("/admin/funds/lock")
def funds_lock(request: Request):
    request.session.pop("funds_portal_ok", None)
    return RedirectResponse(url="/admin/funds/pin", status_code=303)


@app.get("/admin/funds/dashboard", response_class=HTMLResponse)
def funds_dashboard(
    request: Request,
    year: Optional[int] = None,
    q: str = "",
    status: str = "All",
    df: str = "",
    dt: str = "",
):
    if not request.session.get("admin_authenticated"):
        return RedirectResponse(url=_admin_login_url(str(request.url)), status_code=307)
    if not request.session.get("funds_portal_ok"):
        return RedirectResponse(
            url="/admin/funds/pin?next=" + quote("/admin/funds/dashboard"),
            status_code=303,
        )
    y = _clamp_report_year(year)
    y0, y1 = _funds_year_window(y)
    toast = (request.query_params.get("toast") or "").strip()

    with closing(get_conn()) as conn:
        row_b = conn.execute(
            f"SELECT allocated_amount, updated_at FROM {FUNDS_ANNUAL_TABLE} WHERE year = ?",
            (y,),
        ).fetchone()
        allocated = float(row_b["allocated_amount"]) if row_b else 0.0
        budget_updated = (row_b["updated_at"] if row_b else "") or ""

        spent_row = conn.execute(
            f"""
            SELECT COALESCE(SUM(cht_cost_amount), 0) AS s,
                   COUNT(CASE WHEN cht_cost_amount IS NOT NULL THEN 1 END) AS n
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ?
            """,
            (y0, y1),
        ).fetchone()
        spent = float(spent_row["s"] or 0)
        n_costed = int(spent_row["n"] or 0)

        n_total_year = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM {DISPATCH_TABLE}
                WHERE dispatch_date >= ? AND dispatch_date < ?
                """,
                (y0, y1),
            ).fetchone()[0]
        )

        status_count_raw = conn.execute(
            f"""
            SELECT status, COUNT(*) AS c
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ?
            GROUP BY status
            """,
            (y0, y1),
        ).fetchall()

        status_spend_raw = conn.execute(
            f"""
            SELECT status, COALESCE(SUM(cht_cost_amount), 0) AS s
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ? AND cht_cost_amount IS NOT NULL
            GROUP BY status
            """,
            (y0, y1),
        ).fetchall()

        month_raw = conn.execute(
            f"""
            SELECT CAST(strftime('%m', dispatch_date) AS INTEGER) AS m,
                   COALESCE(SUM(cht_cost_amount), 0) AS s
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ? AND cht_cost_amount IS NOT NULL
            GROUP BY m
            ORDER BY m
            """,
            (y0, y1),
        ).fetchall()

        top_dest_raw = conn.execute(
            f"""
            SELECT COALESCE(NULLIF(TRIM(destination), ''), '(No destination)') AS d,
                   COALESCE(SUM(cht_cost_amount), 0) AS s,
                   COUNT(*) AS n
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ? AND cht_cost_amount IS NOT NULL
            GROUP BY d
            ORDER BY s DESC
            LIMIT 8
            """,
            (y0, y1),
        ).fetchall()

        py0, py1 = _funds_year_window(y - 1)
        prow = conn.execute(
            f"""
            SELECT COALESCE(SUM(cht_cost_amount), 0) AS s
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ?
            """,
            (py0, py1),
        ).fetchone()
        prior_year_spent = float(prow["s"] if prow else 0.0)

        vol_raw = conn.execute(
            f"""
            SELECT CAST(strftime('%m', dispatch_date) AS INTEGER) AS m, COUNT(*) AS c
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ?
            GROUP BY m
            ORDER BY m
            """,
            (y0, y1),
        ).fetchall()

        sql = f"""
            SELECT public_token, icn_number, vehicle_number, destination, destination_pincode,
                   dispatch_date, status, cht_cost_amount
            FROM {DISPATCH_TABLE}
            WHERE dispatch_date >= ? AND dispatch_date < ?
        """
        params: list[object] = [y0, y1]
        if q.strip():
            like = f"%{q.strip()}%"
            sql += (
                " AND (LOWER(COALESCE(icn_number,'')) LIKE LOWER(?) OR "
                "LOWER(vehicle_number) LIKE LOWER(?) OR "
                "LOWER(COALESCE(destination,'')) LIKE LOWER(?) OR "
                "LOWER(COALESCE(destination_pincode,'')) LIKE LOWER(?))"
            )
            params.extend([like, like, like, like])
        if status and status != "All":
            sql += " AND status = ?"
            params.append(status)
        if df.strip():
            sql += " AND dispatch_date >= ?"
            params.append(df.strip())
        if dt.strip():
            sql += " AND dispatch_date <= ?"
            params.append(dt.strip())
        sql += " ORDER BY datetime(dispatch_date) DESC, id DESC"
        raw_rows = conn.execute(sql, params).fetchall()

    balance = allocated - spent
    rows_out: list[dict[str, Any]] = []
    for r in raw_rows:
        rd = dict(r)
        c = rd.get("cht_cost_amount")
        rd["cht_cost_display"] = f"₹{float(c):,.2f}" if c is not None else "—"
        rd["dispatch_display"] = fmt_display(rd.get("dispatch_date") or "")
        rows_out.append(rd)

    avg_txt = f"₹{(spent / n_costed):,.2f}" if n_costed else "—"

    coverage_pct = (n_costed / n_total_year * 100.0) if n_total_year else 0.0

    if allocated > 0:
        used_vs_budget_pct = min(100.0, (spent / allocated) * 100.0)
        over_budget_pct = max(0.0, ((spent - allocated) / allocated) * 100.0) if spent > allocated else 0.0
        is_over_budget = spent > allocated
    else:
        used_vs_budget_pct = 0.0
        over_budget_pct = 0.0
        is_over_budget = False

    status_colors = {
        "On Route": "#10b981",
        "Delayed": "#f59e0b",
        "Delivered": "#6366f1",
    }
    status_order = ["On Route", "Delayed", "Delivered"]
    st_counts = {str(r["status"]): int(r["c"]) for r in status_count_raw}
    total_st = sum(st_counts.values()) or 0
    status_mix: list[dict[str, Any]] = []
    for st in status_order:
        c = st_counts.get(st, 0)
        pct = (c / total_st * 100.0) if total_st else 0.0
        status_mix.append(
            {
                "status": st,
                "count": c,
                "pct": round(pct, 1),
                "color": status_colors.get(st, "#94a3b8"),
            }
        )

    st_spend = {str(r["status"]): float(r["s"] or 0) for r in status_spend_raw}
    spend_total_st = sum(st_spend.values()) or 0.0
    status_spend_segments: list[dict[str, Any]] = []
    donut_parts: list[str] = []
    cum = 0.0
    for st in status_order:
        amt = st_spend.get(st, 0.0)
        pct = (amt / spend_total_st * 100.0) if spend_total_st > 0 else 0.0
        col = status_colors.get(st, "#94a3b8")
        status_spend_segments.append(
            {
                "status": st,
                "amount": amt,
                "amount_display": f"₹{amt:,.2f}",
                "pct": round(pct, 1),
                "bar_pct": round(pct, 2),
                "color": col,
            }
        )
        if spend_total_st > 0 and pct > 0:
            nxt = min(100.0, cum + pct)
            donut_parts.append(f"{col} {cum:.4f}% {nxt:.4f}%")
            cum = nxt
    donut_spend_style = f"conic-gradient({', '.join(donut_parts)})" if donut_parts else "conic-gradient(#334155 0% 100%)"

    by_month: dict[int, float] = {}
    for r in month_raw:
        m = r["m"]
        if m is not None:
            by_month[int(m)] = float(r["s"] or 0)
    month_labels = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]
    monthly_bars: list[dict[str, Any]] = []
    for mi in range(1, 13):
        monthly_bars.append(
            {
                "m": mi,
                "label": month_labels[mi - 1],
                "amount": by_month.get(mi, 0.0),
                "amount_short": f"₹{by_month.get(mi, 0.0):,.0f}",
            }
        )
    max_month = max((b["amount"] for b in monthly_bars), default=0.0) or 1.0
    for b in monthly_bars:
        b["h_pct"] = min(100.0, (b["amount"] / max_month) * 100.0)

    by_vol: dict[int, int] = {}
    for r in vol_raw:
        m = r["m"]
        if m is not None:
            by_vol[int(m)] = int(r["c"] or 0)
    for b in monthly_bars:
        b["dispatch_count"] = by_vol.get(int(b["m"]), 0)

    flat_month_avg = spent / 12.0 if spent > 0 else 0.0
    for b in monthly_bars:
        amt = float(b["amount"])
        if flat_month_avg <= 0:
            b["pace_cls"] = "funds-month--none"
            b["pace_label"] = ""
        elif amt <= 0:
            b["pace_cls"] = "funds-month--none"
            b["pace_label"] = "No spend"
        elif amt >= flat_month_avg * 1.35:
            b["pace_cls"] = "funds-month--peak"
            b["pace_label"] = "Busy"
        elif amt <= flat_month_avg * 0.5:
            b["pace_cls"] = "funds-month--quiet"
            b["pace_label"] = "Easy"
        else:
            b["pace_cls"] = "funds-month--mid"
            b["pace_label"] = "Typical"

    pos_month_amts = [float(b["amount"]) for b in monthly_bars if float(b["amount"]) > 0]
    spend_cv_pct: Optional[float] = None
    if len(pos_month_amts) >= 2:
        m_sp = sum(pos_month_amts) / len(pos_month_amts)
        if m_sp > 0:
            var_sp = sum((x - m_sp) ** 2 for x in pos_month_amts) / len(pos_month_amts)
            spend_cv_pct = round((var_sp**0.5) / m_sp * 100.0, 1)

    yoy_change_pct: Optional[float] = None
    if prior_year_spent > 0:
        yoy_change_pct = round((spent - prior_year_spent) / prior_year_spent * 100.0, 1)

    quarters_out: list[dict[str, Any]] = []
    for qn, (ma, mb) in enumerate([(1, 3), (4, 6), (7, 9), (10, 12)], start=1):
        qsum = sum(by_month.get(mi, 0.0) for mi in range(ma, mb + 1))
        quarters_out.append({"q": qn, "label": f"Q{qn}", "amount": qsum, "amount_display": f"₹{qsum:,.0f}"})
    max_q = max((q["amount"] for q in quarters_out), default=0.0) or 1.0
    for q in quarters_out:
        q["w_pct"] = min(100.0, (q["amount"] / max_q) * 100.0)

    forecast_projected: Optional[float] = None
    forecast_vs_budget_pct: Optional[float] = None
    forecast_note = ""
    is_forecast_active = y == date.today().year and spent > 0
    if is_forecast_active:
        m_now = date.today().month
        ytd_spend = sum(by_month.get(mi, 0.0) for mi in range(1, m_now + 1))
        if m_now >= 1 and ytd_spend > 0:
            run_rate = ytd_spend / float(m_now)
            forecast_projected = run_rate * 12.0
            if allocated > 0:
                forecast_vs_budget_pct = round((forecast_projected / allocated) * 100.0, 1)
            if allocated > 0 and forecast_projected > allocated * 1.02:
                forecast_note = (
                    f"At the current {m_now}-month run rate, projected year spend is about "
                    f"₹{forecast_projected:,.0f} ({forecast_vs_budget_pct}% of allocation). Consider pacing or budget."
                )
            elif allocated > 0 and forecast_projected < allocated * 0.75:
                forecast_note = (
                    f"Run rate suggests a soft year (~₹{forecast_projected:,.0f}); headroom for extra CHT or contingencies."
                )
            else:
                forecast_note = (
                    f"Linear projection from YTD: ~₹{forecast_projected:,.0f} for the full year "
                    f"({forecast_vs_budget_pct or 0:.0f}% of allocation if set)."
                )

    top_destinations: list[dict[str, Any]] = []
    max_dest = max((float(r["s"] or 0) for r in top_dest_raw), default=0.0) or 1.0
    for r in top_dest_raw:
        amt = float(r["s"] or 0)
        top_destinations.append(
            {
                "name": str(r["d"])[:42],
                "amount": amt,
                "amount_display": f"₹{amt:,.2f}",
                "n": int(r["n"] or 0),
                "w_pct": min(100.0, (amt / max_dest) * 100.0),
            }
        )

    return templates.TemplateResponse(
        request,
        "funds_dashboard.html",
        {
            "request": request,
            "year": y,
            "allocated": allocated,
            "spent": spent,
            "balance": balance,
            "n_costed": n_costed,
            "n_rows": len(rows_out),
            "n_total_year": n_total_year,
            "budget_updated": budget_updated,
            "rows": rows_out,
            "allocated_display": f"₹{allocated:,.2f}",
            "allocated_input": f"{allocated:.2f}",
            "spent_display": f"₹{spent:,.2f}",
            "balance_display": f"₹{balance:,.2f}",
            "avg_cost_display": avg_txt,
            "coverage_pct": round(coverage_pct, 1),
            "used_vs_budget_pct": round(used_vs_budget_pct, 1),
            "over_budget_pct": round(over_budget_pct, 1),
            "is_over_budget": is_over_budget,
            "has_budget": allocated > 0,
            "status_mix": status_mix,
            "status_spend_segments": status_spend_segments,
            "donut_spend_style": donut_spend_style,
            "has_spend_breakdown": spend_total_st > 0,
            "monthly_bars": monthly_bars,
            "top_destinations": top_destinations,
            "prior_year_spent": prior_year_spent,
            "prior_year_spent_display": f"₹{prior_year_spent:,.2f}",
            "yoy_change_pct": yoy_change_pct,
            "yoy_change_display": (f"{yoy_change_pct:+.1f}%" if yoy_change_pct is not None else "—"),
            "spend_cv_pct": spend_cv_pct,
            "quarters_out": quarters_out,
            "forecast_projected": forecast_projected,
            "forecast_projected_display": (
                f"₹{forecast_projected:,.0f}" if forecast_projected is not None else "—"
            ),
            "forecast_vs_budget_pct": forecast_vs_budget_pct,
            "forecast_note": forecast_note,
            "is_forecast_active": is_forecast_active,
            "today_year": date.today().year,
            "q": q,
            "status": status,
            "df": df,
            "dt": dt,
            "toast": toast,
        },
    )


@app.post("/admin/funds/budget")
def funds_budget_post(request: Request, year: int = Form(...), allocated_amount: str = Form(...)):
    if not request.session.get("admin_authenticated"):
        return RedirectResponse(url=_admin_login_url("/admin/funds/budget"), status_code=307)
    if not request.session.get("funds_portal_ok"):
        return RedirectResponse(url="/admin/funds/pin", status_code=303)
    y = _clamp_report_year(year)
    try:
        amt = float(str(allocated_amount).strip().replace(",", "").replace("₹", ""))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid budget amount.")
    if amt < 0:
        raise HTTPException(status_code=400, detail="Budget cannot be negative.")
    now = datetime.utcnow().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        conn.execute(
            f"""
            INSERT INTO {FUNDS_ANNUAL_TABLE} (year, allocated_amount, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(year) DO UPDATE SET
              allocated_amount = excluded.allocated_amount,
              updated_at = excluded.updated_at
            """,
            (y, amt, now),
        )
        conn.commit()
    return RedirectResponse(url=f"/admin/funds/dashboard?year={y}&toast=budget", status_code=303)


@app.post("/admin/status/{token}")
def set_dispatch_status(token: str, status: str = Form(...)):
    status = status if status in VALID_STATUSES else "On Route"
    now = datetime.utcnow().isoformat(timespec="seconds")
    with closing(get_conn()) as conn:
        conn.execute(
            f"UPDATE {DISPATCH_TABLE} SET status = ?, updated_at = ? WHERE public_token = ?",
            (status, now, token),
        )
        conn.commit()
    return RedirectResponse(url="/?toast=updated", status_code=303)


@app.get("/admin/delete/{token}", response_class=HTMLResponse)
def delete_dispatch_form(request: Request, token: str):
    with closing(get_conn()) as conn:
        row = conn.execute(
            f"SELECT * FROM {DISPATCH_TABLE} WHERE public_token = ?",
            (token,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    item = hydrate_row(row, public=False)
    return templates.TemplateResponse(
        request,
        "delete.html",
        {"request": request, "item": item, "token": token},
    )


@app.post("/admin/delete/{token}")
def delete_dispatch(token: str, remarks: str = Form("")):
    remarks = (remarks or "").strip() or "No remarks provided"
    with closing(get_conn()) as conn:
        row = conn.execute(
            f"SELECT * FROM {DISPATCH_TABLE} WHERE public_token = ?",
            (token,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Dispatch not found")
        r = dict(row)
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn.execute(
            f"""
            INSERT INTO {DELETED_DISPATCHES_TABLE}
            (public_token, vehicle_number, dispatch_date, destination, destination_pincode, icn_number,
             driver_name, driver_mobile, package_count, total_weight_kg, deleted_at, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["public_token"],
                r["vehicle_number"],
                r["dispatch_date"],
                r["destination"],
                r.get("destination_pincode") or "",
                r.get("icn_number") or "",
                r["driver_name"],
                r["driver_mobile"],
                r["package_count"],
                float(r.get("total_weight_kg") or 0),
                now,
                remarks,
            ),
        )
        conn.execute(f"DELETE FROM {DISPATCH_TABLE} WHERE public_token = ?", (token,))
        conn.commit()
    return RedirectResponse(url="/?toast=deleted", status_code=303)


@app.get("/admin/deleted", response_class=HTMLResponse)
def deleted_dispatches_log(request: Request):
    with closing(get_conn()) as conn:
        rows = conn.execute(
            f"""
            SELECT id, public_token, vehicle_number, dispatch_date, destination, icn_number,
                   driver_name, package_count, total_weight_kg, deleted_at, remarks
            FROM {DELETED_DISPATCHES_TABLE}
            ORDER BY deleted_at DESC
            """
        ).fetchall()
    items = [dict(r) for r in rows]
    return templates.TemplateResponse(
        request,
        "deleted.html",
        {"request": request, "items": items},
    )


@app.get("/qr/{token}.png")
def qr_png(token: str, request: Request):
    target = get_public_base_url(request) + f"/dispatch/{token}"
    qr = qrcode.QRCode(border=2, box_size=8)
    qr.add_data(target)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#F8FAFC", back_color="#05060A")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")


@app.get("/admin/share/{token}", response_class=HTMLResponse)
def share_page(request: Request, token: str):
    sync_delayed_statuses()
    with closing(get_conn()) as conn:
        row = conn.execute(
            f"SELECT * FROM {DISPATCH_TABLE} WHERE public_token = ?",
            (token,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Dispatch not found")
    item = hydrate_row(row, public=True, fetch_road_route=True)
    share_url = get_public_base_url(request) + f"/dispatch/{token}"
    qr_url = get_public_base_url(request) + f"/qr/{token}.png"
    nature_part = (
        f" | Nature of Stores {item.get('nature_of_items_display') or '—'}"
        if item.get("nature_of_items_display")
        else ""
    )
    whatsapp_text = (
        f"Dispatch | Vehicle {item['vehicle_number']} | ICN {item.get('icn_number') or '—'} | "
        f"To {item.get('to_display') or item.get('destination') or '—'} | "
        f"PIN {item.get('destination_pincode') or '—'}{nature_part} | "
        f"Driver {item['driver_name']} ({item['driver_mobile']}) | "
        f"Packages {item['package_count']} | Weight {item.get('weight_display') or '—'} | "
        f"ETA {item['eta_display']} | Dist {item.get('distance_display') or '—'} | {share_url}"
    )
    return templates.TemplateResponse(
        request,
        "share.html",
        {
            "request": request,
            "item": item,
            "share_url": share_url,
            "qr_url": qr_url,
            "whatsapp_text": whatsapp_text,
        },
    )


@app.get("/api/place-pin", response_class=JSONResponse)
def api_place_pin(q: str = ""):
    pin = lookup_pincode_for_place(q)
    if not pin:
        return {"found": False}
    return {
        "found": True,
        "pincode": pin["pincode"],
        "place_name": pin["place_name"],
        "state_name": pin.get("state_name") or "",
    }


@app.get("/api/place-pin-suggest", response_class=JSONResponse)
def api_place_pin_suggest(q: str = "", limit: int = 8):
    items = lookup_pincode_suggestions(q, limit=limit)
    return {
        "found": bool(items),
        "items": [
            {
                "pincode": it["pincode"],
                "place_name": it["place_name"],
                "state_name": it.get("state_name") or "",
            }
            for it in items
        ],
    }


@app.get("/pincode/resolve/{pincode}", response_class=JSONResponse)
def pincode_resolve(pincode: str):
    pin = lookup_pincode(pincode)
    if not pin:
        return JSONResponse(
            {
                "found": False,
                "pincode": normalize_pincode(pincode),
            }
        )
    return {
        "found": True,
        "pincode": pin["pincode"],
        "place_name": pin["place_name"],
        "state_name": pin["state_name"],
        "lat": pin["lat"],
        "lng": pin["lng"],
    }
