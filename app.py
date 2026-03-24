
from __future__ import annotations

import io
import random
import secrets
import sqlite3
from contextlib import closing
import csv
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Optional

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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
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
DISPATCH_TABLE = "dispatches"
LOCATIONS_TABLE = "locations"
PIN_CODES_TABLE = "pincodes"
DELETED_DISPATCHES_TABLE = "deleted_dispatches"

app = FastAPI(title="Dispatch QR Platform", version="3.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ---- Admin authentication (simple session cookie) ----
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "cht_mgt")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Security@123")

# Must be stable across workers/processes. Random per-worker secrets break sessions on multi-worker hosts.
SESSION_SECRET = os.environ.get("SESSION_SECRET", "change-me-in-production")
SESSION_COOKIE_SECURE = PUBLIC_BASE_URL.startswith("https://") if PUBLIC_BASE_URL else False

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="admin_session",
    https_only=SESSION_COOKIE_SECURE,
    same_site="lax",
)


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
            or path in ("/admin/login", "/admin/logout", "/healthz")
        )

        if is_public:
            return await call_next(request)

        is_admin_protected = path == "/" or path.startswith("/admin") or path.startswith("/export")
        if not is_admin_protected:
            return await call_next(request)

        if request.session.get("admin_authenticated") is True:
            # Logged-in admin allowed pages:
            # - dashboard
            # - create new CHT
            # - share from dashboard
            # - mark delivered
            allowed_admin = (
                path == "/"
                or path.startswith("/admin/new")
                or path.startswith("/admin/share")
                or path.startswith("/admin/status/")
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
        login_url = "/admin/login"
        if next_path:
            login_url = f"{login_url}?next={quote(next_path, safe='')}"
        return RedirectResponse(url=login_url, status_code=307)


app.add_middleware(AdminGateMiddleware)


def get_public_base_url(request: Request) -> str:
    """
    Base URL used to generate QR links and share URLs.
    Prefer PUBLIC_BASE_URL so QR codes remain correct behind reverse proxies.
    """
    return PUBLIC_BASE_URL or str(request.base_url).rstrip("/")


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
DEPOT_LAT = 26.32535  # fallback if pincode lookup fails
DEPOT_LNG = 73.2401
DEPOT_NAME = "Banar"


def _resolve_depot() -> None:
    """Resolve depot coordinates from pincode at startup."""
    global DEPOT_LAT, DEPOT_LNG, DEPOT_NAME
    pin = lookup_pincode(DEPOT_PINCODE)
    if pin:
        DEPOT_LAT = pin["lat"]
        DEPOT_LNG = pin["lng"]
        DEPOT_NAME = pin["place_name"]

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
    place_name = display.split(",")[0].strip() if display else pin

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


def hydrate_row(row: sqlite3.Row, *, public: bool) -> dict:
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

    if item.get("station_lat") is not None and item.get("station_lng") is not None:
        item["station_coords_display"] = f"{item['station_lat']:.4f}, {item['station_lng']:.4f}"
        try:
            dist = haversine_km(
                DEPOT_LAT,
                DEPOT_LNG,
                float(item["station_lat"]),
                float(item["station_lng"]),
            )
            item["distance_km"] = dist
            item["distance_display"] = f"~{dist:.0f} km"
        except Exception:
            item["distance_display"] = ""
    else:
        item["station_coords_display"] = ""
        item["distance_display"] = ""
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
        # Ensure depot (Banar, PIN 342027) is always resolvable.
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {PIN_CODES_TABLE} (pincode, place_name, state_name, lat, lng)
            VALUES ('342027', 'Banar', 'Rajasthan', 26.32535, 73.2401)
            """
        )

        # Ensure key destination pincodes used by demo/setup are correct.
        # This prevents wrong station GPS pins (which look like "Pakistan" on the schematic/fallback map).
        # Also fills missing pincodes so markings appear for every CHT with a valid destination_pincode.
        pincode_fixes = [
            ("302001", "Jaipur", "Rajasthan", 26.9124, 75.7873),
            ("313001", "Udaipur", "Rajasthan", 24.5854, 73.7125),
            ("334001", "Bikaner", "Rajasthan", 28.0222, 73.3119),
            ("324001", "Kota", "Rajasthan", 25.2138, 75.8564),
            ("345001", "Jaisalmer", "Rajasthan", 26.9156, 70.9076),
            ("380001", "Ahmedabad", "Gujarat", 23.2156, 72.7961),
            ("395001", "Surat", "Gujarat", 21.1702, 72.8311),
            ("390001", "Vadodara", "Gujarat", 22.3072, 73.1812),
            ("452001", "Indore", "Madhya Pradesh", 22.7196, 75.8577),
            ("462001", "Bhopal", "Madhya Pradesh", 23.2599, 77.4126),
            ("431001", "Aurangabad", "Maharashtra", 19.8762, 75.3433),
            ("411001", "Pune", "Maharashtra", 18.5204, 73.8567),
            ("440001", "Nagpur", "Maharashtra", 21.1458, 79.0882),
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

        # Fixed public_token per CHT so QR codes are stable (no change on refresh).
        # Timeline fields are generated as recent, mixed sample traffic each day.
        demo_seed_base = [
            {"public_token": "cht-gj08au8678", "vehicle_number": "GJ 08 AU 8678", "destination": "Jaisalmer", "destination_pincode": "345001", "icn_number": "ICN-CTH-224-10001", "driver_name": "Sattar Khan", "driver_mobile": "+91 98765 43210", "package_count": 95, "total_weight_kg": 4120.0, "nature_of_items": "1"},
            {"public_token": "cht-rj19gb2451", "vehicle_number": "RJ 19 GB 2451", "destination": "Indore", "destination_pincode": "452001", "icn_number": "ICN-CTH-224-10002", "driver_name": "Ramesh Yadav", "driver_mobile": "+91 99887 76655", "package_count": 82, "total_weight_kg": 3680.0, "nature_of_items": "2"},
            {"public_token": "cht-rj07ta5402", "vehicle_number": "RJ 07 TA 5402", "destination": "Aurangabad", "destination_pincode": "431001", "icn_number": "ICN-CTH-224-10003", "driver_name": "Imran Ali", "driver_mobile": "+91 91234 56780", "package_count": 78, "total_weight_kg": 3510.0, "nature_of_items": "3"},
            {"public_token": "cht-rj14kc9021", "vehicle_number": "RJ 14 KC 9021", "destination": "Ahmedabad", "destination_pincode": "380001", "icn_number": "ICN-CTH-224-10004", "driver_name": "Nafees Khan", "driver_mobile": "+91 90012 34567", "package_count": 88, "total_weight_kg": 3960.0, "nature_of_items": "1,3"},
            {"public_token": "cht-rj01ab1234", "vehicle_number": "RJ 01 AB 1234", "destination": "Jaipur", "destination_pincode": "302001", "icn_number": "ICN-CTH-224-10005", "driver_name": "Vikram Singh", "driver_mobile": "+91 98765 11111", "package_count": 102, "total_weight_kg": 4590.0, "nature_of_items": "2,4"},
            {"public_token": "cht-rj13cd5678", "vehicle_number": "RJ 13 CD 5678", "destination": "Udaipur", "destination_pincode": "313001", "icn_number": "ICN-CTH-224-10006", "driver_name": "Mahendra Sharma", "driver_mobile": "+91 98765 22222", "package_count": 91, "total_weight_kg": 4095.0, "nature_of_items": "1,2,3"},
            {"public_token": "cht-gj03ef9012", "vehicle_number": "GJ 03 EF 9012", "destination": "Surat", "destination_pincode": "395001", "icn_number": "ICN-CTH-224-10007", "driver_name": "Rajesh Patel", "driver_mobile": "+91 98765 33333", "package_count": 85, "total_weight_kg": 3825.0, "nature_of_items": "4"},
            {"public_token": "cht-mp09gh3456", "vehicle_number": "MP 09 GH 3456", "destination": "Bhopal", "destination_pincode": "462001", "icn_number": "ICN-CTH-224-10008", "driver_name": "Anil Verma", "driver_mobile": "+91 98765 44444", "package_count": 96, "total_weight_kg": 4320.0, "nature_of_items": "1,4"},
            {"public_token": "cht-mh12ij7890", "vehicle_number": "MH 12 IJ 7890", "destination": "Pune", "destination_pincode": "411001", "icn_number": "ICN-CTH-224-10009", "driver_name": "Suresh Deshmukh", "driver_mobile": "+91 98765 55555", "package_count": 89, "total_weight_kg": 4005.0, "nature_of_items": "2"},
            {"public_token": "cht-rj02kl2468", "vehicle_number": "RJ 02 KL 2468", "destination": "Kota", "destination_pincode": "324001", "icn_number": "ICN-CTH-224-10010", "driver_name": "Deepak Meena", "driver_mobile": "+91 98765 66666", "package_count": 94, "total_weight_kg": 4230.0, "nature_of_items": "3,4"},
            {"public_token": "cht-gj05mn1357", "vehicle_number": "GJ 05 MN 1357", "destination": "Vadodara", "destination_pincode": "390001", "icn_number": "ICN-CTH-224-10011", "driver_name": "Prakash Joshi", "driver_mobile": "+91 98765 77777", "package_count": 87, "total_weight_kg": 3915.0, "nature_of_items": "1,2"},
            {"public_token": "cht-rj08op8642", "vehicle_number": "RJ 08 OP 8642", "destination": "Bikaner", "destination_pincode": "334001", "icn_number": "ICN-CTH-224-10012", "driver_name": "Mohan Lal", "driver_mobile": "+91 98765 88888", "package_count": 76, "total_weight_kg": 3420.0, "nature_of_items": "2,3"},
            {"public_token": "cht-mh15qr9753", "vehicle_number": "MH 15 QR 9753", "destination": "Nagpur", "destination_pincode": "440001", "icn_number": "ICN-CTH-224-10013", "driver_name": "Sanjay Rao", "driver_mobile": "+91 98765 99999", "package_count": 98, "total_weight_kg": 4410.0, "nature_of_items": "1,3,4"},
        ]
        rng = random.Random(datetime.utcnow().date().isoformat())
        status_pool = (
            ["Delivered"] * 4
            + ["On Route"] * 5
            + ["On Route"] * 4
        )
        rng.shuffle(status_pool)
        demo_seed = []
        for base_item, seed_status in zip(demo_seed_base, status_pool):
            dispatch_dt = datetime.utcnow().date() - timedelta(days=rng.randint(0, 6))
            min_eta_dt = dispatch_dt + timedelta(days=1)
            if seed_status == "Delivered":
                eta_target = datetime.utcnow().date() - timedelta(days=rng.randint(0, 2))
            else:
                # Keep some non-delivered rows overdue to show delayed traffic in dashboard urgency badges.
                is_delayed = rng.random() < 0.35
                if is_delayed:
                    eta_target = datetime.utcnow().date() - timedelta(days=rng.randint(1, 2))
                else:
                    eta_target = datetime.utcnow().date() + timedelta(days=rng.randint(0, 4))
            eta_dt = max(min_eta_dt, eta_target)
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
            # Add missing demo CHTs (up to 13) for existing DBs
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
                        updated_at = ?
                    WHERE vehicle_number = ?
                    """,
                    (
                        s["package_count"],
                        s["total_weight_kg"],
                        s["dispatch_date"],
                        s["eta"],
                        s["status"],
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
                    WHEN 'RJ 19 GB 2451' THEN 'Indore'
                    WHEN 'RJ 07 TA 5402' THEN 'Aurangabad'
                    WHEN 'RJ 14 KC 9021' THEN 'Ahmedabad'
                    WHEN 'RJ 01 AB 1234' THEN 'Jaipur'
                    WHEN 'RJ 13 CD 5678' THEN 'Udaipur'
                    WHEN 'GJ 03 EF 9012' THEN 'Surat'
                    WHEN 'MP 09 GH 3456' THEN 'Bhopal'
                    WHEN 'MH 12 IJ 7890' THEN 'Pune'
                    WHEN 'RJ 02 KL 2468' THEN 'Kota'
                    WHEN 'GJ 05 MN 1357' THEN 'Vadodara'
                    WHEN 'RJ 08 OP 8642' THEN 'Bikaner'
                    WHEN 'MH 15 QR 9753' THEN 'Nagpur'
                    ELSE destination
                END,
                destination_pincode = CASE vehicle_number
                    WHEN 'GJ 08 AU 8678' THEN '345001'
                    WHEN 'RJ 19 GB 2451' THEN '452001'
                    WHEN 'RJ 07 TA 5402' THEN '431001'
                    WHEN 'RJ 14 KC 9021' THEN '380001'
                    WHEN 'RJ 01 AB 1234' THEN '302001'
                    WHEN 'RJ 13 CD 5678' THEN '313001'
                    WHEN 'GJ 03 EF 9012' THEN '395001'
                    WHEN 'MP 09 GH 3456' THEN '462001'
                    WHEN 'MH 12 IJ 7890' THEN '411001'
                    WHEN 'RJ 02 KL 2468' THEN '324001'
                    WHEN 'GJ 05 MN 1357' THEN '390001'
                    WHEN 'RJ 08 OP 8642' THEN '334001'
                    WHEN 'MH 15 QR 9753' THEN '440001'
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
        conn.commit()


STATUS_SYNC_INTERVAL_SECONDS = 300
_LAST_STATUS_SYNC_AT: Optional[datetime] = None


def sync_delayed_statuses() -> None:
    """
    Keep dispatch `status` consistent with ETA.

    - If not Delivered and ETA day has ended => mark as Delayed
    - Otherwise => mark as On Route

    This also guarantees there are no lingering "Awaiting" rows.
    """
    global _LAST_STATUS_SYNC_AT
    now = datetime.now()
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


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request, next: str = "/admin/new"):
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "error": "",
            "next": next,
        },
    )


@app.post("/admin/login")
def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/admin/new"),
):
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        request.session["admin_authenticated"] = True
        request.session["admin_user"] = username
        return RedirectResponse(url=next or "/admin/new", status_code=303)

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "error": "Invalid credentials.",
            "next": next,
        },
        status_code=401,
    )


@app.get("/admin/logout", response_class=HTMLResponse)
def admin_logout(request: Request):
    try:
        request.session.clear()
    except Exception:
        pass
    return RedirectResponse(url="/admin/login", status_code=303)


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
        return templates.TemplateResponse(
            request,
            "dashboard_ui.html",
            {
                "request": request,
                "dispatches": rows,
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
    item = hydrate_row(row, public=True)
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
        },
    )


@app.get("/admin/new", response_class=HTMLResponse)
def new_consignment_form(request: Request):
    return templates.TemplateResponse(request, "new.html", {"request": request})


def _parse_nature_form(n1: Optional[str], n2: Optional[str], n3: Optional[str], n4: Optional[str]) -> str:
    parts = [v for v in [n1, n2, n3, n4] if v and str(v).strip() in ("1", "2", "3", "4")]
    return ",".join(sorted(set(parts), key=lambda x: int(x)))


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
):
    status = status if status in VALID_STATUSES else "On Route"
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
             package_count, package_weight_kg, total_weight_kg, cht_capacity_weight_kg, eta_date, status, internal_notes, nature_of_items, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                now,
                now,
            ),
        )
        conn.commit()
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
    item = hydrate_row(row, public=False)
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
):
    status = status if status in VALID_STATUSES else "On Route"
    now = datetime.utcnow().isoformat(timespec="seconds")

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
                package_count = ?, package_weight_kg = ?, total_weight_kg = ?, cht_capacity_weight_kg = ?, eta_date = ?, status = ?, internal_notes = ?, nature_of_items = ?, updated_at = ?
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
                now,
                token,
            ),
        )
        conn.commit()
    return RedirectResponse(url=f"/admin/edit/{token}?toast=updated", status_code=303)


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
    item = hydrate_row(row, public=True)
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
