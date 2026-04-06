import csv
import sqlite3
import urllib.request
from contextlib import closing
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "dispatch.db"
PIN_CODES_TABLE = "pincodes"

# Canonical states used by the app's GPS lookup.
STATE_MAP = {
    "rajasthan": "Rajasthan",
    "madhya pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "gujarat": "Gujarat",
}


def canonical_state(state_value: str) -> str:
    s = (state_value or "").strip().lower()
    return STATE_MAP.get(s, "")


def normalize_pincode(p: str) -> str:
    digits = "".join(ch for ch in (p or "") if ch.isdigit())
    return digits if len(digits) == 6 else ""


def main() -> int:
    # Dataset: sanand0/pincode => data/IN.csv
    url = "https://raw.githubusercontent.com/sanand0/pincode/master/data/IN.csv"
    raw = urllib.request.urlopen(url, timeout=60).read().decode("utf-8", errors="ignore")
    lines = raw.splitlines()
    if not lines:
        return 0

    reader = csv.DictReader(lines)

    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
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

        # Clear only our states so other datasets could be added later.
        conn.execute(
            f"""
            DELETE FROM {PIN_CODES_TABLE}
            WHERE state_name IN ('Rajasthan','Madhya Pradesh','Maharashtra','Gujarat')
            """
        )

        rows_to_insert: list[tuple[str, str, str, float, float]] = []

        for r in reader:
            key = r.get("key", "")  # e.g. IN/110001
            pincode = normalize_pincode(key.split("/")[-1])
            if not pincode:
                continue

            state_name = canonical_state(r.get("admin_name1", ""))
            if not state_name:
                continue

            place_name = (r.get("place_name") or "").strip()
            if not place_name:
                continue

            try:
                lat = float(r.get("latitude", ""))
                lng = float(r.get("longitude", ""))
            except (TypeError, ValueError):
                continue

            rows_to_insert.append((pincode, place_name, state_name, lat, lng))

        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {PIN_CODES_TABLE}
            (pincode, place_name, state_name, lat, lng)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()

        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM {PIN_CODES_TABLE}
            WHERE state_name IN ('Rajasthan','Madhya Pradesh','Maharashtra','Gujarat')
            """
        ).fetchone()[0]
        return int(count)


if __name__ == "__main__":
    inserted = main()
    print(f"Seeded pincodes rows (RJ/MP/MH/GJ): {inserted}")

