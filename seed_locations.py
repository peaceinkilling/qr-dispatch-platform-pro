import sqlite3
import urllib.request
from contextlib import closing
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "dispatch.db"
LOCATIONS_TABLE = "locations"

LOCATION_STATES = {
    "RJ": "Rajasthan",
    "MP": "Madhya Pradesh",
    "MH": "Maharashtra",
    "GJ": "Gujarat",
}


def _is_number_token(tok: str) -> bool:
    tok = (tok or "").strip()
    if not tok or tok.lower() == "nan":
        return False
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", tok))


def _norm_text(value: str) -> str:
    return " ".join((value or "").strip().lower().replace(",", " ").split())


def ensure_locations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
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


def seed_from_districtwise_csv(url: str) -> int:
    # Dataset we fetch is tab-separated and contains district lat/long.
    # We only need: State_Code, State (full name), District, Lat, Long.
    allowed_codes = set(LOCATION_STATES.keys())
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_locations_table(conn)

        # Replace existing rows for those states so reruns are deterministic.
        conn.execute(
            f"DELETE FROM {LOCATIONS_TABLE} WHERE state_code IN ({','.join(['?'] * len(allowed_codes))})",
            tuple(allowed_codes),
        )

        df = pd.read_csv(url, sep="\t")
        df = df[df["State_Code"].isin(list(allowed_codes))]
        df = df[df["Lat"].notna() & df["Long"].notna()]

        rows = []
        for _, r in df.iterrows():
            state_code = str(r["State_Code"]).strip()
            district = str(r["District"]).strip()
            if not district or district.lower() == "nan":
                continue

            rows.append(
                (
                    state_code,
                    str(LOCATION_STATES.get(state_code, r.get("State", ""))).strip(),
                    district,
                    float(r["Lat"]),
                    float(r["Long"]),
                )
            )

        conn.executemany(
            f"""
            INSERT OR IGNORE INTO {LOCATIONS_TABLE}
            (state_code, state_name, district_name, lat, lng)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()

        count = conn.execute(
            f"SELECT COUNT(*) FROM {LOCATIONS_TABLE} WHERE state_code IN ({','.join(['?']*len(allowed_codes))})",
            tuple(allowed_codes),
        ).fetchone()[0]
        return int(count)


if __name__ == "__main__":
    # Source: meghamodi "Districtwise_India.csv" (includes Lat/Long for districts)
    URL = "https://gist.githubusercontent.com/meghamodi/6a49554f26b3d5b7097a9f08841e4241/raw/"
    total = seed_from_districtwise_csv(URL)
    print(f"Seeded locations rows (RJ/MP/MH/GJ): {total}")

