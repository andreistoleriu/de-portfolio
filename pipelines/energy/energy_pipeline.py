"""
Energy Consumption Pipeline
Simulates smart meter data and loads it into DuckDB using dlt.
"""

import dlt
import duckdb
from datetime import datetime, timedelta
import random
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH  = PROJECT_ROOT / "data" / "processed" / "energy.duckdb"

# ── Config ────────────────────────────────────────────────────────────────────
SITES = [
    {"site_id": "SITE_001", "city": "Berlin",    "type": "residential"},
    {"site_id": "SITE_002", "city": "Munich",    "type": "commercial"},
    {"site_id": "SITE_003", "city": "Hamburg",   "type": "industrial"},
    {"site_id": "SITE_004", "city": "Cologne",   "type": "residential"},
    {"site_id": "SITE_005", "city": "Frankfurt", "type": "commercial"},
]

DAYS_OF_DATA = 30


# ── Resource ──────────────────────────────────────────────────────────────────
@dlt.resource(name="meter_readings", write_disposition="replace")
def meter_readings():
    """Generate hourly smart meter readings for all sites."""
    start_date = datetime.now() - timedelta(days=DAYS_OF_DATA)

    for site in SITES:
        base_kwh = {
            "residential": 0.4,
            "commercial":  2.5,
            "industrial":  12.0,
        }[site["type"]]

        for day in range(DAYS_OF_DATA):
            for hour in range(24):
                timestamp    = start_date + timedelta(days=day, hours=hour)
                is_weekend   = timestamp.weekday() >= 5
                is_peak_hour = 8 <= hour <= 20
                variance     = random.uniform(0.8, 1.2)

                consumption_kwh = round(
                    base_kwh
                    * (1.4 if is_peak_hour else 0.6)
                    * (0.7 if is_weekend else 1.0)
                    * variance,
                    4
                )

                yield {
                    "reading_id":      f"{site['site_id']}_{timestamp.strftime('%Y%m%d%H')}",
                    "site_id":         site["site_id"],
                    "city":            site["city"],
                    "site_type":       site["type"],
                    "timestamp":       timestamp.isoformat(),
                    "hour":            hour,
                    "day_of_week":     timestamp.strftime("%A"),
                    "is_weekend":      is_weekend,
                    "consumption_kwh": consumption_kwh,
                    "cost_eur":        round(consumption_kwh * 0.28, 4),
                }


# ── Source ────────────────────────────────────────────────────────────────────
@dlt.source
def energy_source():
    return meter_readings()


# ── Pipeline ──────────────────────────────────────────────────────────────────
def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="energy_pipeline",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_energy",
    )

    print("🔌 Running energy pipeline...")
    load_info = pipeline.run(energy_source())
    print(f"✅ Done: {load_info}")

    conn  = duckdb.connect(str(DUCKDB_PATH))
    count = conn.execute("SELECT COUNT(*) FROM raw_energy.meter_readings").fetchone()[0]
    print(f"📊 Rows loaded:   {count:,}")
    conn.close()


if __name__ == "__main__":
    run_pipeline()
