"""
Weather Pipeline
Fetches real hourly weather data for 5 Romanian cities from Open-Meteo API.
No API key required. Data covers the last 30 days.
"""

import dlt
import duckdb
import requests
from datetime import datetime, timedelta
from pathlib import Path

# --- paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH  = PROJECT_ROOT / "data" / "processed" / "weather.duckdb"

# --- config ---
CITIES = [
    {"name": "Bucharest",   "latitude": 44.4268, "longitude": 26.1025},
    {"name": "Cluj-Napoca", "latitude": 46.7712, "longitude": 23.6236},
    {"name": "Iasi",        "latitude": 47.1585, "longitude": 27.6014},
    {"name": "Timisoara",   "latitude": 45.7489, "longitude": 21.2087},
    {"name": "Constanta",   "latitude": 44.1598, "longitude": 28.6348},
]

# Open-Meteo variables we want
HOURLY_VARIABLES = [
    "temperature_2m",        # temperature at 2m height (°C)
    "relative_humidity_2m",  # humidity (%)
    "wind_speed_10m",        # wind speed (km/h)
    "precipitation",         # rain/snow (mm)
    "cloud_cover",           # cloud coverage (%)
    "is_day",                # 1 = day, 0 = night
]

# --- api ---
def fetch_weather(city: dict, days: int = 30) -> dict:
    """
    Fetch hourly weather data from Open-Meteo API for a given city.
    Returns raw JSON response.
    """
    end_date   = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    url    = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":        city["latitude"],
        "longitude":       city["longitude"],
        "hourly":          ",".join(HOURLY_VARIABLES),
        "start_date":      start_date.isoformat(),
        "end_date":        end_date.isoformat(),
        "timezone":        "Europe/Bucharest",
        "wind_speed_unit": "kmh",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()  # raises exception if API returns error

    return response.json()

# --- resource ---
@dlt.resource(name="hourly_weather", write_disposition="replace")
def hourly_weather():
    """
    Fetch hourly weather for all Romanian cities.
    Yields one row per city per hour — ~3,600 rows total.
    """
    for city in CITIES:
        print(f"  Fetching {city['name']}...")
        data = fetch_weather(city)

        timestamps = data["hourly"]["time"]

        for i, timestamp in enumerate(timestamps):
            yield {
                "city":             city["name"],
                "latitude":         city["latitude"],
                "longitude":        city["longitude"],
                "timestamp":        timestamp,
                "hour":             datetime.fromisoformat(timestamp).hour,
                "date":             datetime.fromisoformat(timestamp).date().isoformat(),
                "day_of_week":      datetime.fromisoformat(timestamp).strftime("%A"),
                "is_day":           bool(data["hourly"]["is_day"][i]),
                "temperature_c":    data["hourly"]["temperature_2m"][i],
                "humidity_pct":     data["hourly"]["relative_humidity_2m"][i],
                "wind_speed_kmh":   data["hourly"]["wind_speed_10m"][i],
                "precipitation_mm": data["hourly"]["precipitation"][i],
                "cloud_cover_pct":  data["hourly"]["cloud_cover"][i],
            }

# --- source ---
@dlt.source
def weather_source():
    return hourly_weather()


# --- pipeline ---
def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="weather_pipeline",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_weather",
    )

    print("🌦️ Fetching real weather data for Romanian cities...")
    load_info = pipeline.run(weather_source())
    print(f"✅ Done: {load_info}")

    # --- sanity check ---
    conn = duckdb.connect(str(DUCKDB_PATH))

    count = conn.execute(
        "SELECT COUNT(*) FROM raw_weather.hourly_weather"
    ).fetchone()[0]
    print(f"\n📊 Total rows: {count:,}")

    print("\n🌡️ Average temperature by city (last 7 days):")
    print(conn.execute("""
        SELECT
            city,
            ROUND(AVG(temperature_c), 1)    AS avg_temp_c,
            ROUND(MAX(temperature_c), 1)    AS max_temp_c,
            ROUND(MIN(temperature_c), 1)    AS min_temp_c,
            ROUND(AVG(humidity_pct), 1)     AS avg_humidity_pct,
            ROUND(SUM(precipitation_mm), 1) AS total_rain_mm
        FROM raw_weather.hourly_weather
        WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY city
        ORDER BY avg_temp_c DESC
    """).df().to_string())

    conn.close()


if __name__ == "__main__":
    run_pipeline()