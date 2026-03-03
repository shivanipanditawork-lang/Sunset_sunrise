import requests
import sqlite3
from datetime import datetime, timedelta
import pytz
import time

# -----------------------------
# Configuration
# -----------------------------

LAT = 18.5204   # Pune Latitude
LNG = 73.8567   # Pune Longitude

INITIAL_START_DATE = datetime(2020, 1, 1)
END_DATE = datetime.today()

IST = pytz.timezone("Asia/Kolkata")

DB_NAME = "sunrise.db"
TABLE_NAME = "sunrise_data"

API_URL = "https://api.sunrise-sunset.org/json"

# -----------------------------
# Database Setup
# -----------------------------

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date TEXT PRIMARY KEY,
    sunrise TEXT,
    sunset TEXT,
    day_length INTEGER
)
""")

conn.commit()

# -----------------------------
# Helper Functions
# -----------------------------

def get_last_loaded_date():
    """
    Get latest date already stored in DB.
    If no data exists, return INITIAL_START_DATE.
    """
    cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
    result = cursor.fetchone()[0]

    if result:
        last_date = datetime.strptime(result, "%Y-%m-%d")
        return last_date + timedelta(days=1)
    else:
        return INITIAL_START_DATE


def convert_to_ist(utc_time_str, date_str):
    """
    Convert API UTC time to IST timezone.
    """
    utc_datetime = datetime.strptime(
        f"{date_str} {utc_time_str}",
        "%Y-%m-%d %I:%M:%S %p"
    )
    utc_datetime = pytz.utc.localize(utc_datetime)
    ist_datetime = utc_datetime.astimezone(IST)

    return ist_datetime.strftime("%Y-%m-%d %H:%M:%S")


def fetch_sun_data(date_str):
    """
    Fetch sunrise/sunset data from Sunrise-Sunset API.
    """
    params = {
        "lat": LAT,
        "lng": LNG,
        "date": date_str
    }

    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    if data["status"] != "OK":
        raise Exception("API returned error")

    results = data["results"]

    sunrise_ist = convert_to_ist(results["sunrise"], date_str)
    sunset_ist = convert_to_ist(results["sunset"], date_str)

    # Calculate day length in seconds
    sunrise_dt = datetime.strptime(sunrise_ist, "%Y-%m-%d %H:%M:%S")
    sunset_dt = datetime.strptime(sunset_ist, "%Y-%m-%d %H:%M:%S")

    day_length = int((sunset_dt - sunrise_dt).total_seconds())

    return sunrise_ist, sunset_ist, day_length


# -----------------------------
# Main ETL Process
# -----------------------------

current_date = get_last_loaded_date()

if current_date > END_DATE:
    print("No new data to load. Database is up to date.")
else:
    print(f"Starting incremental load from {current_date.date()}")

    while current_date <= END_DATE:

        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Processing: {date_str}")

        try:
            sunrise, sunset, day_length = fetch_sun_data(date_str)

            cursor.execute(f"""
            INSERT OR REPLACE INTO {TABLE_NAME}
            (date, sunrise, sunset, day_length)
            VALUES (?, ?, ?, ?)
            """, (date_str, sunrise, sunset, day_length))

            conn.commit()

        except Exception as e:
            print(f"Error on {date_str}: {e}")

        time.sleep(0.2)  # Prevent API overloading
        current_date += timedelta(days=1)

conn.close()

print("Pipeline completed successfully!")