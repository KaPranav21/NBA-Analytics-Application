import os
import time
from dotenv import load_dotenv
import mysql.connector
from nba_api.stats.endpoints import commonplayerinfo
import warnings

warnings.simplefilter(action="ignore")
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

def safe_int_field(x):
    """Convert x to int or return None if empty/invalid."""
    try:
        if x is None or str(x).strip() == '':
            return None
        return int(x)
    except:
        return None

def parse_height(height_str):
    """
    Convert NBA API height string '6-7' to total inches (79).
    Returns None if empty or invalid.
    """
    try:
        if not height_str or height_str.strip() == '':
            return None
        feet, inches = height_str.split('-')
        return int(feet) * 12 + int(inches)
    except:
        return None

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def fetch_player_ids():
    """Get all player_ids already in PlayerStatsRegularSeason table."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT player_id FROM PlayerStatsRegularSeason")
    ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return ids


def fetch_player_info(player_id, sleep_time=0.8):
    """Fetch minimal CommonPlayerInfo for one player."""
    try:
        info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=60).get_data_frames()[0]
        time.sleep(sleep_time)

        if info.empty:
            return None

        row = info.iloc[0]
        is_active = 1 if row.get("ROSTERSTATUS") == "Active" else 0
        
        return (
            player_id,
            row.get("FIRST_NAME"),
            row.get("LAST_NAME"),
            row.get("BIRTHDATE"),
            row.get("COUNTRY"),
            parse_height(row.get("HEIGHT")),
            safe_int_field(row.get("WEIGHT")),
            row.get("POSITION"),
            1 if row.get("ROSTERSTATUS") == "Active" else 0
        )

    except Exception as e:
        print(f"Error fetching player info {player_id}: {e}")
        return None


def upsert_player_info(rows):
    """Batch upsert minimal player info."""
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO Players (
        player_id, first_name, last_name, birthdate, country, height, weight, position, is_active
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        first_name = VALUES(first_name),
        last_name = VALUES(last_name),
        birthdate = VALUES(birthdate),
        country = VALUES(country),
        height = VALUES(height),
        weight = VALUES(weight),
        position = VALUES(position),
        is_active = VALUES(is_active)
    """

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Upserted {len(rows)} players")
    except Exception as e:
        print(f"DB error: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    player_ids = fetch_player_ids()

    # Define your range
    a = 5000
    b = 5500

    # Slice the player_ids list
    player_ids = player_ids[a:b]

    batch_size = 100
    batch = []

    print(f"Found {len(player_ids)} players to enrich in range {a}:{b}")

    for i, player_id in enumerate(player_ids, start=1):
        info = fetch_player_info(player_id)
        if info:
            batch.append(info)

        if len(batch) >= batch_size:
            upsert_player_info(batch)
            batch.clear()

        if i % batch_size == 0:
            print(f"Processed {i}/{len(player_ids)} players")

    if batch:
        upsert_player_info(batch)

    print("Players info pipeline completed")
    
