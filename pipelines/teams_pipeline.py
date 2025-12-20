import os
import time
import math
from nba_api.stats.endpoints import leaguedashteamstats
from nba_api.stats.static import teams
from dotenv import load_dotenv
import mysql.connector
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# -----------------------------
# Helper functions
# -----------------------------
def safe_int(x):
    try:
        if x is None:
            return None
        return int(x)
    except:
        return None

def safe_float(x):
    try:
        if x is None:
            return None
        f = float(x)
        return None if math.isnan(f) else f
    except:
        return None

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def fetch_team_stats(season, sleep_time=1.0):
    rows = []
    try:
        df = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            season_type_all_star="Regular Season"
        ).get_data_frames()[0]

        time.sleep(sleep_time)

        if df.empty:
            print(f"No data for season {season}")
            return rows

        for _, row in df.iterrows():
            season_id = row.get('SEASON_ID', season)  # fallback
            rows.append((
                safe_int(row.get('TEAM_ID')),
                season_id,
                safe_int(row.get('GP')),
                safe_int(row.get('W')),
                safe_int(row.get('L')),
                safe_float(row.get('W_PCT')),
                safe_int(row.get('FGM')),
                safe_int(row.get('FGA')),
                safe_int(row.get('FG3M')),
                safe_int(row.get('FG3A')),
                safe_int(row.get('FTM')),
                safe_int(row.get('FTA')),
                safe_int(row.get('PTS')),
                safe_int(row.get('REB')),
                safe_int(row.get('AST')),
                safe_int(row.get('STL')),
                safe_int(row.get('BLK')),
                safe_int(row.get('TOV')),
                safe_float(row.get('OFF_RATING')),
                safe_float(row.get('DEF_RATING')),
                safe_float(row.get('PLUS_MINUS'))
            ))

        print(f"Fetched {len(df)} teams for season {season}")

    except Exception as e:
        print(f"Error fetching season {season}: {e}")

    return rows

def fetch_team_advanced_ratings(season, sleep_time=1.0):
    rows = []

    try:
        df = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            season_type_all_star="Regular Season",
            measure_type_detailed_defense="Advanced",
            per_mode_detailed="PerGame"
        ).get_data_frames()[0]

        time.sleep(sleep_time)

        if df.empty:
            print(f"No advanced data for season {season}")
            return rows

        for _, row in df.iterrows():
            rows.append((
                safe_int(row.get("TEAM_ID")),
                season,
                safe_float(row.get("OFF_RATING")),
                safe_float(row.get("DEF_RATING"))
            ))

        print(f"Fetched advanced ratings for {len(rows)} teams ({season})")

    except Exception as e:
        print(f"Error fetching advanced ratings for {season}: {e}")

    return rows

def fetch_team_advanced_stats(season, sleep_time=1.0):
    rows = []
    try:
        df = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            season_type_all_star="Regular Season",
            measure_type_detailed_defense="Advanced",  # Advanced stats
            per_mode_detailed="PerGame",
            pace_adjust="Y",
            plus_minus="Y"
        ).get_data_frames()[0]

        time.sleep(sleep_time)

        if df.empty:
            print(f"No advanced stats for season {season}")
            return rows

        for _, row in df.iterrows():
            rows.append((
                safe_int(row.get('TEAM_ID')),
                season,  # use season string as season_id
                safe_float(row.get('OFF_RATING')),
                safe_float(row.get('DEF_RATING'))
            ))

        print(f"Fetched {len(df)} teams advanced stats for season {season}")

    except Exception as e:
        print(f"Error fetching advanced stats for season {season}: {e}")

    return rows

def upsert_team_advanced_ratings(rows):
    if not rows:
        return

    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO TeamStatsRegularSeason (
        team_id,
        season_id,
        off_rating,
        def_rating
    )
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        off_rating = VALUES(off_rating),
        def_rating = VALUES(def_rating);
    """

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Inserted/updated {len(rows)} advanced rating rows")
    except Exception as e:
        print(f"DB error (advanced ratings): {e}")
    finally:
        cursor.close()
        conn.close()


def upsert_team_stats(rows):
    if not rows:
        return

    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO TeamStatsRegularSeason (
        team_id, season_id, gp, w, l, win_pct, fgm, fga, fg3m, fg3a,
        ftm, fta, pts, reb, ast, stl, blk, tov, off_rating, def_rating, plus_minus
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        gp=VALUES(gp),
        w=VALUES(w),
        l=VALUES(l),
        win_pct=VALUES(win_pct),
        fgm=VALUES(fgm),
        fga=VALUES(fga),
        fg3m=VALUES(fg3m),
        fg3a=VALUES(fg3a),
        ftm=VALUES(ftm),
        fta=VALUES(fta),
        pts=VALUES(pts),
        reb=VALUES(reb),
        ast=VALUES(ast),
        stl=VALUES(stl),
        blk=VALUES(blk),
        tov=VALUES(tov),
        off_rating=VALUES(off_rating),
        def_rating=VALUES(def_rating),
        plus_minus=VALUES(plus_minus);
    """

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Inserted/updated {len(rows)} team-season rows")
    except Exception as e:
        print(f"DB error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    seasons = [f"{y}-{str(y+1)[2:]}" for y in range(2020, 2025)]

    base_rows = []
    for season in seasons:
        rows = fetch_team_stats(season)
        base_rows.extend(rows)

    upsert_team_stats(base_rows)
    print("Base team stats pipeline completed")

    advanced_rows = []
    for season in seasons:
        rows = fetch_team_advanced_ratings(season)
        advanced_rows.extend(rows)

    upsert_team_advanced_ratings(advanced_rows)
    print("Advanced team ratings pipeline completed")
