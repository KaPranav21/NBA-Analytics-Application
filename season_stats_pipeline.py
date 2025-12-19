import os
import time
import math
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players
from dotenv import load_dotenv
import mysql.connector
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")


def safe_float(x):
    try:
        f = float(x)
        return None if math.isnan(f) else f
    except:
        return None

def safe_int(x):
    f = safe_float(x)
    return None if f is None else int(f)

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def fetch_player_stats(players_batch, sleep_time=1.0):
    """Fetch season stats for a batch of players."""
    rows = []

    for p in players_batch:
        try:
            career = playercareerstats.PlayerCareerStats(player_id=p['id'], timeout=60).get_data_frames()
            time.sleep(sleep_time)

            if career and len(career) > 0 and not career[0].empty:
                df = career[0]
                for _, row in df.iterrows():
                    rows.append([
                        int(row['PLAYER_ID']),
                        row['SEASON_ID'],
                        row['LEAGUE_ID'],
                        safe_int(row['TEAM_ID']),
                        row['TEAM_ABBREVIATION'],
                        safe_int(row.get('PLAYER_AGE')),
                        safe_int(row.get('GP')),
                        safe_int(row.get('GS')),
                        safe_float(row.get('MIN')),
                        safe_int(row.get('FGM')),
                        safe_int(row.get('FGA')),
                        safe_float(row.get('FG_PCT')),
                        safe_int(row.get('FG3M')),
                        safe_int(row.get('FG3A')),
                        safe_float(row.get('FG3_PCT')),
                        safe_int(row.get('FTM')),
                        safe_int(row.get('FTA')),
                        safe_float(row.get('FT_PCT')),
                        safe_int(row.get('OREB')),
                        safe_int(row.get('DREB')),
                        safe_int(row.get('REB')),
                        safe_int(row.get('AST')),
                        safe_int(row.get('STL')),
                        safe_int(row.get('BLK')),
                        safe_int(row.get('TOV')),
                        safe_int(row.get('PF')),
                        safe_int(row.get('PTS')),
                    ])
                print(f"Added stats for {p['first_name']} {p['last_name']} (ID: {p['id']})")

        except Exception as e:
            print(f"Error fetching stats for player {p['id']}: {e}")

    return rows

def upsert_players(players_list):
    """Batch insert/update players."""
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO Players (player_id, first_name, last_name)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        first_name=VALUES(first_name),
        last_name=VALUES(last_name);
    """

    rows = [(p['id'], p['first_name'], p['last_name']) for p in players_list]

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Inserted/updated {len(rows)} players")
    except Exception as e:
        print(f"Error inserting players: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_player_stats(rows):
    """Batch insert/update season stats."""
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO PlayerStatsRegularSeason (
        player_id, season_id, league_id, team_id, team_abbreviation, player_age,
        gp, gs, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, pts
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        player_age=VALUES(player_age),
        gp=VALUES(gp),
        gs=VALUES(gs),
        min=VALUES(min),
        fgm=VALUES(fgm),
        fga=VALUES(fga),
        fg_pct=VALUES(fg_pct),
        fg3m=VALUES(fg3m),
        fg3a=VALUES(fg3a),
        fg3_pct=VALUES(fg3_pct),
        ftm=VALUES(ftm),
        fta=VALUES(fta),
        ft_pct=VALUES(ft_pct),
        oreb=VALUES(oreb),
        dreb=VALUES(dreb),
        reb=VALUES(reb),
        ast=VALUES(ast),
        stl=VALUES(stl),
        blk=VALUES(blk),
        tov=VALUES(tov),
        pf=VALUES(pf),
        pts=VALUES(pts);
    """

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Inserted/updated {len(rows)} season stats")
    except Exception as e:
        print(f"Error inserting season stats: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    all_players = players.get_players()[5200:5500]
    batch_size = 100

    upsert_players(all_players)

    for i in range(0, len(all_players), batch_size):
        print('\nBatch : ', i)
        batch = all_players[i:i + batch_size]
        stats_rows = fetch_player_stats(batch)
        if stats_rows:
            insert_player_stats(stats_rows)  
