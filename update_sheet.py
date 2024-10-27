import arrow
import requests
import os
import json
import gspread
import time
from io import StringIO
from nba_api.stats import endpoints
from nba_api.stats.library import http
from fp.fp import FreeProxy
USE_PROXY = True
RANDOMIZE_PROXY = True
# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "1c0939dPegfZ4_x8Oit0l9SWhAthDV5ujuqS6zTiYSYQ"
RANGE_NAME = "Games!A1:I"


def results_to_rows(results):
    rows = []
    for row in results['rowSet']:
        d = {}
        for i, v in enumerate(row):
            d[results['headers'][i]] = v
        rows.append(d)
    return rows


def query_nba_api(player_id, game_id, season="2024-25", proxy=None, attempt=1):
    try:
        results = endpoints.CumeStatsPlayer(
            player_id=int(player_id), game_ids=[game_id], season=season, proxy=proxy
        ).get_dict()
        return results
    except requests.exceptions.ReadTimeout:
        print(f"Request timeout. Sleeping for 5s... attempt={attempt}")
        if proxy and RANDOMIZE_PROXY:
            proxy = FreeProxy(https=True, random=True).get()
            print(f"Chose new proxy: {proxy}")

        time.sleep(5)
        if attempt > 4:
            raise
        return query_nba_api(player_id, game_id, season=season, proxy=proxy, attempt=attempt + 1)
    except json.decoder.JSONDecodeError:
        print("JSON decoder error")
        return None
    except requests.exceptions.ProxyError:
        print(f"Proxy failure. trying a different one (attempt={attempt})")
        if proxy and RANDOMIZE_PROXY:
            proxy = FreeProxy(https=True, random=True).get()
            print(f"Chose new proxy: {proxy}")

        if attempt > 4:
            raise
        return query_nba_api(player_id, game_id, season=season, proxy=proxy, attempt=attempt + 1)

def get_game_stats(player_id, game_id, season="2024-25", proxy=None):
    results = query_nba_api(player_id, game_id, season="2024-25", proxy=proxy)
    if not results:
        return None

    results = results['resultSets']

    rows = results_to_rows(results[0])
    if not rows:
        return None
    return rows[0]

    # return results[0].to_dict('records')[0]

def _update_row(worksheet, header, rows, row_to_update):
    print("Updating Google Sheet row")
    columns_to_update = ["POINTS", "UPDATED_AT"]
    for i, row in enumerate(rows):
        should_update = (
            row_to_update["PLAYER_ID"] == row["PLAYER_ID"] 
            and row_to_update["GAME_ID"] == row["GAME_ID"]
        )
        if not should_update:
            continue

        for col in columns_to_update:
            j = header.index(col)
            worksheet.update_cell(i+2, j+1, row_to_update[col])


def main():
    sleep_time = float(os.environ.get("SLEEP") or 2.5)
    print(f"Sleep time set at {sleep_time}s")
    if os.environ.get("GOOGLE_SERVICE_ACCOUNT"):
        service_account = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT"))
        gc = gspread.service_account_from_dict(service_account)
    else:
        gc = gspread.service_account()

    sheet = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sheet.worksheet("Games")
    original_rows = worksheet.get()
    header = original_rows[0]
    rows = []
    for row in original_rows[1:]:
        d = {}
        for i, val in enumerate(row):
            d[header[i]] = val
        rows.append(d)

    rows_to_update = []
    for row in rows:
        game_time = arrow.get(f"{row['GAME_DATE_YMD']} {row['GAME_TIME']}", "YYYY-MM-DD h:mm A").replace(tzinfo='US/Eastern')
        now = arrow.utcnow()
        updated_at = None

        try:
            updated_at = arrow.get(row.get("UPDATED_AT"))
        except:
            pass

        if updated_at and updated_at > game_time.shift(hours=6):
            # Stop updating rows 6 hours after the game
            continue

        if game_time > now:
            # Don't update games that haven't started yet
            continue

        rows_to_update.append(row)

    updated_rows = []

    proxy = None
    if USE_PROXY:
        proxy = FreeProxy(https=True).get()
        print(f"Using proxy {proxy}")

    print(f"Updating {len(rows_to_update)} rows")
    for row in rows_to_update:
        game = f"{row['PLAYER_NAME']}, {row['FORMATTED_GAME']}"
        print(f"Updating {game}")
        stats = get_game_stats(row["PLAYER_ID"], row["GAME_ID"], season="2023-24", proxy=proxy)
        game_time = arrow.get(f"{row['GAME_DATE_YMD']} {row['GAME_TIME']}", "YYYY-MM-DD h:mm A").replace(tzinfo='US/Eastern')
        if stats:
            new_row = {**row}
            new_row["POINTS"] = stats["PTS"]
            new_row["UPDATED_AT"] = str(arrow.utcnow())
            updated_rows.append(new_row)
            _update_row(worksheet, header, rows, new_row)
        elif game_time < arrow.utcnow().shift(days=-1):
            print("No results for a game over a day ago, counting as 0")
            # Games that don't return any results and were over a day ago we assume that they did not play
            new_row = {**row}
            new_row["POINTS"] = 0
            new_row["UPDATED_AT"] = str(arrow.utcnow())
            updated_rows.append(new_row)
            _update_row(worksheet, header, rows, new_row)
        else:
            print("No results returned. Continuing...")

        time.sleep(sleep_time)

    print(f"Updated {len(updated_rows)}/{len(rows_to_update)} rows")


if __name__ == "__main__":
    main()