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

class GetProxy:

    def __init__(self):
        self.active_proxy = None



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


def get_game_stats(player_id, game_id, season="2024-25", proxy=None):
    results = query_nba_api(player_id, game_id, season="2024-25", proxy=None)
    if not results:
        return None

    results = results['resultSets']

    rows = results_to_rows(results[0])
    print(rows)
    if not rows:
        return None
    return rows[0]

    # return results[0].to_dict('records')[0]


def main():
    sleep_time = float(os.environ.get("SLEEP") or 3.5)
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

    rows_to_update = [r for r in rows if arrow.get(r["GAME_DATE_YMD"]) <= arrow.utcnow() and arrow.get(r["GAME_DATE_YMD"]) >= arrow.utcnow().shift(days=-3)]
    updated_rows = []

    proxy = None
    if USE_PROXY:
        proxy = FreeProxy(https=True).get()
        print(f"Using proxy {proxy}")

    for row in rows_to_update:
        print(f"Updating {row['PLAYER_NAME']}, {row['FORMATTED_GAME']}")
        stats = get_game_stats(row["PLAYER_ID"], row["GAME_ID"], season="2023-24", proxy=proxy)

        if stats:
            new_row = {**row}
            new_row["POINTS"] = stats["PTS"]
            updated_rows.append(new_row)
        else:
            print("Failed to query API. Continuing...")

        time.sleep(sleep_time)

    columns_to_update = ["POINTS"]
    for i, row in enumerate(rows):
        found_rows = [r for r in updated_rows if r["PLAYER_ID"] == row["PLAYER_ID"] and r["GAME_ID"] == row["GAME_ID"]]
        if found_rows and len(found_rows) > 1:
            raise ValueError("Duplicate rows!!")
        elif not found_rows:
            continue

        found_row = found_rows[0]
        for col in columns_to_update:
            j = header.index(col)
            worksheet.update_cell(i+2, j+1, found_row[col])

if __name__ == "__main__":
    main()