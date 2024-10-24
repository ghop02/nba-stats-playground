import arrow
import requests
import os
import json
import gspread
import time
from io import StringIO
from nba_api.stats import endpoints


# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "1c0939dPegfZ4_x8Oit0l9SWhAthDV5ujuqS6zTiYSYQ"
RANGE_NAME = "Games!A1:I"

def get_game_stats(player_id, game_id, season="2024-25", attempt=1):
    print(player_id, game_id)
    try:
        results = endpoints.CumeStatsPlayer(player_id=int(player_id), game_ids=[game_id], season=season).get_data_frames()
    except requests.exceptions.ReadTimeout:
        time.sleep(5)
        if attempt > 4:
            raise
        return get_game_stats(player_id, game_id, season=season, attempt=attempt + 1)

    if not len(results[0]):
        return None
    
    return results[0].to_dict('records')[0]


def main():
    if os.environ.get("GOOGLE_CREDENTIALS"):
        gc = gspread.oauth_from_dict(json.loads(os.environ.get("GOOGLE_CREDENTIALS")))
    else:
        gc = gspread.oauth()
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

        # print(d)

    rows_to_update = [r for r in rows if arrow.get(r["GAME_DATE_YMD"]) < arrow.utcnow() and arrow.get(r["GAME_DATE_YMD"]) >= arrow.utcnow().shift(days=-1)]
    updated_rows = []
    for row in rows_to_update:
        stats = get_game_stats(row["PLAYER_ID"], row["GAME_ID"], season="2023-24")

        time.sleep(3.5)
        if stats:
            new_row = {**row}
            new_row["POINTS"] = stats["PTS"]
            updated_rows.append(new_row)

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