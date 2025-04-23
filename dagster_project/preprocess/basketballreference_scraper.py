import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = "https://www.basketball-reference.com/leagues/NBA_2025_per_game.html"

def scrape_basketball_reference():
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"id": "per_game_stats"})
    headers = [th.text for th in table.find("thead").find_all("th")][1:]  # Skip rank column
    rows = table.find("tbody").find_all("tr")

    data = []
    for row in rows:
        if row.find("th", {"scope": "row"}) is None:  # Skip separators
            continue
        stats = [td.text for td in row.find_all("td")]
        if stats:
            player_data = dict(zip(headers, stats))
            # Pick only the relevant fields
            filtered = {
                "Player": player_data.get("Player"),
                "Team": player_data.get("Team"),
                "PTS": player_data.get("PTS"),
                "AST": player_data.get("AST"),
                "TRB": player_data.get("TRB")
            }
            data.append(filtered)

    df = pd.DataFrame(data)
    return df

# Example use
if __name__ == "__main__":
    df_bref = scrape_basketball_reference()
    print(df_bref.head())
    df_bref.to_csv("basketball_reference_stats.csv", index=False)
