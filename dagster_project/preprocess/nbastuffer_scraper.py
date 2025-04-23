import pandas as pd
import requests
from bs4 import BeautifulSoup

def scrape_nbastuffer_regular_season_from_url(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {url}")
    
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")

    target_table = None
    for idx, table in enumerate(tables):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "TEAM" in headers and "oEFF" in headers and "dEFF" in headers:
            if idx >= 1:  # make sure it's the second occurrence
                target_table = table
                break

    if not target_table:
        raise ValueError("Could not find the Regular Season table.")

    headers = [th.get_text(strip=True) for th in target_table.find_all("th")]
    rows = target_table.find_all("tr")[1:]  # skip header row

    data = []
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) == len(headers):
            row_data = dict(zip(headers, cols))
            filtered = {
                "Team": row_data.get("TEAM"),
                "GP": row_data.get("GP"),
                "PPG": row_data.get("PPG"),
                "Opp PPG": row_data.get("oPPG"),
                "Pace": row_data.get("PACE"),
                "OffRtg": row_data.get("oEFF"),
                "DefRtg": row_data.get("dEFF"),
                "NetRtg": row_data.get("eDIFF"),
                "Win%": row_data.get("WIN%"),
                "SoS": row_data.get("SoS")
            }
            data.append(filtered)

    return pd.DataFrame(data)

# Example usage
if __name__ == "__main__":
    url = "https://www.nbastuffer.com/2024-2025-nba-team-stats/"
    df_team_stats = scrape_nbastuffer_regular_season_from_url(url)
    print(df_team_stats.head())

    df_team_stats.to_csv("nbastuffer_team_stats.csv", index=False)
