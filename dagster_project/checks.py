import requests
from bs4 import BeautifulSoup

URL = "https://www.basketball-reference.com/leagues/NBA_2024_per_game.html"
resp = requests.get(URL, headers={"User-Agent":"Mozilla/5.0"})
soup = BeautifulSoup(resp.text, "html.parser")

# 1) Check whether BeautifulSoup sees any table with id=per_game_stats at all:
table = soup.find("table", {"id":"per_game_stats"})
print("TABLE FOUND?", bool(table))

# 2) If it is found, how many <tr> in its <tbody>?
if table:
    tbody = table.find("tbody")
    print("NUMBER OF ROWS IN TBODY:", len(tbody.find_all("tr")))
