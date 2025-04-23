from dagster import asset
import pandas as pd
import matplotlib.pyplot as plt

# 1. Top scorers chart
@asset
def top_scorers_chart(processed_stats: pd.DataFrame) -> None:
    top_players = processed_stats.sort_values(by="pts_per_game", ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    bars = plt.barh(top_players["player"], top_players["pts_per_game"], color='orange')
    for bar, team in zip(bars, top_players["team_code"]):
        plt.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, team, va='center')
    plt.xlabel("Points Per Game")
    plt.title("Top 10 NBA Scorers")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig("preprocess/top_scorers.png")

# 2. Top assist leaders
@asset
def assist_leaders_chart(processed_stats: pd.DataFrame) -> None:
    top_ast = processed_stats.sort_values(by="ast_per_game", ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    bars = plt.barh(top_ast["player"], top_ast["ast_per_game"], color='skyblue')
    for bar, team in zip(bars, top_ast["team_code"]):
        plt.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, team, va='center')
    plt.xlabel("Assists Per Game")
    plt.title("Top 10 NBA Assist Leaders")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig("preprocess/top_assists.png")

# 3. Best offensive teams
@asset
def best_team_offense_chart(processed_stats: pd.DataFrame) -> None:
    # Group by team, take the mean (or max depending on your context)
    grouped = processed_stats.groupby("team_code")["offensive_rating"].mean().sort_values(ascending=False).head(5)

    plt.figure(figsize=(8, 5))
    bars = plt.bar(grouped.index, grouped.values, color='green')
    plt.ylabel("Offensive Rating")
    plt.title("Top 5 Offensive Teams")
    plt.tight_layout()
    plt.savefig("preprocess/best_offense.png")


# 4. Team PPG ranking (highest to lowest)
@asset
def team_ppg_bar_chart(processed_stats: pd.DataFrame) -> None:
    # sort all teams by team_ppg descending
    df = processed_stats.sort_values("team_ppg", ascending=False)
    plt.figure(figsize=(10, 6))
    plt.barh(df["team_code"], df["team_ppg"])
    plt.gca().invert_yaxis()
    plt.xlabel("Points Per Game (Team PPG)")
    plt.title("Team PPG Ranking (High → Low)")
    plt.tight_layout()
    plt.savefig("preprocess/team_ppg_ranking.png")


# 5. Net Rating vs Win% scatter plot
@asset
def net_rating_vs_win_scatter(processed_stats: pd.DataFrame) -> None:
    plt.figure(figsize=(8, 6))
    plt.scatter(processed_stats["net_rating"], processed_stats["win_percentage"], color='purple')
    for i, row in processed_stats.iterrows():
        plt.text(row["net_rating"] + 0.2, row["win_percentage"], row["team_code"], fontsize=8)
    plt.xlabel("Net Rating")
    plt.ylabel("Win Percentage")
    plt.title("Net Rating vs Win % by Team")
    plt.tight_layout()
    plt.savefig("preprocess/netrating_vs_win.png")

# 6. Top 10 all-around players based on z-scores   
@asset
def top_all_around_players_chart(processed_stats: pd.DataFrame) -> None:
    top10 = processed_stats.head(10)

    plt.figure()
    plt.barh(top10["player"], top10["all_around_score"])
    plt.gca().invert_yaxis()
    plt.xlabel("All-Around Score (sum of z-scores)")
    plt.title("Top 10 All-Around NBA Players")
    plt.tight_layout()
    plt.savefig("preprocess/top_all_around.png")

# 7. Team Pace ranking (fastest to slowest)
@asset
def team_pace_bar_chart(processed_stats: pd.DataFrame) -> None:
    df = processed_stats.sort_values("pace", ascending=False)
    plt.figure(figsize=(10, 6))
    plt.barh(df["team_code"], df["pace"])
    plt.gca().invert_yaxis()
    plt.xlabel("Pace (possessions per game)")
    plt.title("Team Pace Ranking (Fast → Slow)")
    plt.tight_layout()
    plt.savefig("preprocess/team_pace_ranking.png")