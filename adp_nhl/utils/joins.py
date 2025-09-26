from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = ROOT / "data" / "processed"

def load_processed():
    """
    Load all processed baseline tables from parquet.
    Returns: (skaters, goalies, lines, teams, players)
    """
    skaters = pd.read_parquet(DATA_PROCESSED / "skater_stats_2024_25.parquet")
    goalies = pd.read_parquet(DATA_PROCESSED / "goalie_stats_2024_25.parquet")
    lines   = pd.read_parquet(DATA_PROCESSED / "line_stats_2024_25.parquet")
    teams   = pd.read_parquet(DATA_PROCESSED / "teams.parquet")
    players = pd.read_parquet(DATA_PROCESSED / "players_2024_25.parquet")

    return skaters, goalies, lines, teams, players

def join_lineups_with_baseline(lineups_json: dict):
    """
    Example join:
    Take the lineups JSON from the API and attach baseline stats for each player.
    Returns a DataFrame that can be used in projections.
    """
    skaters, goalies, _, _, players = load_processed()

    # Flatten lineup JSON -> DataFrame
    if not lineups_json or "data" not in lineups_json:
        return pd.DataFrame()

    lineup_data = lineups_json["data"]
    if isinstance(lineup_data, dict):
        lineup_data = [lineup_data]

    df_lineups = pd.json_normalize(lineup_data)

    # Merge with baseline skaters
    if "playerId" in df_lineups.columns:
        merged = df_lineups.merge(
            skaters, on="playerId", how="left", suffixes=("", "_baseline")
        )
    else:
        merged = df_lineups

    return merged
