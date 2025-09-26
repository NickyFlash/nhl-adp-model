from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = ROOT / "data" / "processed"

def load_processed():
    """
    Load all processed baseline tables from parquet (2024–25).
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
    Join 2025–26 projected lineups (API) with 2024–25 baseline stats.
    
    Args:
        lineups_json (dict): JSON response from the lineups API.
    
    Returns:
        DataFrame: merged lineups with baseline stats (where available).
    """
    skaters, goalies, _, _, players = load_processed()

    # Flatten lineup JSON -> DataFrame
    if not lineups_json or "data" not in lineups_json:
        return pd.DataFrame()

    lineup_data = lineups_json["data"]
    if isinstance(lineup_data, dict):
        lineup_data = [lineup_data]

    df_lineups = pd.json_normalize(lineup_data)

    # Merge with baseline skaters on playerId if present
    if "playerId" in df_lineups.columns:
        merged = df_lineups.merge(
            skaters, on="playerId", how="left", suffixes=("", "_baseline")
        )
    else:
        merged = df_lineups

    return merged

def join_with_nst(merged_lineups: pd.DataFrame, nst_df: pd.DataFrame):
    """
    Join merged lineups (baseline + API) with 2025–26 NST stats.

    Args:
        merged_lineups (DataFrame): DataFrame from join_lineups_with_baseline.
        nst_df (DataFrame): DataFrame of 2025–26 NST skater stats (already blended).

    Returns:
        DataFrame: full merged table with baseline + NST stats.
    """
    if merged_lineups.empty or nst_df.empty:
        print("⚠️ Cannot join with NST — one or both inputs empty.")
        return merged_lineups

    # Expect both to have NormName column
    if "NormName" not in merged_lineups.columns and "name" in merged_lineups.columns:
        merged_lineups["NormName"] = merged_lineups["name"].str.upper().str.strip()

    out = merged_lineups.merge(
        nst_df,
        how="left",
        left_on="NormName",
        right_on="NormName",
        suffixes=("", "_nst")
    )

    return out
