# -*- coding: utf-8 -*-
"""
ETL for ADP NHL Model
- Ingest 2024–25 baseline CSVs from data/raw/
- Save standardized parquet files into data/processed/
- Build a unified players table (skaters + goalies)
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"

# Standard parquet outputs
SKATERS_PARQ = DATA_PROCESSED / "skater_stats_2024_25.parquet"
GOALIES_PARQ = DATA_PROCESSED / "goalie_stats_2024_25.parquet"
LINES_PARQ   = DATA_PROCESSED / "line_stats_2024_25.parquet"
TEAMS_PARQ   = DATA_PROCESSED / "teams.parquet"
PLAYERS_PARQ = DATA_PROCESSED / "players_2024_25.parquet"

BASELINE_SEASON_INT = 2024  # represents 2024–25 season

def _read_csv(name: str) -> pd.DataFrame:
    """
    Read a CSV from data/raw/ with some defaults.
    Raises FileNotFoundError if missing.
    """
    p = DATA_RAW / name
    if not p.exists():
        raise FileNotFoundError(f"Missing raw file: {p}")
    return pd.read_csv(p, low_memory=False)

def ingest_baseline() -> dict:
    """
    Load baseline CSVs (2024–25) and save as parquet for faster, consistent use.

    Returns:
        dict: summary of shapes for each processed dataset
    """
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    # ---------------------------- Load raw CSVs ----------------------------
    teams   = _read_csv("teams.csv")
    lines   = _read_csv("lines.csv")
    goalies = _read_csv("goalies.csv")
    skaters = _read_csv("skaters.csv")

    # ---------------------------- Clean Lines ----------------------------
    if "situation" in lines.columns:
        # Keep only 5v5 line combinations as synergy baseline
        lines = lines[lines["situation"] == "5on5"].copy()

    if "icetime" in lines.columns:
        lines["_low_toi"] = lines["icetime"] < 60
    else:
        lines["_low_toi"] = False

    # ---------------------------- Build Players ----------------------------
    if {"playerId", "name", "team"}.issubset(skaters.columns):
        sk_players = skaters[["playerId", "name", "team"]].drop_duplicates()
    else:
        sk_players = pd.DataFrame(columns=["playerId", "name", "team"])

    if {"playerId", "name", "team"}.issubset(goalies.columns):
        go_players = goalies[["playerId", "name", "team"]].drop_duplicates()
    else:
        go_players = pd.DataFrame(columns=["playerId", "name", "team"])

    players = pd.concat([sk_players, go_players], ignore_index=True).drop_duplicates("playerId")

    # ---------------------------- Save parquet ----------------------------
    teams.to_parquet(TEAMS_PARQ, index=False)
    lines.to_parquet(LINES_PARQ, index=False)
    goalies.to_parquet(GOALIES_PARQ, index=False)
    skaters.to_parquet(SKATERS_PARQ, index=False)
    players.to_parquet(PLAYERS_PARQ, index=False)

    return {
        "teams": teams.shape,
        "lines": lines.shape,
        "goalies": goalies.shape,
        "skaters": skaters.shape,
        "players": players.shape,
    }

def ingest_baseline_if_needed() -> dict:
    """
    Only ingest if parquet files don’t exist yet.
    Returns either 'skipped' or the ingest summary.
    """
    if all(p.exists() for p in [SKATERS_PARQ, GOALIES_PARQ, LINES_PARQ, TEAMS_PARQ, PLAYERS_PARQ]):
        return {"status": "skipped", "reason": "already ingested"}
    return ingest_baseline()
