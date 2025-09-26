from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"

SKATERS_PARQ = DATA_PROCESSED / "skater_stats_2024_25.parquet"
GOALIES_PARQ = DATA_PROCESSED / "goalie_stats_2024_25.parquet"
LINES_PARQ   = DATA_PROCESSED / "line_stats_2024_25.parquet"
TEAMS_PARQ   = DATA_PROCESSED / "teams.parquet"
PLAYERS_PARQ = DATA_PROCESSED / "players_2024_25.parquet"

BASELINE_SEASON_INT = 2024  # represents 2024–25 season

def _read_csv(name):
    p = DATA_RAW / name
    if not p.exists():
        raise FileNotFoundError(f"Missing raw file: {p}")
    return pd.read_csv(p, low_memory=False)

def ingest_baseline():
    """Load baseline CSVs (2024–25) and save as parquet for faster use."""
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    teams   = _read_csv("teams.csv")
    lines   = _read_csv("lines.csv")
    goalies = _read_csv("goalies.csv")
    skaters = _read_csv("skaters.csv")

    # Keep only 5v5 lines as synergy baseline
    if "situation" in lines.columns:
        lines = lines[lines["situation"] == "5on5"].copy()
    lines["_low_toi"] = lines.get("icetime", 0) < 60 if "icetime" in lines else False

    # Build player dimension
    if "playerId" in skaters.columns and "name" in skaters.columns:
        sk_players = skaters[["playerId", "name", "team"]].drop_duplicates()
    else:
        sk_players = pd.DataFrame()

    if "playerId" in goalies.columns and "name" in goalies.columns:
        go_players = goalies[["playerId", "name", "team"]].drop_duplicates()
    else:
        go_players = pd.DataFrame()

    players = pd.concat([sk_players, go_players], ignore_index=True).drop_duplicates("playerId")

    # Save parquet
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

def ingest_baseline_if_needed():
    """Only ingest if parquet files don’t exist yet."""
    if all(p.exists() for p in [SKATERS_PARQ, GOALIES_PARQ, LINES_PARQ, TEAMS_PARQ, PLAYERS_PARQ]):
        return {"status": "skipped", "reason": "already ingested"}
    return ingest_baseline()
