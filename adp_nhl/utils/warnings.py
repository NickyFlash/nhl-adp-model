import pandas as pd

def _ensure_int_series(s: pd.Series) -> pd.Series:
    """Ensure a Series is int64, dropping NaNs first."""
    return s.dropna().astype("int64")

def tag_missing_baseline(merged_lineups_df: pd.DataFrame, baseline_players_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a boolean column '_missingBaseline' to merged_lineups_df indicating
    players that do NOT exist in the 2024–25 baseline players parquet.
    
    Priority:
      - If playerId is available → match by ID
      - Else if NormName exists → match by normalized name
    """
    merged = merged_lineups_df.copy()

    if "playerId" in merged.columns and "playerId" in baseline_players_df.columns:
        lineup_ids = set(_ensure_int_series(merged["playerId"]).unique())
        base_ids = set(_ensure_int_series(baseline_players_df["playerId"]).unique())
        missing_ids = lineup_ids - base_ids
        merged["_missingBaseline"] = merged["playerId"].isin(missing_ids)
    elif "NormName" in merged.columns and "NormName" in baseline_players_df.columns:
        base_names = set(baseline_players_df["NormName"].unique())
        merged["_missingBaseline"] = ~merged["NormName"].isin(base_names)
    else:
        # Fallback: nothing to tag
        merged["_missingBaseline"] = False

    return merged

def players_missing_baseline(merged_lineups_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a small table of unique players missing baseline (rookies/new).
    Requires tag_missing_baseline to have been applied first.
    """
    if "_missingBaseline" not in merged_lineups_df.columns:
        return pd.DataFrame(columns=["team", "playerId", "name", "NormName"]).iloc[:0]

    cols = [c for c in ["team", "playerId", "name", "NormName"] if c in merged_lineups_df.columns]
    out = merged_lineups_df.loc[merged_lineups_df["_missingBaseline"], cols].drop_duplicates()
    return out
