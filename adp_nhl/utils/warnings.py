mport pandas as pd

def _ensure_int_series(s: pd.Series) -> pd.Series:
    return s.dropna().astype("int64")

def tag_missing_baseline(merged_lineups_df: pd.DataFrame, baseline_players_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a boolean column '_missingBaseline' to merged_lineups_df indicating
    players that do NOT exist in the baseline players parquet.
    """
    if "playerId" not in merged_lineups_df.columns:
        # Nothing to tag if we don't have playerId
        merged_lineups_df["_missingBaseline"] = False
        return merged_lineups_df

    lineup_ids = set(_ensure_int_series(merged_lineups_df["playerId"]).unique())
    base_ids = set(_ensure_int_series(baseline_players_df["playerId"]).unique())

    missing_ids = lineup_ids - base_ids
    merged = merged_lineups_df.copy()
    merged["_missingBaseline"] = merged["playerId"].isin(missing_ids)
    return merged

def players_missing_baseline(merged_lineups_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a small table of unique players missing baseline.
    Requires tag_missing_baseline to have been applied.
    """
    if "_missingBaseline" not in merged_lineups_df.columns:
        return pd.DataFrame(columns=["team", "playerId", "name"]).iloc[:0]

    cols = [c for c in ["team", "playerId", "name"] if c in merged_lineups_df.columns]
    out = merged_lineups_df.loc[merged_lineups_df["_missingBaseline"], cols].drop_duplicates()
    return out
