import os
import pandas as pd
import pytest
from adp_nhl.utils import nst

DATA_DIR = "data"

# ---------------------------- HELPERS ----------------------------
def test_blend_three_layer_basic():
    """Blend returns correct weighted average."""
    result = nst.blend_three_layer(1.0, 2.0, 3.0)
    # weights: 0.50*1 + 0.35*2 + 0.15*3 = 1.65
    assert abs(result - 1.65) < 1e-6

def test_blend_three_layer_missing():
    """Blend ignores NaN inputs."""
    result = nst.blend_three_layer(None, 2.0, None)
    # only 2.0 with weight 0.35 → should equal 2.0
    assert abs(result - 2.0) < 1e-6

# ---------------------------- TEAM STATS ----------------------------
def test_get_team_stats_returns_df():
    df = nst.get_team_stats()
    assert isinstance(df, pd.DataFrame)
    assert set(["Team","CA/60","xGA/60","SF/60","xGF/60"]).issubset(df.columns)

# ---------------------------- GOALIES ----------------------------
def test_get_goalie_stats_returns_df():
    df = nst.get_goalie_stats()
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "NormName" in df.columns
        assert "SV%" in df.columns

# ---------------------------- SKATERS ----------------------------
def test_fetch_nst_player_stats_multi_returns_df():
    df = nst.fetch_nst_player_stats_multi(["DET","BOS"])
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "NormName" in df.columns
        assert any(col.startswith("B_") for col in df.columns)

# ---------------------------- CACHING ----------------------------
def test_caching_team_stats(tmp_path, monkeypatch):
    """Ensure team stats cache file is written."""
    cache_file = os.path.join(DATA_DIR, "raw", "nst_teamtable_" + 
                              pd.Timestamp.today().strftime("%Y%m%d") + ".html")
    # Force re-download
    if os.path.exists(cache_file):
        os.remove(cache_file)
    df = nst.get_team_stats()
    assert os.path.exists(cache_file)
    assert isinstance(df, pd.DataFrame)
