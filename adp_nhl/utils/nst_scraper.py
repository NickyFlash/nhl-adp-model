# -*- coding: utf-8 -*-
"""
NST Scraper (Safe + Patched)
Pulls team, skater, and goalie stats from Natural Stat Trick.
Always returns DataFrames with expected columns so projections never crash.
"""

import os, time, re, requests
import pandas as pd
from datetime import datetime
from io import StringIO
from adp_nhl.utils.common import norm_name

# ---------------- CONFIG ----------------
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(RAW_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (ADP Free Model)"}
TIMEOUT = 60

# Fallbacks
LEAGUE_AVG_SV = 0.905
FALLBACK_CA60, FALLBACK_xGA60 = 58.0, 2.65
FALLBACK_SF60, FALLBACK_xGF60 = 31.0, 2.95

# ---------------- HELPERS ----------------
def http_get_cached(url, tag, sleep=3):
    """Fetch with caching to raw/, sleep to avoid throttling."""
    today = datetime.today().strftime("%Y%m%d")
    cache_file = os.path.join(RAW_DIR, f"{tag}_{today}.html")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return f.read()
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        html = r.text
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write(html)
        time.sleep(sleep)
        return html
    except Exception as e:
        print(f"❌ Fetch error {tag}: {e}")
        return None

def safe_to_numeric(series, default=None):
    """Convert series to float, replace errors with default."""
    return pd.to_numeric(series, errors="coerce").fillna(default)

# ---------------- TEAM STATS ----------------
def get_team_stats(season):
    url = f"https://www.naturalstattrick.com/teamtable.php?fromseason={season}&thruseason={season}&stype=2&sit=all"
    html = http_get_cached(url, tag=f"nst_teamtable_{season}")
    cols = ["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"]

    if not html:
        print("⚠️ NST team stats unavailable; using empty frame.")
        return pd.DataFrame(columns=cols)

    try:
        tables = pd.read_html(StringIO(html))
        df = tables[0]
        # normalize
        if "Team" not in df.columns and "Tm" in df.columns:
            df = df.rename(columns={"Tm": "Team"})
        df = df[["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"]]
        for c in ["CA/60","SF/60","xGF/60","xGA/60"]:
            df[c] = safe_to_numeric(df[c], 
                FALLBACK_CA60 if c=="CA/60" else 
                FALLBACK_SF60 if c=="SF/60" else 
                FALLBACK_xGF60 if c=="xGF/60" else FALLBACK_xGA60)
        df.to_csv(os.path.join(DATA_DIR,"team_stats.csv"),index=False)
        return df
    except Exception as e:
        print(f"❌ Parse NST team stats failed: {e}")
        return pd.DataFrame(columns=cols)

# ---------------- PLAYER (SKATERS) ----------------
def get_team_players(team_code, season_code, tgp=None):
    qs = f"team={team_code}&sit=all&fromseason={season_code}&thruseason={season_code}"
    if tgp: qs += f"&tgp={tgp}"
    url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
    html = http_get_cached(url, tag=f"nst_players_{team_code}_{season_code}_{tgp or 'all'}")
    cols = ["PlayerRaw","NormName","G/60","A/60","SOG/60","BLK/60","CF/60","xGF/60","HDCF/60"]

    if not html: 
        return pd.DataFrame(columns=cols)

    try:
        tables = pd.read_html(StringIO(html))
        df = tables[0]
        # normalize cols
        ren = {c:c.replace(" ","") for c in df.columns}
        df = df.rename(columns=ren)
        out = pd.DataFrame()
        out["PlayerRaw"] = df["Player"]
        out["NormName"] = out["PlayerRaw"].apply(norm_name)
        def col_or_default(col, default=None):
            return safe_to_numeric(df[col], default) if col in df else pd.Series([default]*len(df))
        out["G/60"]    = col_or_default("G/60")
        out["A/60"]    = col_or_default("A/60")
        out["SOG/60"]  = col_or_default("S/60")
        out["BLK/60"]  = col_or_default("Blk/60")
        out["CF/60"]   = col_or_default("CF/60")
        out["xGF/60"]  = col_or_default("xGF/60")
        out["HDCF/60"] = col_or_default("HDCF/60")
        return out
    except Exception as e:
        print(f"❌ NST skater parse failed {team_code}: {e}")
        return pd.DataFrame(columns=cols)

# ---------------- GOALIES ----------------
def get_goalies(season, last_season=None):
    if not last_season:
        last_season = str(int(season[:4])-1) + str(int(season[:4]))

    def fetch(season, tgp=None, label="SV%"):
        qs = f"fromseason={season}&thruseason={season}&sit=all&playerstype=goalies"
        if tgp: qs += f"&tgp={tgp}"
        url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
        html = http_get_cached(url, tag=f"nst_goalies_{season}_{tgp or 'all'}")
        if not html: return pd.DataFrame(columns=["PlayerRaw","NormName",label])
        try:
            tables = pd.read_html(StringIO(html))
            df = tables[0]
            df = df.rename(columns={c:c.strip() for c in df.columns})
            out = pd.DataFrame()
            out["PlayerRaw"] = df["Player"]
            out["NormName"] = out["PlayerRaw"].apply(norm_name)
            sv_col = "SV%" if "SV%" in df.columns else [c for c in df.columns if "SV" in c][0]
            out[label] = safe_to_numeric(df[sv_col], LEAGUE_AVG_SV)
            return out
        except Exception as e:
            print(f"❌ NST goalie parse failed: {e}")
            return pd.DataFrame(columns=["PlayerRaw","NormName",label])

    season_df = fetch(season, None, "SV_season")
    recent_df = fetch(season, 10, "SV_recent")
    last_df   = fetch(last_season, None, "SV_last")

    merged = season_df.merge(recent_df, on="NormName", how="left")
    merged = merged.merge(last_df, on="NormName", how="left")
    return merged
