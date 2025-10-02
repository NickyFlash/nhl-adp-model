import os, time, requests
import pandas as pd
from datetime import datetime
from adp_nhl.utils.common import norm_name

# Config
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(RAW_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (ADP Free Model)"}
TIMEOUT = 60
LEAGUE_AVG_SV = 0.905

# Fallback values if NST fails
FALLBACK_CA60  = 58.0
FALLBACK_xGA60 = 2.65
FALLBACK_SF60  = 31.0
FALLBACK_xGF60 = 2.95

# --- Simple cache fetch ---
def http_get_cached(url, tag, sleep=3):
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
        print(f"❌ Fetch error {tag}:", e)
        return None

# --- Team Stats ---
def get_team_stats(season):
    url = f"https://www.naturalstattrick.com/teamtable.php?fromseason={season}&thruseason={season}&stype=2&sit=all"
    html = http_get_cached(url, tag=f"nst_teamtable_{season}", sleep=3)
    if html is None:
        print("⚠️ NST team stats unavailable; using league fallbacks.")
        return pd.DataFrame(columns=["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"])
    try:
        tables = pd.read_html(html)
        if not tables:
            raise ValueError("No tables parsed from NST team page")

        df = tables[0]
        df.columns = [c.strip() for c in df.columns]

        # Ensure correct naming
        if "Team" not in df.columns and "Tm" in df.columns:
            df = df.rename(columns={"Tm": "Team"})

        keep_cols = ["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None

        df = df[keep_cols].copy()
        df.to_csv(os.path.join(DATA_DIR, "team_stats.csv"), index=False)
        return df
    except Exception as e:
        print("❌ Parse NST team stats failed:", e)
        return pd.DataFrame(columns=["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"])

# --- Player Stats (Skaters) ---
def get_team_players(team_code, season_code, tgp=None):
    qs = f"team={team_code}&sit=all&fromseason={season_code}&thruseason={season_code}"
    if tgp:
        qs += f"&tgp={tgp}"
    url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
    tag = f"nst_players_{team_code}_{season_code}_{tgp or 'all'}"
    html = http_get_cached(url, tag=tag)
    if html is None:
        return pd.DataFrame()

    try:
        tables = pd.read_html(html)
        if not tables:
            return pd.DataFrame()

        df = tables[0]
        df.columns = [c.strip() for c in df.columns]

        # Map to consistent names
        cols_map = {
            "Player": "PlayerRaw",
            "G/60": "G/60",
            "A/60": "A/60",
            "S/60": "SOG/60",
            "CF/60": "CF/60",
            "xGF/60": "xGF/60",
            "HDCF/60": "HDCF/60",
            "Blk/60": "BLK/60"
        }

        for c in cols_map:
            if c not in df.columns:
                df[c] = None

        out = df.rename(columns=cols_map)
        out["NormName"] = out["PlayerRaw"].apply(norm_name)
        return out[list(cols_map.values()) + ["NormName"]]
    except Exception as e:
        print("❌ Failed NST player parse:", e)
        return pd.DataFrame()

# --- Goalie Stats ---
def get_goalies(season, last_season=None):
    if not last_season:
        last_season = str(int(season[:4]) - 1) + str(int(season[:4]))

    def fetch_goalie_stats(season, tgp=None):
        qs = f"fromseason={season}&thruseason={season}&sit=all&playerstype=goalies"
        if tgp:
            qs += f"&tgp={tgp}"
        url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
        tag = f"nst_goalies_{season}_{tgp or 'all'}"
        html = http_get_cached(url, tag=tag, sleep=3)
        if html is None:
            return pd.DataFrame()
        try:
            tables = pd.read_html(html)
            if not tables:
                return pd.DataFrame()
            df = tables[0]
            df.columns = [c.strip() for c in df.columns]

            if "Player" not in df.columns:
                return pd.DataFrame()

            if "SV%" not in df.columns:
                df["SV%"] = None

            out = pd.DataFrame({
                "PlayerRaw": df["Player"],
                "NormName": df["Player"].apply(norm_name),
                "SV%": df["SV%"].astype(str).str.replace("%","").astype(float)/100.0
            })
            return out
        except Exception as e:
            print("❌ Failed NST goalie parse:", e)
            return pd.DataFrame()

    season_df = fetch_goalie_stats(season).rename(columns={"SV%": "SV_season"})
    recent_df = fetch_goalie_stats(season, tgp=10).rename(columns={"SV%": "SV_recent"})
    last_df   = fetch_goalie_stats(last_season).rename(columns={"SV%": "SV_last"})

    merged = season_df.merge(recent_df[["NormName","SV_recent"]], on="NormName", how="left")
    merged = merged.merge(last_df[["NormName","SV_last"]], on="NormName", how="left")
    return merged
