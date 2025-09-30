import os, re, time, requests
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
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.DOTALL|re.IGNORECASE)
        out = []
        for row in rows:
            m = re.search(r'teamreport\.php\?team=([A-Z]{2,3})', row)
            if not m:
                continue
            abbr = m.group(1)

            def num(label, fallback=None):
                m2 = re.search(rf"{label}[^0-9]*([0-9]+\.[0-9]+)", row, flags=re.IGNORECASE)
                return float(m2.group(1)) if m2 else fallback

            out.append({
                "Team": abbr,
                "CF/60":  num("CF/60"),
                "CA/60":  num("CA/60")  or FALLBACK_CA60,
                "SF/60":  num("SF/60")  or FALLBACK_SF60,
                "xGF/60": num("xGF/60") or FALLBACK_xGF60,
                "xGA/60": num("xGA/60") or FALLBACK_xGA60,
            })
        df = pd.DataFrame(out)
        df.to_csv(os.path.join(DATA_DIR, "team_stats.csv"), index=False)
        return df
    except Exception as e:
        print("❌ Parse NST team stats failed:", e)
        return pd.DataFrame(columns=["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"])

# --- Player Stats (Skaters) ---
def _parse_nst_player_rows(html):
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
    out = []
    for row in rows:
        m_name = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
        if not m_name:
            continue
        pname = m_name.group(1).strip()

        def pick_num(label):
            pats = [
                rf'{label}\s*/\s*60[^0-9\-]*([0-9]*\.?[0-9]+)',
                rf'{label}60[^0-9\-]*([0-9]*\.?[0-9]+)',
                rf'{label}[^0-9\-]*([0-9]*\.?[0-9]+)'
            ]
            for pat in pats:
                m = re.search(pat, row, flags=re.IGNORECASE)
                if m:
                    try: return float(m.group(1))
                    except: pass
            return None

        g60   = pick_num("G")
        a60   = pick_num("A")
        sog60 = pick_num("S")
        blk60 = pick_num("Blk")
        cf60  = pick_num("CF")
        xgf60 = pick_num("xGF")
        hdcf60= pick_num("HDCF")
        out.append({
            "PlayerRaw": pname,
            "NormName": norm_name(pname),
            "G/60": g60, "A/60": a60, "SOG/60": sog60, "BLK/60": blk60,
            "CF/60": cf60, "xGF/60": xgf60, "HDCF/60": hdcf60
        })
    return pd.DataFrame(out)

def get_team_players(team_code, season_code, tgp=None):
    qs = f"team={team_code}&sit=all&fromseason={season_code}&thruseason={season_code}"
    if tgp:
        qs += f"&tgp={tgp}"
    url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
    tag = f"nst_players_{team_code}_{season_code}_{tgp or 'all'}"
    html = http_get_cached(url, tag=tag)
    if html is None:
        return pd.DataFrame()
    return _parse_nst_player_rows(html)

# --- Goalie Stats ---
def get_goalies(season, last_season=None):
    """
    Pull goalie stats for season, last 10 GP, and last season.
    Do not blend here — just return splits so projections can weight them.
    """
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

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
        out = []
        for row in rows:
            m_name = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
            if not m_name:
                continue
            pname = m_name.group(1).strip()
            sv_match = re.search(r'SV%[^0-9]*([0-9]*\.?[0-9]+)', row, flags=re.IGNORECASE)
            sv_pct = float(sv_match.group(1))/100.0 if sv_match else None
            out.append({
                "PlayerRaw": pname,
                "NormName": norm_name(pname),
                "SV%": sv_pct
            })
        return pd.DataFrame(out)

    season_df = fetch_goalie_stats(season).rename(columns={"SV%": "SV_season"})
    recent_df = fetch_goalie_stats(season, tgp=10).rename(columns={"SV%": "SV_recent"})
    last_df   = fetch_goalie_stats(last_season).rename(columns={"SV%": "SV_last"})

    merged = season_df.merge(recent_df[["NormName","SV_recent"]], on="NormName", how="left")
    merged = merged.merge(last_df[["NormName","SV_last"]], on="NormName", how="left")

    return merged
