# -*- coding: utf-8 -*-
"""
NST Data Fetchers for 2025–26 season
- Team-level stats
- Goalie stats
- Skater stats (blended: last 10 GP, last 30 days, season-to-date, last season)
"""

import os, re
import pandas as pd
from datetime import datetime
from adp_nhl.utils.common import norm_name, http_get_cached

# ---------------------------- CONFIG ----------------------------
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

LEAGUE_AVG_SV = 0.905
FALLBACK_SF60  = 31.0
FALLBACK_xGF60 = 2.95
FALLBACK_CA60  = 58.0
FALLBACK_xGA60 = 2.65

SETTINGS = {
    "sleep_nst": 3.0
}

# Detect current NHL season
today = datetime.today()
year = today.year
month = today.month
if month < 7:  # July rollover
    start = year - 1
    end   = year
else:
    start = year
    end   = year + 1

CURR_SEASON = f"{start}{end}"
LAST_SEASON = f"{start-1}{start}"

# ---------------------------- HELPERS ----------------------------
def blend_three_layer(recent, mid, last, w_recent=0.50, w_mid=0.35, w_last=0.15):
    """
    Blend 3 stat sources into one weighted average.
    Default: 50% recent, 35% season-to-date (or 30 days), 15% last season.
    """
    vals = [recent, mid, last]
    if all(pd.isna(v) for v in vals):
        return None
    num = 0.0; den = 0.0
    if pd.notna(recent): num += w_recent*recent; den += w_recent
    if pd.notna(mid):    num += w_mid*mid;       den += w_mid
    if pd.notna(last):   num += w_last*last;     den += w_last
    return num/den if den>0 else None

# ---------------------------- TEAM STATS ----------------------------
def get_team_stats():
    """Pull 2025–26 team defensive/offensive rates from NST."""
    url = f"https://www.naturalstattrick.com/teamtable.php?fromseason={CURR_SEASON}&thruseason={CURR_SEASON}&stype=2&sit=all"
    html = http_get_cached(url, tag="nst_teamtable", sleep=SETTINGS["sleep_nst"])
    if html is None:
        print("⚠️ NST team stats unavailable; using league fallbacks.")
        return pd.DataFrame(columns=["Team","CA/60","xGA/60","SF/60","xGF/60"])
    try:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.DOTALL|re.IGNORECASE)
        out = []
        for row in rows:
            m = re.search(r'teamreport\.php\?team=([A-Z]{2,3})', row)
            if not m: 
                continue
            abbr = m.group(1)
            def num(label):
                m2 = re.search(rf"{label}[^0-9]*([0-9]+\.[0-9]+)", row, flags=re.IGNORECASE)
                return float(m2.group(1)) if m2 else None
            out.append({
                "Team": abbr,
                "CA/60":  num("CA/60")  or FALLBACK_CA60,
                "xGA/60": num("xGA/60") or FALLBACK_xGA60,
                "SF/60":  num("SF/60")  or FALLBACK_SF60,
                "xGF/60": num("xGF/60") or FALLBACK_xGF60,
            })
        df = pd.DataFrame(out)
        df.to_csv(os.path.join(DATA_DIR, "team_stats.csv"), index=False)
        return df
    except Exception as e:
        print("❌ Parse NST team stats failed:", e)
        return pd.DataFrame(columns=["Team","CA/60","xGA/60","SF/60","xGF/60"])

# ---------------------------- GOALIES ----------------------------
def get_goalie_stats():
    """Pull 2025–26 goalie save % from NST."""
    url = f"https://www.naturalstattrick.com/playerteams.php?fromseason={CURR_SEASON}&thruseason={CURR_SEASON}&sit=all&playerstype=goalies"
    html = http_get_cached(url, tag="nst_goalies", sleep=SETTINGS["sleep_nst"])
    if html is None:
        return pd.DataFrame(columns=["PlayerRaw","NormName","SV%"])
    try:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
        out = []
        for row in rows:
            m_name = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
            if not m_name:
                continue
            pname = m_name.group(1).strip()
            sv_match = re.search(r'SV%[^0-9]*([0-9]*\.?[0-9]+)', row, flags=re.IGNORECASE)
            sv_pct = float(sv_match.group(1))/100.0 if sv_match else LEAGUE_AVG_SV
            out.append({
                "PlayerRaw": pname,
                "NormName": norm_name(pname),
                "SV%": sv_pct
            })
        df = pd.DataFrame(out)
        df.to_csv(os.path.join(DATA_DIR, "goalie_sv_table.csv"), index=False)
        return df
    except Exception as e:
        print("❌ Parse NST goalie stats failed:", e)
        return pd.DataFrame(columns=["PlayerRaw","NormName","SV%"])

# ---------------------------- SKATERS ----------------------------
def _parse_nst_player_rows(html):
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
    out = []
    for row in rows:
        m_name = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
        if not m_name:
            m_name = re.search(r'<td[^>]*>\s*([A-Za-z \'\-\.]+)\s*</td>', row)
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

        out.append({
            "PlayerRaw": pname, "NormName": norm_name(pname),
            "G/60": pick_num("G"), "A/60": pick_num("A"),
            "SOG/60": pick_num("(?:S|Shots)"), "BLK/60": pick_num("Blk"),
            "CF/60": pick_num("CF"), "CA/60": pick_num("CA"),
            "xGF/60": pick_num("xGF"), "xGA/60": pick_num("xGA"),
            "HDCF/60": pick_num("HDCF"), "HDCA/60": pick_num("HDCA")
        })
    return pd.DataFrame(out)

def _nst_players_for_team(team_code, from_season, thru_season, tgp=None):
    qs = f"team={team_code}&sit=all&fromseason={from_season}&thruseason={thru_season}"
    if tgp:
        qs += f"&tgp={tgp}"
    url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
    tag = f"nst_players_{team_code}_{from_season}_{thru_season}_{tgp or 'all'}"
    html = http_get_cached(url, tag=tag, sleep=SETTINGS['sleep_nst'])
    if html is None:
        return pd.DataFrame()
    return _parse_nst_player_rows(html)

def fetch_nst_player_stats_multi(teams):
    """
    For each team playing today, pull:
      - Last 10 GP
      - Last 30 days
      - Full 2025–26 season
      - Last season (for rookies/missing history)
    Then blend into weighted averages.
    """
    frames = []
    for abbr in teams:
        try_codes = [abbr] if abbr != "UTA" else ["UTA","ARI"]
        got_any = False
        for code in try_codes:
            season = _nst_players_for_team(code, CURR_SEASON, CURR_SEASON)
            recent = _nst_players_for_team(code, CURR_SEASON, CURR_SEASON, tgp=10)
            last30 = _nst_players_for_team(code, CURR_SEASON, CURR_SEASON, tgp=30)
            last   = _nst_players_for_team(code, LAST_SEASON, LAST_SEASON)

            if season.empty and code == "UTA":
                continue

            merged = season.merge(recent[["NormName","G/60","A/60","SOG/60","BLK/60"]],
                                  on="NormName", how="left", suffixes=("","_r"))
            merged = merged.merge(last30[["NormName","G/60","A/60","SOG/60","BLK/60"]],
                                  on="NormName", how="left", suffixes=("","_30"))
            merged = merged.merge(last[["NormName","G/60","A/60","SOG/60","BLK/60"]],
                                  on="NormName", how="left", suffixes=("","_l"))

            def b3(row, col):
                return blend_three_layer(row.get(col+"_r"),
                                         row.get(col+"_30"),
                                         row.get(col+"_l"))

            for col in ["G/60","A/60","SOG/60","BLK/60"]:
                merged[f"B_{col}"] = merged.apply(lambda r: b3(r, col), axis=1)

            merged["Team"] = abbr
            merged["PlayerKey"] = merged.apply(lambda r: f"{abbr}_{r['NormName']}", axis=1)
            frames.append(merged[["Team","PlayerRaw","NormName","PlayerKey",
                                  "B_G/60","B_A/60","B_SOG/60","B_BLK/60"]])
            got_any = True
            break
        if not got_any:
            print(f"⚠️ NST players missing for {abbr}; using fallbacks.")
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    out.to_csv(os.path.join(DATA_DIR, "nst_player_stats.csv"), index=False)
    return out
