# -*- coding: utf-8 -*-
"""
NaturalStatTrick scraper utilities
- Pulls team stats, player skater totals (season & recent), goalie splits, and line-combo metrics
- Uses a simple file cache under data/raw to avoid repeated live hits in CI
- Exposes helpers to compute /60 rates and to merge NST line combos with DFO line assignments
"""

import os
import time
import requests
import pandas as pd
import re
from io import StringIO
from datetime import datetime
from typing import Optional
from adp_nhl.utils.common import norm_name

# ---- Config ----
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(RAW_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (ADP Free Model)"}
TIMEOUT = 30
DEFAULT_SLEEP = 2.0

# Fallbacks
FALLBACK_CA60  = 58.0
FALLBACK_xGA60 = 2.65
FALLBACK_SF60  = 31.0
FALLBACK_xGF60 = 2.95
LEAGUE_AVG_SV = 0.905

# ---- Internal helpers ----
def _cache_path(tag: str):
    today = datetime.today().strftime("%Y%m%d")
    return os.path.join(RAW_DIR, f"{tag}_{today}.html")

def http_get_cached(url: str, tag: str, sleep: float = DEFAULT_SLEEP):
    """
    GET with simple day-based cache. Returns HTML string or None.
    """
    path = _cache_path(tag)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass

    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        html = r.text
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass
        time.sleep(sleep)
        return html
    except Exception as e:
        print(f"❌ Fetch error {tag}: {e}")
        return None

def _safe_read_html(html: str):
    """
    Wrapper around pd.read_html that uses StringIO to satisfy future warnings.
    Returns list of dataframes or empty list.
    """
    try:
        return pd.read_html(StringIO(html))
    except Exception:
        return []

# ---- Team stats ----
def get_team_stats(season: str) -> pd.DataFrame:
    """
    Pull NST team table for the season.
    season: e.g. "20242025" or "20232024"
    Returns DataFrame with columns: Team, CF/60, CA/60, SF/60, xGF/60, xGA/60
    """
    url = f"https://www.naturalstattrick.com/teamtable.php?fromseason={season}&thruseason={season}&stype=2&sit=all"
    tag = f"nst_teamtable_{season}"
    html = http_get_cached(url, tag=tag)
    if not html:
        print("⚠️ NST team stats unavailable; returning empty DataFrame with expected columns.")
        return pd.DataFrame(columns=["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"])

    # Try to read table with pandas first. If it fails, fallback to regex parsing.
    tables = _safe_read_html(html)
    if tables:
        # find likely table by column names
        for t in tables:
            cols = [c.lower() for c in t.columns.astype(str)]
            if any("cf/60" in c for c in cols) or any("cf" == c for c in cols):
                df = t.copy()
                # normalize columns to the expected names if possible
                rename_map = {}
                lower_map = {c.lower(): c for c in df.columns}
                def pick(col_options):
                    for opt in col_options:
                        if opt in lower_map:
                            return lower_map[opt]
                    return None
                # options we may encounter
                col_cf = pick(["cf/60","cf/60:","cf/60.0","cf","cf60"])
                col_ca = pick(["ca/60","ca","ca60"])
                col_sf = pick(["sf/60","sf","sf60","sfp/60"])
                col_xgf = pick(["xgf/60","xgf","xgf60","xgf/60.0"])
                col_xga = pick(["xga/60","xga","xga60"])
                if col_cf: rename_map[col_cf] = "CF/60"
                if col_ca: rename_map[col_ca] = "CA/60"
                if col_sf: rename_map[col_sf] = "SF/60"
                if col_xgf: rename_map[col_xgf] = "xGF/60"
                if col_xga: rename_map[col_xga] = "xGA/60"
                # Team column
                team_col = pick(["team","tm","team."])
                if team_col:
                    rename_map[team_col] = "Team"
                df = df.rename(columns=rename_map)
                needed = ["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"]
                # if some missing, add defaults
                for c in needed:
                    if c not in df.columns:
                        df[c] = pd.NA
                df = df[needed]
                # coerce numeric
                for c in ["CF/60","CA/60","SF/60","xGF/60","xGA/60"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                # fill fallback values where applicable
                df["CA/60"] = df["CA/60"].fillna(FALLBACK_CA60)
                df["SF/60"] = df["SF/60"].fillna(FALLBACK_SF60)
                df["xGF/60"] = df["xGF/60"].fillna(FALLBACK_xGF60)
                df["xGA/60"] = df["xGA/60"].fillna(FALLBACK_xGA60)
                df.to_csv(os.path.join(DATA_DIR, "team_stats.csv"), index=False)
                return df.reset_index(drop=True)

    # fallback regex parsing
    try:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.DOTALL|re.IGNORECASE)
        out = []
        for row in rows:
            m = re.search(r'teamreport\.php\?team=([A-Z]{2,3})', row)
            if not m:
                continue
            abbr = m.group(1)
            def num(label, fallback=None):
                m2 = re.search(rf"{label}[^0-9\-]*([0-9]*\.?[0-9]+)", row, flags=re.IGNORECASE)
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
        if df.empty:
            raise ValueError("no team rows parsed")
        df.to_csv(os.path.join(DATA_DIR, "team_stats.csv"), index=False)
        return df
    except Exception as e:
        print("❌ Parse NST team stats failed:", e)
        return pd.DataFrame(columns=["Team","CF/60","CA/60","SF/60","xGF/60","xGA/60"])

# ---- Skater player totals ----
def _parse_player_table_df(df: pd.DataFrame, team_code: Optional[str]=None) -> pd.DataFrame:
    """
    Given a dataframe read from NST playerteams table, map/rename to expected columns.
    Returns DataFrame with PlayerRaw, NormName, G/60, A/60, SOG/60, BLK/60, CF/60, xGF/60, HDCF/60
    """
    df = df.copy()
    # Merge columns heuristically
    cols = {c.lower(): c for c in df.columns}
    # find name column
    name_col = None
    for opt in ["player","player/team","player/team link","player name","player.name"]:
        if opt in cols:
            name_col = cols[opt]; break
    # sometimes the player column contains HTML anchor; coerce to string and strip
    if name_col is None:
        # try first column
        name_col = df.columns[0]
    # numeric column picks
    def pick(*opts):
        for o in opts:
            lo = o.lower()
            if lo in cols:
                return cols[lo]
        return None
    g_col = pick("g/60","g/60.","g60","g / 60","g")
    a_col = pick("a/60","a60","a")
    s_col = pick("s/60","s60","s","sog/60","sog")
    blk_col = pick("blk/60","blk","blks","blk")
    cf_col = pick("cf/60","cf")
    xgf_col = pick("xgf/60","xgf")
    hdcf_col = pick("hdcf/60","hdcf")
    out = []
    for _, r in df.iterrows():
        raw_name = str(r.get(name_col, "")).strip()
        # strip HTML tags if present
        raw_name = re.sub(r"<[^>]+>", "", raw_name)
        nm = raw_name
        # build row
        def val(c):
            return float(r[c]) if (c is not None and pd.notna(r.get(c))) else pd.NA
        row = {
            "PlayerRaw": nm,
            "NormName": norm_name(nm),
            "G/60": val(g_col),
            "A/60": val(a_col),
            "SOG/60": val(s_col),
            "BLK/60": val(blk_col),
            "CF/60": val(cf_col),
            "xGF/60": val(xgf_col),
            "HDCF/60": val(hdcf_col)
        }
        if team_code:
            row["Team"] = team_code
        out.append(row)
    return pd.DataFrame(out)

def get_team_players(team_code: str, season_code: str, tgp: Optional[int]=None) -> pd.DataFrame:
    """
    Pull NST playerteams page for a given team and season.
    If tgp is provided (e.g., 10) pulls that split.
    Returns DataFrame of parsed player totals with NormName.
    """
    qs = f"team={team_code}&sit=all&fromseason={season_code}&thruseason={season_code}"
    if tgp:
        qs += f"&tgp={tgp}"
    url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
    tag = f"nst_players_{team_code}_{season_code}_{tgp or 'all'}"
    html = http_get_cached(url, tag=tag, sleep=DEFAULT_SLEEP)
    if not html:
        return pd.DataFrame()
    tables = _safe_read_html(html)
    if not tables:
        # fallback to row regex parsing similar to earlier approaches
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
                        try:
                            return float(m.group(1))
                        except:
                            pass
                return pd.NA
            out.append({
                "PlayerRaw": pname,
                "NormName": norm_name(pname),
                "G/60": pick_num("G"),
                "A/60": pick_num("A"),
                "SOG/60": pick_num("S"),
                "BLK/60": pick_num("Blk"),
                "CF/60": pick_num("CF"),
                "xGF/60": pick_num("xGF"),
                "HDCF/60": pick_num("HDCF"),
                "Team": team_code
            })
        return pd.DataFrame(out)
    # Usually the first table or one of the tables contains players
    # Try to find the one containing "Player" or "Player" in header
    chosen = None
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if any("player" in c for c in cols):
            chosen = t
            break
    if chosen is None:
        chosen = tables[0]
    parsed = _parse_player_table_df(chosen, team_code=team_code)
    # ensure numeric columns exist
    for c in ["G/60","A/60","SOG/60","BLK/60","CF/60","xGF/60","HDCF/60"]:
        if c not in parsed.columns:
            parsed[c] = pd.NA
    parsed.to_csv(os.path.join(DATA_DIR, f"nst_players_{team_code}_{season_code}_{tgp or 'all'}.csv"), index=False)
    return parsed

# ---- Line combos (NST lines page) ----
def get_line_combos(team_code: str, season_code: str) -> pd.DataFrame:
    """
    Scrape the NST line combos page for a team.
    Returns combos with player names and totals (TOI, CF, CA, xGF, xGA, SF, SCF, HDCF, etc.)
    """
    url = f"https://www.naturalstattrick.com/line_combos.php?team={team_code}&season={season_code}&stype=2"
    tag = f"nst_line_combos_{team_code}_{season_code}"
    html = http_get_cached(url, tag=tag, sleep=DEFAULT_SLEEP)
    if not html:
        return pd.DataFrame()
    tables = _safe_read_html(html)
    if not tables:
        # fallback parse rows
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
        out = []
        for row in rows:
            # attempt to capture three skaters in cells
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.IGNORECASE|re.DOTALL)
            if len(cells) < 5:
                continue
            # best-effort: first cell(s) are players separated by breaks
            players_html = cells[0]
            players = re.findall(r'>([^<]+)</a>', players_html)
            players = [p.strip() for p in players if p.strip()]
            players = players[:3]
            def num_from_cells(idx):
                try:
                    txt = re.sub(r"<[^>]+>", "", cells[idx])
                    return float(re.sub(r"[^\d\.\-]","", txt)) if txt.strip() else pd.NA
                except Exception:
                    return pd.NA
            toi = num_from_cells(1)
            cf = num_from_cells(2)
            ca = num_from_cells(3)
            xgf = num_from_cells(4)
            xga = num_from_cells(5) if len(cells) > 5 else pd.NA
            out.append({
                "Team": team_code,
                "Players": players,
                "P1": players[0] if len(players)>0 else pd.NA,
                "P2": players[1] if len(players)>1 else pd.NA,
                "P3": players[2] if len(players)>2 else pd.NA,
                "TOI": toi,
                "CF": cf,
                "CA": ca,
                "xGF": xgf,
                "xGA": xga
            })
        df = pd.DataFrame(out)
        df.to_csv(os.path.join(DATA_DIR, f"nst_line_combos_{team_code}_{season_code}.csv"), index=False)
        return df

    # Usually the first table is combos; try to find
    chosen = None
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if any("players" in c or "combination" in c or "line" in c for c in cols):
            chosen = t
            break
    if chosen is None:
        chosen = tables[0]
    # normalize chosen to expected columns
    df = chosen.copy()
    # Identify player column (may contain anchors)
    player_col = None
    for c in df.columns:
        if isinstance(c, str) and ("player" in c.lower() or "comb" in c.lower() or "combination" in c.lower()):
            player_col = c
            break
    if player_col is None:
        player_col = df.columns[0]
    # extract player names
    players_list = []
    for cell in df[player_col].astype(str):
        names = re.findall(r'>([^<]+)</a>', cell)
        if not names:
            # fallback split on newline or /
            parts = re.split(r"\s*[/\n,]\s*", re.sub(r"<[^>]+>","", cell))
            names = [p.strip() for p in parts if p.strip()][:3]
        players_list.append(names)
    df["Players"] = players_list
    df["P1"] = df["Players"].apply(lambda x: x[0] if isinstance(x, list) and len(x)>0 else pd.NA)
    df["P2"] = df["Players"].apply(lambda x: x[1] if isinstance(x, list) and len(x)>1 else pd.NA)
    df["P3"] = df["Players"].apply(lambda x: x[2] if isinstance(x, list) and len(x)>2 else pd.NA)

    # coerce numeric columns heuristically
    for cand in df.columns:
        low = str(cand).lower()
        if any(k in low for k in ["toi","time on ice","minutes"]):
            df = df.rename(columns={cand: "TOI"})
        if any(k in low for k in ["cf","corsi for"]):
            df = df.rename(columns={cand: "CF"})
        if any(k in low for k in ["ca","corsi against"]):
            df = df.rename(columns={cand: "CA"})
        if any(k in low for k in ["xgf","expected goals for"]):
            df = df.rename(columns={cand: "xGF"})
        if any(k in low for k in ["xga","expected goals against"]):
            df = df.rename(columns={cand: "xGA"})
        if any(k in low for k in ["sf","shots for"]):
            df = df.rename(columns={cand: "SF"})
        if any(k in low for k in ["scf","scf","shots for corsi"]):
            df = df.rename(columns={cand: "SCF"})
        if any(k in low for k in ["hdcf","high danger chances for"]):
            df = df.rename(columns={cand: "HDCF"})

    # ensure columns and numeric coercion
    for k in ["TOI","CF","CA","xGF","xGA","SF","SCF","HDCF"]:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce")
        else:
            df[k] = pd.NA

    df["Team"] = team_code
    df.to_csv(os.path.join(DATA_DIR, f"nst_line_combos_{team_code}_{season_code}.csv"), index=False)
    return df

# ---- Goalies ----
def get_goalies(season: str, last_season: Optional[str]=None) -> pd.DataFrame:
    """
    Pull goalie lists (season, recent tgp=10, last season) and return merged DataFrame:
    NormName, PlayerRaw, SV_season, SV_recent, SV_last
    """
    if not last_season:
        last_season = str(int(season[:4]) - 1) + str(int(season[:4]))

    def fetch_goalie_stats(season_q: str, tgp: Optional[int]=None):
        qs = f"fromseason={season_q}&thruseason={season_q}&sit=all&playerstype=goalies"
        if tgp:
            qs += f"&tgp={tgp}"
        url = f"https://www.naturalstattrick.com/playerteams.php?{qs}"
        tag = f"nst_goalies_{season_q}_{tgp or 'all'}"
        html = http_get_cached(url, tag=tag, sleep=DEFAULT_SLEEP)
        if not html:
            return pd.DataFrame()
        tables = _safe_read_html(html)
        out = []
        if tables:
            # find table with SV%
            chosen = None
            for t in tables:
                cols = [str(c).lower() for c in t.columns]
                if any("sv%" in c.lower() or "sv%" == c for c in cols):
                    chosen = t
                    break
            if chosen is None:
                chosen = tables[0]
            # locate columns
            colmap = {c.lower(): c for c in chosen.columns}
            sv_col = None
            for k in colmap:
                if "sv" in k and "%" in k:
                    sv_col = colmap[k]; break
            # name column
            name_col = None
            for k in colmap:
                if "player" in k:
                    name_col = colmap[k]; break
            if name_col is None:
                name_col = chosen.columns[0]
            for _, r in chosen.iterrows():
                raw = str(r.get(name_col, "")).strip()
                raw = re.sub(r"<[^>]+>", "", raw)
                sv_val = None
                if sv_col:
                    try:
                        txt = str(r.get(sv_col, ""))
                        sv_val = float(re.sub(r"[^\d\.]","", txt)) / 100.0 if txt and txt.strip() else pd.NA
                    except Exception:
                        sv_val = pd.NA
                out.append({"PlayerRaw": raw, "NormName": norm_name(raw), "SV%": sv_val})
            return pd.DataFrame(out)
        # fallback regex parse
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
        for row in rows:
            m_name = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
            if not m_name:
                continue
            pname = m_name.group(1).strip()
            sv_match = re.search(r'SV%[^0-9]*([0-9]*\.?[0-9]+)', row, flags=re.IGNORECASE)
            sv_pct = float(sv_match.group(1))/100.0 if sv_match else pd.NA
            out.append({"PlayerRaw": pname, "NormName": norm_name(pname), "SV%": sv_pct})
        return pd.DataFrame(out)

    season_df = fetch_goalie_stats(season).rename(columns={"SV%":"SV_season"})
    recent_df = fetch_goalie_stats(season, tgp=10).rename(columns={"SV%":"SV_recent"})
    last_df   = fetch_goalie_stats(last_season).rename(columns={"SV%":"SV_last"})

    # guard against empty dfs
    if season_df.empty and recent_df.empty and last_df.empty:
        return pd.DataFrame()

    merged = season_df.copy()
    if not recent_df.empty and "NormName" in recent_df.columns and "SV_recent" in recent_df.columns:
        merged = merged.merge(recent_df[["NormName","SV_recent"]], on="NormName", how="left")
    else:
        merged["SV_recent"] = pd.NA

    if not last_df.empty and "NormName" in last_df.columns and "SV_last" in last_df.columns:
        merged = merged.merge(last_df[["NormName","SV_last"]], on="NormName", how="left")
    else:
        merged["SV_last"] = pd.NA

    # ensure SV_season exists
    if "SV_season" not in merged.columns:
        merged["SV_season"] = pd.NA

    merged.to_csv(os.path.join(DATA_DIR, "nst_goalies_merged.csv"), index=False)
    return merged

# ---- Utilities ----
def compute_per60(df: pd.DataFrame, toi_col: str = "TOI") -> pd.DataFrame:
    """
    Given a DataFrame with totals (CF, xGF, SF, etc.) and TOI in minutes,
    compute per-60 rates. Returns df with columns appended: CF/60, xGF/60, SF/60, etc.
    """
    df = df.copy()
    if toi_col not in df.columns:
        print(f"⚠️ compute_per60: {toi_col} not in df; skipping per60 computation.")
        return df
    # ToI in minutes expected. If TOI is in seconds, caller must convert.
    def per60(col):
        return (df[col] / df[toi_col] * 60).replace([pd.NA, pd.NaT], pd.NA)
    for col in ["CF","xGF","SF","SCF","HDCF","CA","xGA"]:
        if col in df.columns:
            try:
                df[f"{col}/60"] = per60(col)
            except Exception:
                df[f"{col}/60"] = pd.NA
        else:
            df[f"{col}/60"] = pd.NA
    return df

def merge_dfo_lines_with_nst(dfo_lines_df: pd.DataFrame, nst_line_df: pd.DataFrame) -> pd.DataFrame:
    """
    Match DFO-assigned lines (team, line label, P1/P2/P3 names normalized) to NST combos.
    nst_line_df expected to contain P1, P2, P3 (raw names) and TOI/CF/xGF etc.
    Returns a merged DataFrame where each DFO line has NST combo metrics attached where matched.
    Matching strategy: compare sets of normalized names.
    """
    if dfo_lines_df is None or dfo_lines_df.empty:
        return pd.DataFrame()
    if nst_line_df is None or nst_line_df.empty:
        # return dfo lines with empty NST cols
        out = dfo_lines_df.copy()
        for c in ["TOI","CF","CA","xGF","xGA","SF","SCF","HDCF"]:
            out[c] = pd.NA
        return out

    # Normalize names in both frames
    dfo = dfo_lines_df.copy()
    nst = nst_line_df.copy()
    # ensure P1/P2/P3 exist in NST (they may be columns or inside Players list)
    if "P1" not in nst.columns and "Players" in nst.columns:
        nst["P1"] = nst["Players"].apply(lambda pl: pl[0] if isinstance(pl, (list,tuple)) and len(pl)>0 else pd.NA)
        nst["P2"] = nst["Players"].apply(lambda pl: pl[1] if isinstance(pl, (list,tuple)) and len(pl)>1 else pd.NA)
        nst["P3"] = nst["Players"].apply(lambda pl: pl[2] if isinstance(pl, (list,tuple)) and len(pl)>2 else pd.NA)

    # normalized sets
    def trio_set(row, cols):
        names = []
        for c in cols:
            v = row.get(c, pd.NA)
            if pd.isna(v):
                continue
            names.append(norm_name(str(v)))
        return frozenset(names)

    nst["__trio"] = nst.apply(lambda r: trio_set(r, ["P1","P2","P3"]), axis=1)
    dfo["__trio"] = dfo.apply(lambda r: trio_set(r, ["Player1","Player2","Player3"]), axis=1)

    # Build lookup: team -> trio -> nst row index
    lookup = {}
    for idx, r in nst.iterrows():
        t = r.get("Team", None)
        trio = r.get("__trio", frozenset())
        if t not in lookup:
            lookup[t] = {}
        if trio not in lookup[t]:
            lookup[t][trio] = idx

    merged_rows = []
    for _, r in dfo.iterrows():
        team = r.get("Team")
        trio = r.get("__trio", frozenset())
        match_idx = None
        if team in lookup and trio in lookup[team]:
            match_idx = lookup[team][trio]
        # fallback: try matching ignoring team (rare)
        if match_idx is None:
            for tmap in lookup.values():
                if trio in tmap:
                    match_idx = tmap[trio]; break
        nst_row = nst.loc[match_idx] if match_idx is not None else pd.Series()
        merged = r.to_dict()
        # attach NST metrics
        for c in ["TOI","CF","CA","xGF","xGA","SF","SCF","HDCF"]:
            merged[c] = nst_row.get(c, pd.NA) if not nst_row.empty else pd.NA
        merged_rows.append(merged)

    outdf = pd.DataFrame(merged_rows)
    # compute per60 rates for convenience
    outdf = compute_per60(outdf, toi_col="TOI")
    outdf.to_csv(os.path.join(DATA_DIR, "dfo_nst_lines_merged.csv"), index=False)
    return outdf

# ---- Convenience bulk functions ----
def fetch_all_teams_players(team_list, season_code):
    """
    Helper to fetch full-season and recent (tgp=10) players for all teams in team_list.
    Returns two DataFrames: players_season, players_recent (concatenated).
    """
    season_list = []
    recent_list = []
    for t in team_list:
        try:
            s = get_team_players(t, season_code)
            if not s.empty:
                season_list.append(s)
            r = get_team_players(t, season_code, tgp=10)
            if not r.empty:
                recent_list.append(r)
        except Exception as e:
            print(f"⚠️ Error fetching players for {t}: {e}")
    season_df = pd.concat(season_list, ignore_index=True) if season_list else pd.DataFrame()
    recent_df = pd.concat(recent_list, ignore_index=True) if recent_list else pd.DataFrame()
    return season_df, recent_df

def fetch_all_line_combos(team_list, season_code):
    """
    Helper to fetch line combos for every team and return concatenated DataFrame.
    """
    combos = []
    for t in team_list:
        try:
            df = get_line_combos(t, season_code)
            if not df.empty:
                combos.append(df)
        except Exception as e:
            print(f"⚠️ Error fetching line combos for {t}: {e}")
    return pd.concat(combos, ignore_index=True) if combos else pd.DataFrame()
