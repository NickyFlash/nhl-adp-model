# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script
Outputs: dfs_projections.csv, goalies.csv, top_stacks.csv (+ helper snapshots)
"""

import os, re, time, math, requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date

# --- ADP NHL baseline + lineups helpers ---
from adp_nhl.utils.etl import ingest_baseline_if_needed
from adp_nhl.utils.lineups_api import fetch_lineups
from adp_nhl.utils.joins import join_lineups_with_baseline

# Step 3: Join lineups with baseline stats
merged_lineups = join_lineups_with_baseline(lineups)
 print("✅ Merged lineups shape:", merged_lineups.shape)

# --- ADP NHL baseline + lineups helpers ---
from adp_nhl.utils.etl import ingest_baseline_if_needed
from adp_nhl.utils.lineups_api import fetch_lineups
from adp_nhl.utils.joins import join_lineups_with_baseline, load_processed
from adp_nhl.utils.warnings import tag_missing_baseline, players_missing_baseline

# ---------------------------- CONFIG ----------------------------
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (ADP Free Model)"}
TIMEOUT = 60

# Auto-detect current and last NHL season
today = date.today()
year = today.year
month = today.month

# NHL seasons run Oct–Jun, so "season year" rolls over in July
if month < 7:
    start = year - 1
    end   = year
else:
    start = year
    end   = year + 1

CURR_SEASON = f"{start}{end}"
LAST_SEASON = f"{start-1}{start}"

LEAGUE_AVG_SV = 0.905
GOALIE_TOI_MIN = 60.0

# League fallback averages
FALLBACK_SF60  = 31.0
FALLBACK_xGF60 = 2.95
FALLBACK_CA60  = 58.0
FALLBACK_xGA60 = 2.65

SETTINGS = {
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3},
    "LineMult_byType": {
        "5V5 LINE 1": 1.12, "5V5 LINE 2": 1.04, "5V5 LINE 3": 0.97, "5V5 LINE 4": 0.92,
        "D PAIR 1": 1.05, "D PAIR 2": 1.00, "D PAIR 3": 0.96,
        "PP1": 1.12, "PP2": 1.03, "PK1": 0.92, "PK2": 0.95
    },
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.2, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
    "sleep_nst": 3.0,
    "sleep_lines": 2.0
}

DFO_SLUG = {
    "ANA":"ducks","UTA":"utah-mammoth","BOS":"bruins","BUF":"sabres","CGY":"flames",
    "CAR":"hurricanes","CHI":"blackhawks","COL":"avalanche","CBJ":"blue-jackets","DAL":"stars",
    "DET":"red-wings","EDM":"oilers","FLA":"panthers","LAK":"kings","MIN":"wild",
    "MTL":"canadiens","NSH":"predators","NJD":"devils","NYI":"islanders","NYR":"rangers",
    "OTT":"senators","PHI":"flyers","PIT":"penguins","SJS":"sharks","SEA":"kraken",
    "STL":"blues","TBL":"lightning","TOR":"maple-leafs","VAN":"canucks","VGK":"golden-knights",
    "WSH":"capitals","WPG":"jets"
}

# ---------------------------- HELPERS ----------------------------
def norm_name(s: str) -> str:
    s = re.sub(r"[\u2013\u2014\u2019]", "-", str(s))
    s = re.sub(r"[^A-Za-z0-9\-\' ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().upper()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 2:
            s = f"{parts[1]} {parts[0]}".strip()
    return s

def http_get_cached(url, tag, sleep=2):
    """Fetch from cache if exists, else scrape and save. Retries if 429."""
    today = datetime.today().strftime("%Y%m%d")
    cache_file = os.path.join(RAW_DIR, f"{tag}_{today}.html")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return f.read()
    tries = 0
    while tries < 5:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 429:
                print("⚠️ Rate limited by NST/DFO. Sleeping 60s...")
                time.sleep(60)
                tries += 1
                continue
            r.raise_for_status()
            html = r.text
            with open(cache_file, "w", encoding="utf-8") as f:
                f.write(html)
            time.sleep(sleep)
            return html
        except Exception as e:
            print(f"❌ Fetch error {tag}:", e)
            time.sleep(10)
            tries += 1
    return None

def blend_three_layer(recent, season, last, w_recent=0.50, w_season=0.35, w_last=0.15):
    vals = [recent, season, last]
    if all(pd.isna(v) for v in vals):
        return None
    num = 0.0; den = 0.0
    if pd.notna(recent): num += w_recent*recent; den += w_recent
    if pd.notna(season): num += w_season*season; den += w_season
    if pd.notna(last):   num += w_last*last;     den += w_last
    return num/den if den>0 else None

def guess_role(pos: str) -> str:
    if not isinstance(pos,str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

# ---------------------------- DRAFTKINGS ----------------------------
def load_dk_salaries():
    path = os.path.join(DATA_DIR, "dk_salaries.csv")
    if not os.path.exists(path):
        print("❌ Missing dk_salaries.csv. Download from DraftKings and save to /data/")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        # Some DK exports can be semicolon-delimited
        df = pd.read_csv(path, sep=";")
    # Normalize columns
    ren = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "name": ren[c] = "Name"
        if lc == "teamabbrev": ren[c] = "TeamAbbrev"
        if lc == "position": ren[c] = "Position"
        if lc == "salary": ren[c] = "Salary"
    df = df.rename(columns=ren)
    required = ["Name","TeamAbbrev","Position","Salary"]
    for r in required:
        if r not in df.columns:
            print(f"❌ dk_salaries.csv missing column: {r}")
            return pd.DataFrame()
    df["NormName"] = df["Name"].apply(norm_name)
    return df[required + ["NormName"]]


# ---------------------------- NHL SCHEDULE ----------------------------
def get_today_schedule():
    url = "https://statsapi.web.nhl.com/api/v1/schedule"
    try:
        js = requests.get(url, timeout=30).json()
    except Exception as e:
        print("❌ Schedule fetch failed:", e)
        return pd.DataFrame()
    games = []
    for d in js.get("dates", []):
        for g in d.get("games", []):
            home = g["teams"]["home"]["team"].get("triCode") or g["teams"]["home"]["team"].get("abbreviation")
            away = g["teams"]["away"]["team"].get("triCode") or g["teams"]["away"]["team"].get("abbreviation")
            if home and away:
                games.append({"Home": home, "Away": away})
    df = pd.DataFrame(games)
    if df.empty:
        print("ℹ️ No NHL games on the schedule today.")
        return df
    df.to_csv(os.path.join(DATA_DIR, "schedule_today.csv"), index=False)
    return df

def build_opp_map(schedule_df: pd.DataFrame):
    opp = {}
    for _, g in schedule_df.iterrows():
        h, a = g["Home"], g["Away"]
        opp[h] = a
        opp[a] = h
    return opp


# ---------------------------- NST TEAM STATS (CACHED) ----------------------------
def get_team_stats():
    url = f"https://www.naturalstattrick.com/teamtable.php?fromseason={CURR_SEASON}&thruseason={CURR_SEASON}&stype=2&sit=all"
    html = http_get_cached(url, tag="nst_teamtable", sleep=SETTINGS["sleep_nst"])
    if html is None:
        print("⚠️ NST team stats unavailable; formulas will use league fallbacks.")
        return pd.DataFrame(columns=["Team","CA/60","xGA/60","SF/60","xGF/60"])
    try:
        # Parse manually to grab team codes + a few key rates reliably
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


# ---------------------------- NST PLAYER STATS (3-LAYER, CACHED) ----------------------------
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

        g60   = pick_num("G")
        a60   = pick_num("A")
        sog60 = pick_num("(?:S|Shots)")
        blk60 = pick_num("Blk")
        cf60  = pick_num("CF")
        ca60  = pick_num("CA")
        xgf60 = pick_num("xGF")
        xga60 = pick_num("xGA")
        hdcf60= pick_num("HDCF")
        hdca60= pick_num("HDCA")
        out.append({
            "PlayerRaw": pname, "NormName": norm_name(pname),
            "G/60": g60, "A/60": a60, "SOG/60": sog60, "BLK/60": blk60,
            "CF/60": cf60, "CA/60": ca60, "xGF/60": xgf60, "xGA/60": xga60,
            "HDCF/60": hdcf60, "HDCA/60": hdca60
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
      - Season blended per-60 (curr season + last season + recent 10 GP)
    Uses caching per-team to stay well under NST limits.
    """
    frames = []
    for abbr in teams:
        try_codes = [abbr] if abbr != "UTA" else ["UTA","ARI"]  # Utah may alias to ARI
        got_any = False
        for code in try_codes:
            season = _nst_players_for_team(code, CURR_SEASON, CURR_SEASON, tgp=None)
            if season.empty and code == "UTA":
                continue
            recent = _nst_players_for_team(code, CURR_SEASON, CURR_SEASON, tgp=10)
            last   = _nst_players_for_team(code, LAST_SEASON, LAST_SEASON, tgp=None)

            # blend
            merged = season.merge(recent[["NormName","G/60","A/60","SOG/60","BLK/60","CF/60","xGF/60","HDCF/60"]], 
                                  on="NormName", how="left", suffixes=("","_r"))
            merged = merged.merge(last[["NormName","G/60","A/60","SOG/60","BLK/60","CF/60","xGF/60","HDCF/60"]],
                                  on="NormName", how="left", suffixes=("","_l"))
            def b3(row, col):
                return blend_three_layer(row.get(col+"_r"), row.get(col), row.get(col+"_l"))
            for col in ["G/60","A/60","SOG/60","BLK/60","CF/60","xGF/60","HDCF/60"]:
                merged[f"B_{col}"] = merged.apply(lambda r: b3(r, col), axis=1)

            merged["Team"] = abbr
            merged["PlayerKey"] = merged.apply(lambda r: f"{abbr}_{r['NormName']}", axis=1)
            frames.append(merged[["Team","PlayerRaw","NormName","PlayerKey",
                                  "B_G/60","B_A/60","B_SOG/60","B_BLK/60","B_CF/60","B_xGF/60","B_HDCF/60"]])
            got_any = True
            break
        if not got_any:
            print(f"⚠️ NST players missing for {abbr}; will use fallbacks for that team.")
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    out.to_csv(os.path.join(DATA_DIR, "nst_player_stats.csv"), index=False)
    return out

# ---------------------------- DAILY FACEOFF LINES (CACHED) ----------------------------
def get_team_lines(team_abbrev):
    slug = DFO_SLUG.get(team_abbrev)
    if not slug:
        return pd.DataFrame()
    url = f"https://www.dailyfaceoff.com/teams/{slug}/line-combinations/"
    html = http_get_cached(url, tag=f"dfo_{team_abbrev}", sleep=SETTINGS["sleep_lines"])
    if html is None:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "lxml")
    lines = []
    for sec in soup.find_all("section"):
        title = sec.find("h2")
        if not title:
            continue
        group = title.get_text(strip=True).upper()
        table = sec.find("table")
        if not table:
            continue
        for tr in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cols) < 1:
                continue
            player = norm_name(cols[0])
            lines.append({
                "Team": team_abbrev,
                "NormName": player,
                "Assignment": group
            })
    return pd.DataFrame(lines)

def get_all_lines(schedule_df):
    all_lines = []
    for team in pd.unique(schedule_df[["Home","Away"]].values.ravel()):
        print(f"📋 Fetching lines for {team}...")
        df = get_team_lines(team)
        if not df.empty:
            all_lines.append(df)
    if all_lines:
        merged = pd.concat(all_lines, ignore_index=True)
        merged.to_csv(os.path.join(DATA_DIR, "line_context.csv"), index=False)
        return merged
    return pd.DataFrame()


# ---------------------------- GOALIE STATS (CACHED) ----------------------------
def get_goalie_stats():
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

# ---------------------------- BUILD PROJECTIONS ----------------------------
def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    players = []
    for _, row in dk_df.iterrows():
        nm   = row["NormName"]
        team = row["TeamAbbrev"]
        pos  = row["Position"]
        opp  = opp_map.get(team)

        # Player stats
        nst_row = nst_df[nst_df["NormName"]==nm].head(1)
        if not nst_row.empty:
            g60 = nst_row["B_G/60"].values[0]
            a60 = nst_row["B_A/60"].values[0]
            s60 = nst_row["B_SOG/60"].values[0]
            b60 = nst_row["B_BLK/60"].values[0]
        else:
            # fallback by role
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role=="D" else SETTINGS["F_fallback"]
            g60,a60,s60,b60 = fb["G60"],fb["A60"],fb["SOG60"],fb["BLK60"]

        # Opponent adjustment
        opp_stats = team_stats[team_stats.Team==opp]
        sog_factor = (opp_stats["SF/60"].values[0] / FALLBACK_SF60) if not opp_stats.empty else 1.0
        xga_factor = (opp_stats["xGA/60"].values[0] / FALLBACK_xGA60) if not opp_stats.empty else 1.0

        # Line context
        line_row = lines_df[(lines_df.NormName==nm)&(lines_df.Team==team)]
        line_info = line_row["Assignment"].iloc[0] if not line_row.empty else "NA"
        line_mult = SETTINGS["LineMult_byType"].get(line_info,1.0)

        # Projections
        proj_goals  = g60 * xga_factor * line_mult
        proj_assists= a60 * xga_factor * line_mult
        proj_sog    = s60 * sog_factor * line_mult
        proj_blocks = b60 * line_mult

        dk_points = (proj_goals*SETTINGS["DK_points"]["Goal"] +
                     proj_assists*SETTINGS["DK_points"]["Assist"] +
                     proj_sog*SETTINGS["DK_points"]["SOG"] +
                     proj_blocks*SETTINGS["DK_points"]["Block"])
        value = dk_points / row["Salary"]*1000 if row["Salary"]>0 else 0

        players.append({
            "Player": row["Name"], "Team": team, "Opponent": opp, "Position": pos,
            "Line": line_info, "Salary": row["Salary"],
            "Proj Goals": proj_goals, "Proj Assists": proj_assists,
            "Proj SOG": proj_sog, "Proj Blocks": proj_blocks,
            "DK Points": dk_points, "DFS Value Score": value
        })
    df = pd.DataFrame(players)
    df.to_csv(os.path.join(DATA_DIR,"dfs_projections.csv"), index=False)
    return df


def build_goalies(goalie_df, team_stats, opp_map):
    goalies = []
    for _, row in goalie_df.iterrows():
        team = row.get("Team","")
        if not team: 
            continue
        opp = opp_map.get(team)
        sv_pct = row.get("SV%", LEAGUE_AVG_SV)
        opp_stats = team_stats[team_stats.Team==opp]
        opp_sf = opp_stats["SF/60"].values[0] if not opp_stats.empty else FALLBACK_SF60
        proj_saves = opp_sf * sv_pct
        proj_ga    = opp_sf * (1-sv_pct)
        goalies.append({
            "Goalie": row["PlayerRaw"], "Team": team, "Opponent": opp,
            "Proj Saves": proj_saves, "Proj GA": proj_ga
        })
    df = pd.DataFrame(goalies)
    df.to_csv(os.path.join(DATA_DIR,"goalies.csv"), index=False)
    return df


def build_stacks(dfs_proj):
    stacks = []
    for team, grp in dfs_proj.groupby("Team"):
        for line, players in grp.groupby("Line"):
            if line=="NA": continue
            cost = players["Salary"].sum()
            pts  = players["DK Points"].sum()
            val  = pts/cost*1000 if cost>0 else 0
            stacks.append({
                "Team": team, "Line": line,
                "Players": ", ".join(players.Player),
                "Cost": cost, "ProjPts": pts, "StackValue": val
            })
    df = pd.DataFrame(stacks)
    df.to_csv(os.path.join(DATA_DIR,"top_stacks.csv"), index=False)
    return df


# ---------------------------- MAIN ----------------------------
def main():
 # Step 1: Ingest baseline (2024–25) if not already done
    baseline_summary = ingest_baseline_if_needed()
    print("✅ Baseline summary:", baseline_summary)

    # Step 2: Fetch today's lineups (2025–26) via API (ETag-aware)
    lineups = fetch_lineups()
    print("✅ Lineups status:", lineups.get("status"), "Teams:", lineups.get("count"))

    # Step 3: Join lineups with baseline skater stats
    merged_lineups = join_lineups_with_baseline(lineups)
    print("✅ Merged lineups shape:", getattr(merged_lineups, "shape", None))

    # Step 4: Load baseline players parquet and tag rookies/missing-history players
    _, _, _, _, players_df = load_processed()
    merged_lineups = tag_missing_baseline(merged_lineups, players_df)
    missing_df = players_missing_baseline(merged_lineups)
    print("⚠️ Missing-baseline players:", len(missing_df))
    # Optional: preview a few
    try:
        print(missing_df.head(15).to_string(index=False))
    except Exception:
        pass

    print("🚀 Starting ADP NHL DFS Model")

    # Step 1: Ingest baseline (2024–25 stats) if not already done
    baseline_summary = ingest_baseline_if_needed()
    print("✅ Baseline summary:", baseline_summary)

    # Step 2: Fetch today’s lineups (2025–26, API-driven)
    lineups = fetch_lineups()
    print("✅ Lineups status:", lineups["status"], "Teams:", lineups.get("count"))

    dk_df = load_dk_salaries()
    if dk_df.empty: 
        return

    schedule_df = get_today_schedule()
    if schedule_df.empty:
        return
    opp_map = build_opp_map(schedule_df)

    team_stats = get_team_stats()
    nst_df = fetch_nst_player_stats_multi(pd.unique(schedule_df[["Home","Away"]].values.ravel()))
    lines_df = get_all_lines(schedule_df)
    goalie_df = get_goalie_stats()

    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

if __name__=="__main__":
    main()
