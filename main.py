# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script
Outputs: dfs_projections.csv, goalies.csv, top_stacks.csv (+ helper snapshots, GSheet export)
"""

import os, requests
import pandas as pd
from datetime import date, datetime

# --- ADP NHL baseline + lineups helpers ---
from adp_nhl.utils.etl import ingest_baseline_if_needed
from adp_nhl.utils.lineups_api import fetch_lineups
from adp_nhl.utils.joins import join_lineups_with_baseline, load_processed
from adp_nhl.utils.warnings import tag_missing_baseline, players_missing_baseline
from adp_nhl.utils.common import norm_name
from adp_nhl.utils import nst_scraper
from adp_nhl.utils.export_sheets import upload_to_sheets

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

if month < 7:  # NHL seasons run Oct–Jun, rollover in July
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

# ---------------------------- HELPERS ----------------------------
def guess_role(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

def get_today_schedule(max_retries=3):
    today_str = date.today().strftime("%Y-%m-%d")
    endpoints = [
        f"https://statsapi.web.nhl.com/api/v1/schedule?date={today_str}",
        f"https://api-web.nhle.com/v1/schedule?date={today_str}",
        f"https://sports.algobook.info/api/v1/nhl/schedule/{today_str}"
    ]
    for endpoint in endpoints:
        tries = 0
        while tries < max_retries:
            try:
                resp = requests.get(endpoint, timeout=30)
                resp.raise_for_status()
                js = resp.json()
                games = []
                # parse depending on structure
                if "dates" in js:
                    for d in js.get("dates", []):
                        for g in d.get("games", []):
                            home = g["teams"]["home"]["team"].get("triCode")
                            away = g["teams"]["away"]["team"].get("triCode")
                            if home and away:
                                games.append({"Home": home, "Away": away})
                elif "gameWeeks" in js:
                    # Algobook structure fallback
                    for w in js["gameWeeks"]:
                        for g in w.get("games", []):
                            home = g.get("homeTeam", {}).get("abbrev")
                            away = g.get("awayTeam", {}).get("abbrev")
                            if home and away:
                                games.append({"Home": home, "Away": away})
                # else structure may vary — you can inspect
                df = pd.DataFrame(games)
                if not df.empty:
                    df.to_csv(os.path.join(DATA_DIR, "schedule_today.csv"), index=False)
                    return df
                break  # no games, but endpoint responded
            except Exception as e:
                print(f"⚠️ Schedule endpoint {endpoint} attempt {tries+1} failed: {e}")
                tries += 1
                time.sleep(3)
        # try next endpoint
    print("❌ All schedule endpoints failed or no games found.")
    return pd.DataFrame()

def build_opp_map(schedule_df: pd.DataFrame):
    opp = {}
    for _, g in schedule_df.iterrows():
        h, a = g["Home"], g["Away"]
        opp[h] = a
        opp[a] = h
    return opp

# ---------------------------- DRAFTKINGS ----------------------------
def load_dk_salaries():
    path = os.path.join(DATA_DIR, "dk_salaries.csv")
    if not os.path.exists(path):
        print("❌ Missing dk_salaries.csv. Download from DraftKings and save to /data/")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.read_csv(path, sep=";")
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

# ---------------------------- BUILD PROJECTIONS ----------------------------
def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    players = []

    # If no salaries available, fallback to using NST players instead
    source_df = dk_df if not dk_df.empty else nst_df.copy()
    if dk_df.empty:
        print("⚠️ DK salaries not found, building projections without salaries...")

    for _, row in source_df.iterrows():
        nm   = row.get("NormName")
        team = row.get("TeamAbbrev") if "TeamAbbrev" in row else row.get("Team")
        pos  = row.get("Position", "F")
        opp  = opp_map.get(team)

        # Player stats
        nst_row = nst_df[nst_df["NormName"] == nm].head(1)
        if not nst_row.empty:
            g60 = nst_row["G/60"].values[0]
            a60 = nst_row["A/60"].values[0]
            s60 = nst_row["SOG/60"].values[0]
            b60 = nst_row["BLK/60"].values[0]
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role == "D" else SETTINGS["F_fallback"]
            g60,a60,s60,b60 = fb["G60"],fb["A60"],fb["SOG60"],fb["BLK60"]

        # Opponent adjustment
        opp_stats = team_stats[team_stats.Team == opp]
        sog_factor = (opp_stats["SF/60"].values[0] / FALLBACK_SF60) if not opp_stats.empty else 1.0
        xga_factor = (opp_stats["xGA/60"].values[0] / FALLBACK_xGA60) if not opp_stats.empty else 1.0

        # Line context
        line_row = lines_df[(lines_df.NormName == nm) & (lines_df.Team == team)]
        line_info = line_row["Assignment"].iloc[0] if not line_row.empty else "NA"
        line_mult = SETTINGS["LineMult_byType"].get(line_info, 1.0)

        # Projections
        proj_goals  = g60 * xga_factor * line_mult
        proj_assists= a60 * xga_factor * line_mult
        proj_sog    = s60 * sog_factor * line_mult
        proj_blocks = b60 * line_mult

        dk_points = (proj_goals*SETTINGS["DK_points"]["Goal"] +
                     proj_assists*SETTINGS["DK_points"]["Assist"] +
                     proj_sog*SETTINGS["DK_points"]["SOG"] +
                     proj_blocks*SETTINGS["DK_points"]["Block"])

        player_dict = {
            "Player": row.get("Name", nm), "Team": team, "Opponent": opp, "Position": pos,
            "Line": line_info,
            "Proj Goals": proj_goals, "Proj Assists": proj_assists,
            "Proj SOG": proj_sog, "Proj Blocks": proj_blocks,
            "DK Points": dk_points
        }

        # Only add Salary + Value if salaries exist
        if not dk_df.empty:
            player_dict["Salary"] = row["Salary"]
            player_dict["DFS Value Score"] = dk_points / row["Salary"] * 1000 if row["Salary"] > 0 else 0

        players.append(player_dict)

    df = pd.DataFrame(players)
    df.to_csv(os.path.join(DATA_DIR, "dfs_projections.csv"), index=False)
    return df


def build_goalies(goalie_df, team_stats, opp_map):
    goalies = []
    if goalie_df.empty:
        print("⚠️ No goalie stats found, skipping goalie projections...")
        return pd.DataFrame()

    for _, row in goalie_df.iterrows():
        team = row.get("Team", "")
        if not team:
            continue
        opp = opp_map.get(team)
        sv_pct = row.get("SV_season", LEAGUE_AVG_SV)
        opp_stats = team_stats[team_stats.Team == opp]
        opp_sf = opp_stats["SF/60"].values[0] if not opp_stats.empty else FALLBACK_SF60

        proj_saves = opp_sf * sv_pct
        proj_ga    = opp_sf * (1 - sv_pct)

        dk_points = proj_saves*0.7 - proj_ga*3.5  # DK scoring approx for goalies

        goalie_dict = {
            "Goalie": row.get("PlayerRaw", row.get("NormName")),
            "Team": team,
            "Opponent": opp,
            "Proj Saves": proj_saves,
            "Proj GA": proj_ga,
            "DK Points": dk_points
        }

        goalies.append(goalie_dict)

    df = pd.DataFrame(goalies)
    df.to_csv(os.path.join(DATA_DIR, "goalies.csv"), index=False)
    return df


def build_stacks(dfs_proj):
    stacks = []
    if dfs_proj.empty:
        print("⚠️ No skater projections available, skipping stacks...")
        return pd.DataFrame()

    for team, grp in dfs_proj.groupby("Team"):
        for line, players in grp.groupby("Line"):
            if line == "NA": 
                continue
            pts = players["DK Points"].sum()

            stack_dict = {
                "Team": team,
                "Line": line,
                "Players": ", ".join(players.Player),
                "ProjPts": pts
            }

            if "Salary" in players.columns:
                cost = players["Salary"].sum()
                val  = pts / cost * 1000 if cost > 0 else 0
                stack_dict["Cost"] = cost
                stack_dict["StackValue"] = val

            stacks.append(stack_dict)

    df = pd.DataFrame(stacks)
    df.to_csv(os.path.join(DATA_DIR, "top_stacks.csv"), index=False)
    return df

# ---------------------------- MAIN ----------------------------
def main():
    # Step 1: Ingest baseline (2024–25 stats) if not already done
    baseline_summary = ingest_baseline_if_needed()
    print("✅ Baseline summary:", baseline_summary)

    # Step 2: Fetch today’s lineups (2025–26, API-driven)
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
    try:
        print(missing_df.head(15).to_string(index=False))
    except Exception:
        pass

    print("🚀 Starting ADP NHL DFS Model")

    # Step 5: Load DK salaries (optional)
    dk_df = load_dk_salaries()
    if dk_df.empty:
        print("⚠️ No DraftKings salaries found, continuing without salary/value context.")

    # Step 6: Get today’s NHL schedule
    schedule_df = get_today_schedule()
    if schedule_df.empty:
        print("ℹ️ No games today, skipping projections.")
        return
    opp_map = build_opp_map(schedule_df)

    # Step 7: Fetch NST stats
    print("📊 Fetching NST team stats...")
    team_stats = nst_scraper.get_team_stats(CURR_SEASON)

    print("📊 Fetching NST skater stats...")
    nst_players = []
    for team in pd.unique(schedule_df[["Home","Away"]].values.ravel()):
        # Last 10 games
        nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON, tgp=10))
        # Season-to-date
        nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON))
    nst_df = pd.concat(nst_players, ignore_index=True) if nst_players else pd.DataFrame()

    print("📊 Fetching NST goalie stats...")
    goalie_df = nst_scraper.get_goalies(CURR_SEASON, LAST_SEASON)

    print("📊 Fetching line assignments...")
    lines_df = get_all_lines(schedule_df)

    # Step 8: Build projections
    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

    # Step 9: Export to Google Sheets
    from adp_nhl.utils.export_sheets import upload_to_sheets
    print("📤 Uploading projections to Google Sheets...")
    tabs = {
        "Skaters": dfs_proj,
        "Goalies": goalie_proj,
        "Stacks": stack_proj,
        "Teams": team_stats,
        "NST_Raw": nst_df
    }
    upload_to_sheets("ADP NHL Projections", tabs)  # <-- replace with your exact sheet name

if __name__=="__main__":
    main()
