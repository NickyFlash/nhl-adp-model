# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script
Outputs: dfs_projections.csv, goalies.csv, top_stacks.csv (+ helper snapshots, Excel, Google Sheets)
"""

import os, requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, datetime   # ✅ need both date + datetime

# --- ADP NHL baseline + lineups helpers ---
from adp_nhl.utils.etl import ingest_baseline_if_needed
from adp_nhl.utils.lineups_api import fetch_lineups
from adp_nhl.utils.joins import join_lineups_with_baseline, load_processed
from adp_nhl.utils.warnings import tag_missing_baseline, players_missing_baseline
from adp_nhl.utils.common import norm_name, http_get_cached
from adp_nhl.utils import nst_scraper   # ✅ NST scraper helpers

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

def guess_role(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

# ---------------------------- DRAFTKINGS ----------------------------
def load_dk_salaries():
    path = os.path.join(DATA_DIR, "dk_salaries.csv")
    if not os.path.exists(path):
        print("⚠️ Missing dk_salaries.csv. Building projections without salaries...")
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
            print(f"⚠️ dk_salaries.csv missing column: {r}")
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
            home = g["teams"]["home"]["team"].get("triCode")
            away = g["teams"]["away"]["team"].get("triCode")
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

def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    players = []
    source_df = dk_df if not dk_df.empty else nst_df.copy()

    for _, row in source_df.iterrows():
        nm   = row.get("NormName")
        team = row.get("TeamAbbrev") if "TeamAbbrev" in row else row.get("Team")
        pos  = row.get("Position", "F")
        opp  = opp_map.get(team)

        # Player stats
        nst_row = nst_df[nst_df["NormName"] == nm].head(1)
        if not nst_row.empty:
            g60 = nst_row["B_G/60"].values[0]
            a60 = nst_row["B_A/60"].values[0]
            s60 = nst_row["B_SOG/60"].values[0]
            b60 = nst_row["B_BLK/60"].values[0]
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

        if not dk_df.empty:
            player_dict["Salary"] = row["Salary"]
            player_dict["DFS Value Score"] = dk_points / row["Salary"] * 1000 if row["Salary"] > 0 else 0

        players.append(player_dict)

    df = pd.DataFrame(players)
    df.to_csv(os.path.join(DATA_DIR, "dfs_projections.csv"), index=False)
    return df


def build_goalies(goalie_df, team_stats, opp_map, dk_df):
    if goalie_df.empty:
        print("⚠️ No goalie stats found, skipping goalie projections...")
        return pd.DataFrame()

    goalie_df["Opponent"] = goalie_df["Team"].map(opp_map)
    goalie_df = goalie_df.merge(
        team_stats[["Team","SF/60"]].rename(columns={"Team":"Opponent","SF/60":"Opp_SF60"}),
        on="Opponent", how="left"
    )
    goalie_df["Opp_SF60"] = goalie_df["Opp_SF60"].fillna(FALLBACK_SF60)

    # --- blended SV% for projections ---
    def blend_sv(row):
        num, den = 0.0, 0.0
        if pd.notna(row.get("SV_recent")): num += 0.50 * row["SV_recent"]; den += 0.50
        if pd.notna(row.get("SV_season")): num += 0.35 * row["SV_season"]; den += 0.35
        if pd.notna(row.get("SV_last")):   num += 0.15 * row["SV_last"];   den += 0.15
        return num/den if den > 0 else LEAGUE_AVG_SV

    goalie_df["SV_blend"] = goalie_df.apply(blend_sv, axis=1)

    # Projections
    goalie_df["Proj Saves"] = goalie_df["Opp_SF60"] * goalie_df["SV_blend"]
    goalie_df["Proj GA"]    = goalie_df["Opp_SF60"] * (1.0 - goalie_df["SV_blend"])
    goalie_df["DK Points"]  = goalie_df["Proj Saves"]*0.7 + goalie_df["Proj GA"]*(-3.5)

    # Merge DK salaries if available
    if not dk_df.empty:
        dk_goalies = dk_df[dk_df["Position"].str.contains("G")][["NormName","Salary"]]
        goalie_df = goalie_df.merge(dk_goalies, on="NormName", how="left")

    out = goalie_df.rename(columns={"NormName":"Goalie"})[
        ["Goalie","Team","Opponent","Salary","SV_season","Proj Saves","Proj GA","DK Points"]
    ]
    out.to_csv(os.path.join(DATA_DIR, "goalies.csv"), index=False)
    return out

def build_stacks(dfs_proj):
    stacks = []
    if dfs_proj.empty:
        return pd.DataFrame()
    for team, grp in dfs_proj.groupby("Team"):
        for line, players in grp.groupby("Line"):
            if line == "NA": continue
            pts = players["DK Points"].sum()
            stack_dict = {"Team": team, "Line": line, "Players": ", ".join(players.Player), "ProjPts": pts}
            if "Salary" in players.columns:
                cost = players["Salary"].sum()
                stack_dict["Cost"] = cost
                stack_dict["StackValue"] = pts / cost * 1000 if cost > 0 else 0
            stacks.append(stack_dict)
    df = pd.DataFrame(stacks)
    df.to_csv(os.path.join(DATA_DIR, "top_stacks.csv"), index=False)
    return df

def main():
    baseline_summary = ingest_baseline_if_needed()
    print("✅ Baseline summary:", baseline_summary)

    lineups = fetch_lineups()
    print("✅ Lineups status:", lineups.get("status"), "Teams:", lineups.get("count"))

    merged_lineups = join_lineups_with_baseline(lineups)
    print("✅ Merged lineups shape:", getattr(merged_lineups, "shape", None))

    _, _, _, _, players_df = load_processed()
    merged_lineups = tag_missing_baseline(merged_lineups, players_df)
    missing_df = players_missing_baseline(merged_lineups)
    print("⚠️ Missing-baseline players:", len(missing_df))

    print("🚀 Starting ADP NHL DFS Model")

    dk_df = load_dk_salaries()
    schedule_df = get_today_schedule()
    if schedule_df.empty: return
    opp_map = build_opp_map(schedule_df)

    print("📊 Fetching NST team stats...")
    team_stats = nst_scraper.get_team_stats(CURR_SEASON)
    print("📊 Fetching NST skater stats...")
    nst_players = []
    for team in pd.unique(schedule_df[["Home","Away"]].values.ravel()):
        nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON, tgp=10))
        nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON))
    nst_df = pd.concat(nst_players, ignore_index=True)
    print("📊 Fetching NST goalie stats...")
    goalie_df = nst_scraper.get_goalies(CURR_SEASON)

    print("📊 Fetching line assignments...")
    lines_df = nst_scraper.get_all_lines(schedule_df)

    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map, merged_lineups)
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

    # --- EXPORT TO EXCEL ---
    print("📊 Exporting results to Excel...")
    output_path = os.path.join(DATA_DIR, f"projections_{datetime.today().strftime('%Y%m%d')}.xlsx")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        dfs_proj.to_excel(writer, sheet_name="Skaters", index=False)
        goalie_proj.to_excel(writer, sheet_name="Goalies", index=False)
        stack_proj.to_excel(writer, sheet_name="Stacks", index=False)
        team_stats.to_excel(writer, sheet_name="Teams", index=False)
        nst_df.to_excel(writer, sheet_name="NST_Raw", index=False)
    print(f"✅ Excel workbook ready: {output_path}")

    # --- EXPORT TO GOOGLE SHEETS ---
    from adp_nhl.utils.export_sheets import upload_to_sheets
    print("📤 Uploading projections to Google Sheets...")
    tabs = {"Skaters": dfs_proj,"Goalies": goalie_proj,"Stacks": stack_proj,"Teams": team_stats,"NST_Raw": nst_df}
    upload_to_sheets("ADP NHL Projections", tabs)

if __name__=="__main__":
    main()
