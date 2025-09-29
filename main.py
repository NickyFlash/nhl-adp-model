# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script
Outputs: dfs_projections.csv, goalies.csv, top_stacks.csv (+ Excel + Google Sheets)
"""

import os
import pandas as pd
from datetime import date, datetime

# --- ADP NHL helpers ---
from adp_nhl.utils.etl import ingest_baseline_if_needed
from adp_nhl.utils.lineups_api import fetch_lineups
from adp_nhl.utils.joins import join_lineups_with_baseline, load_processed
from adp_nhl.utils.warnings import tag_missing_baseline, players_missing_baseline
from adp_nhl.utils import nst_scraper
from adp_nhl.utils.common import norm_name
from adp_nhl.utils.export_sheets import upload_to_sheets

# ---------------------------- CONFIG ----------------------------
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

SETTINGS = {
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3, "Save": 0.7, "GA": -3.5, "Win": 6},
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.2, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
}

# ---------------------------- DATE LOGIC ----------------------------
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

# ---------------------------- DRAFTKINGS ----------------------------
def load_dk_salaries():
    path = os.path.join(DATA_DIR, "dk_salaries.csv")
    if not os.path.exists(path):
        print("⚠️ dk_salaries.csv missing – running without salaries.")
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
    df["NormName"] = df["Name"].apply(norm_name)
    return df[["Name","TeamAbbrev","Position","Salary","NormName"]]

# ---------------------------- NHL SCHEDULE ----------------------------
def get_today_schedule():
    import requests
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
    return pd.DataFrame(games)

def build_opp_map(schedule_df: pd.DataFrame):
    opp = {}
    for _, g in schedule_df.iterrows():
        h, a = g["Home"], g["Away"]
        opp[h] = a
        opp[a] = h
    return opp

# ---------------------------- PROJECTIONS ----------------------------
def guess_role(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

def build_skaters(dk_df, nst_df, team_stats, opp_map):
    players = []
    source_df = dk_df if not dk_df.empty else nst_df.copy()
    for _, row in source_df.iterrows():
        nm   = row.get("NormName")
        team = row.get("TeamAbbrev") if "TeamAbbrev" in row else row.get("Team")
        pos  = row.get("Position", "F")
        opp  = opp_map.get(team)

        nst_row = nst_df[nst_df["PlayerRaw"] == row.get("PlayerRaw", nm)].head(1)
        if not nst_row.empty:
            # Actuals
            g60_actual = nst_row["G/60"].values[0]
            a60_actual = nst_row["A/60"].values[0]
            s60_actual = nst_row["SOG/60"].values[0]
            b60_actual = nst_row["BLK/60"].values[0]
            # Projections (blended)
            g60 = nst_row["B_G/60"].values[0]
            a60 = nst_row["B_A/60"].values[0]
            s60 = nst_row["B_SOG/60"].values[0]
            b60 = nst_row["B_BLK/60"].values[0]
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role=="D" else SETTINGS["F_fallback"]
            g60=a60=s60=b60 = fb["G60"],fb["A60"],fb["SOG60"],fb["BLK60"]
            g60_actual=a60_actual=s60_actual=b60_actual = g60,a60,s60,b60

        # Opponent adjustment
        sog_factor, xga_factor = 1.0, 1.0
        opp_stats = team_stats[team_stats.Team==opp]
        if not opp_stats.empty:
            sog_factor = opp_stats["SF/60"].values[0] / 31.0
            xga_factor = opp_stats["xGA/60"].values[0] / 2.65

        # Projections
        proj_goals  = g60 * xga_factor
        proj_assists= a60 * xga_factor
        proj_sog    = s60 * sog_factor
        proj_blocks = b60
        dk_points = (proj_goals*SETTINGS["DK_points"]["Goal"] +
                     proj_assists*SETTINGS["DK_points"]["Assist"] +
                     proj_sog*SETTINGS["DK_points"]["SOG"] +
                     proj_blocks*SETTINGS["DK_points"]["Block"])

        player_dict = {
            "Player": row.get("Name", nm), "Team": team, "Opponent": opp, "Position": pos,
            # --- Actuals ---
            "G/60_Actual": g60_actual, "A/60_Actual": a60_actual,
            "SOG/60_Actual": s60_actual, "BLK/60_Actual": b60_actual,
            # --- Projections ---
            "Proj Goals": proj_goals, "Proj Assists": proj_assists,
            "Proj SOG": proj_sog, "Proj Blocks": proj_blocks,
            "DK Points (Proj)": dk_points
        }
        if not dk_df.empty:
            player_dict["Salary"] = row["Salary"]
            player_dict["DFS Value Score (Proj)"] = dk_points/row["Salary"]*1000 if row["Salary"]>0 else 0
        players.append(player_dict)

    df = pd.DataFrame(players)
    df.to_csv(os.path.join(DATA_DIR,"dfs_projections.csv"), index=False)
    return df

def build_goalies(goalie_df, team_stats, opp_map):
    goalies = []
    for _, row in goalie_df.iterrows():
        team = row.get("Team","")
        if not team: continue
        opp = opp_map.get(team)

        sv_season = row.get("SV_season", 0.905)
        sv_recent = row.get("SV_recent")
        sv_last   = row.get("SV_last")
        sv_blend  = nst_scraper.blend_three_layer(sv_recent, sv_season, sv_last)

        opp_stats = team_stats[team_stats.Team==opp]
        opp_sf = opp_stats["SF/60"].values[0] if not opp_stats.empty else 31.0

        # Projections
        proj_saves = opp_sf * sv_blend
        proj_ga    = opp_sf * (1 - sv_blend)
        dk_points  = proj_saves*SETTINGS["DK_points"]["Save"] + proj_ga*SETTINGS["DK_points"]["GA"]

        goalies.append({
            "Goalie": row["PlayerRaw"], "Team": team, "Opponent": opp,
            # --- Actuals ---
            "SV%_Actual (Season)": sv_season,
            "SV%_Actual (Recent10)": sv_recent,
            "SV%_Actual (LastSeason)": sv_last,
            # --- Projections ---
            "Proj Saves": proj_saves, "Proj GA": proj_ga,
            "DK Points (Proj)": dk_points
        })
    df = pd.DataFrame(goalies)
    df.to_csv(os.path.join(DATA_DIR,"goalies.csv"), index=False)
    return df

def build_stacks(dfs_proj):
    stacks = []
    for team, grp in dfs_proj.groupby("Team"):
        pts = grp["DK Points (Proj)"].sum()
        stack_dict = {"Team": team, "ProjPts (Proj)": pts}
        if "Salary" in grp.columns:
            cost = grp["Salary"].sum()
            stack_dict["Cost"] = cost
            stack_dict["StackValue (Proj)"] = pts/cost*1000 if cost>0 else 0
        stacks.append(stack_dict)
    df = pd.DataFrame(stacks)
    df.to_csv(os.path.join(DATA_DIR,"top_stacks.csv"), index=False)
    return df

# ---------------------------- MAIN ----------------------------
def main():
    baseline_summary = ingest_baseline_if_needed()
    print("✅ Baseline summary:", baseline_summary)

    lineups = fetch_lineups()
    print("✅ Lineups status:", lineups.get("status"))

    _, _, _, _, players_df = load_processed()
    merged_lineups = join_lineups_with_baseline(lineups)
    merged_lineups = tag_missing_baseline(merged_lineups, players_df)
    missing_df = players_missing_baseline(merged_lineups)
    print("⚠️ Missing-baseline players:", len(missing_df))

    dk_df = load_dk_salaries()
    schedule_df = get_today_schedule()
    if schedule_df.empty: return
    opp_map = build_opp_map(schedule_df)

    team_stats = nst_scraper.get_team_stats(CURR_SEASON)
    nst_df     = nst_scraper.fetch_nst_player_stats_multi(schedule_df.Home.tolist()+schedule_df.Away.tolist(), CURR_SEASON, LAST_SEASON)
    goalie_df  = nst_scraper.get_goalies(CURR_SEASON, LAST_SEASON)

    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    # Excel Export
    output_path = os.path.join(DATA_DIR, f"projections_{datetime.today().strftime('%Y%m%d')}.xlsx")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        dfs_proj.to_excel(writer, sheet_name="Skaters", index=False)
        goalie_proj.to_excel(writer, sheet_name="Goalies", index=False)
        stack_proj.to_excel(writer, sheet_name="Stacks", index=False)
        team_stats.to_excel(writer, sheet_name="Teams", index=False)
        nst_df.to_excel(writer, sheet_name="NST_Raw", index=False)
    print(f"✅ Excel saved: {output_path}")

    # Google Sheets Export
    tabs = {"Skaters": dfs_proj, "Goalies": goalie_proj, "Stacks": stack_proj, "Teams": team_stats, "NST_Raw": nst_df}
    upload_to_sheets("ADP NHL Projections", tabs)
    print("✅ Uploaded to Google Sheets")

if __name__=="__main__":
    main()
