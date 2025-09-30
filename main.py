# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script
Outputs: dfs_projections.csv, goalies.csv, top_stacks.csv (+ GSheet export)
"""

import os, pandas as pd
from datetime import date, datetime

# --- ADP NHL utils ---
from adp_nhl.utils.etl import ingest_baseline_if_needed
from adp_nhl.utils.lineups_api import fetch_lineups
from adp_nhl.utils.joins import join_lineups_with_baseline, load_processed
from adp_nhl.utils.warnings import tag_missing_baseline, players_missing_baseline
from adp_nhl.utils.common import norm_name
from adp_nhl.utils import nst_scraper
from adp_nhl.utils.export_sheets import upload_to_sheets

# ---------------------------- CONFIG ----------------------------
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

SETTINGS = {
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3},
    "LineMult_byType": {
        "5V5 LINE 1": 1.12, "5V5 LINE 2": 1.04, "5V5 LINE 3": 0.97, "5V5 LINE 4": 0.92,
        "D PAIR 1": 1.05, "D PAIR 2": 1.00, "D PAIR 3": 0.96,
        "PP1": 1.12, "PP2": 1.03, "PK1": 0.92, "PK2": 0.95
    },
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.2, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
}

FALLBACK_SF60  = 31.0
FALLBACK_xGA60 = 2.65
LEAGUE_AVG_SV  = 0.905

# ---------------------------- HELPERS ----------------------------
def guess_role(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

# ---------------------------- PROJECTIONS ----------------------------
def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    players = []
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
            g60 = nst_row.get("G/60", row.get("G/60", SETTINGS["F_fallback"]["G60"]))
            a60 = nst_row.get("A/60", row.get("A/60", SETTINGS["F_fallback"]["A60"]))
            s60 = nst_row.get("SOG/60", row.get("SOG/60", SETTINGS["F_fallback"]["SOG60"]))
            b60 = nst_row.get("BLK/60", row.get("BLK/60", SETTINGS["F_fallback"]["BLK60"]))
            g60,a60,s60,b60 = g60.values[0],a60.values[0],s60.values[0],b60.values[0]
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role=="D" else SETTINGS["F_fallback"]
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


def build_goalies(goalie_df, team_stats, opp_map):
    goalies = []
    for _, row in goalie_df.iterrows():
        team = row.get("Team", "")
        if not team: continue
        opp = opp_map.get(team)
        sv_pct = row.get("SV_season", LEAGUE_AVG_SV)
        opp_stats = team_stats[team_stats.Team == opp]
        opp_sf = opp_stats["SF/60"].values[0] if not opp_stats.empty else FALLBACK_SF60

        proj_saves = opp_sf * sv_pct
        proj_ga    = opp_sf * (1 - sv_pct)

        goalie_dict = {
            "Goalie": row.get("PlayerRaw", row.get("NormName")),
            "Team": team,
            "Opponent": opp,
            "Proj Saves": proj_saves,
            "Proj GA": proj_ga,
            "DK Points": proj_saves*0.7 - proj_ga*3.5  # basic DK scoring
        }

        goalies.append(goalie_dict)

    df = pd.DataFrame(goalies)
    df.to_csv(os.path.join(DATA_DIR, "goalies.csv"), index=False)
    return df


def build_stacks(dfs_proj):
    stacks = []
    for team, grp in dfs_proj.groupby("Team"):
        for line, players in grp.groupby("Line"):
            if line == "NA": continue
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
    # Baseline (2024–25)
    baseline_summary = ingest_baseline_if_needed()
    print("✅ Baseline summary:", baseline_summary)

    # Lineups (2025–26)
    lineups = fetch_lineups()
    print("✅ Lineups status:", lineups.get("status"), "Teams:", lineups.get("count"))

    merged_lineups = join_lineups_with_baseline(lineups)
    _, _, _, _, players_df = load_processed()
    merged_lineups = tag_missing_baseline(merged_lineups, players_df)
    missing_df = players_missing_baseline(merged_lineups)
    print("⚠️ Missing-baseline players:", len(missing_df))

    # NHL schedule
    from adp_nhl.utils.common import build_opp_map, get_today_schedule
    schedule_df = get_today_schedule()
    if schedule_df.empty: return
    opp_map = build_opp_map(schedule_df)

    # NST stats
    team_stats = nst_scraper.get_team_stats(baseline_summary["season"])
    nst_players = []
    for team in pd.unique(schedule_df[["Home","Away"]].values.ravel()):
        nst_players.append(nst_scraper.get_team_players(team, baseline_summary["season"], tgp=10))
        nst_players.append(nst_scraper.get_team_players(team, baseline_summary["season"]))
    nst_df = pd.concat(nst_players, ignore_index=True)
    goalie_df = nst_scraper.get_goalies(baseline_summary["season"])

    # Build outputs
    dfs_proj = build_skaters(pd.DataFrame(), nst_df, team_stats, merged_lineups, opp_map)
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

    # Export to Google Sheets
    print("📤 Uploading projections to Google Sheets...")
    tabs = {
        "Skaters": dfs_proj,
        "Goalies": goalie_proj,
        "Stacks": stack_proj,
        "Teams": team_stats,
        "NST_Raw": nst_df
    }
    upload_to_sheets("ADP NHL Projections", tabs)

if __name__=="__main__":
    main()
