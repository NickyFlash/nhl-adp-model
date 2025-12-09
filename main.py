# main.py
# One-file NHL DFS projections script with NST + DK + lines + Excel export.

import os
import time
from datetime import datetime

import pandas as pd
import requests

# -------------------------------------------------------------------
# GLOBAL SETTINGS / CONSTANTS
# -------------------------------------------------------------------

DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)
if not os.path.exists(RAW_DIR):
    os.makedirs(RAW_DIR, exist_ok=True)

CURR_SEASON = "2024-2025"

FALLBACK_SF60 = 30.0
FALLBACK_xGA60 = 2.8
LEAGUE_AVG_SV = 0.905

SETTINGS = {
    "D_fallback": {
        "G60": 0.2,
        "A60": 0.8,
        "SOG60": 1.5,
        "BLK60": 1.5,
    },
    "F_fallback": {
        "G60": 0.6,
        "A60": 0.6,
        "SOG60": 2.8,
        "BLK60": 0.4,
    },
    "LineMult_byType": {
        "5V5 LINE 1": 1.15,
        "5V5 LINE 2": 1.05,
        "5V5 LINE 3": 0.95,
        "5V5 LINE 4": 0.85,
        "PP1": 1.25,
        "PP2": 1.05,
        "PK1": 0.9,
        "PK2": 0.85,
        "NA": 1.0,
    },
}

# -------------------------------------------------------------------
# PROJECT-SPECIFIC PLACEHOLDER STUBS
# -------------------------------------------------------------------
# These are "fake" functions so the file runs.
# Later, you or a dev can replace them with your real versions.

class NSTScraperStub(object):
    def get_team_stats(self, season):
        # Return empty DF by default; replace with real NST team stats.
        return pd.DataFrame()

    def get_team_players(self, team, season, tgp=None):
        # Return empty DF by default; replace with real NST player stats.
        return pd.DataFrame()

    def get_goalies(self, season):
        # Return empty DF by default; replace with real NST goalie stats.
        return pd.DataFrame()

nst_scraper = NSTScraperStub()

def ingest_baseline_if_needed():
    # Replace with your real baseline ingestion code.
    return {"status": "stub", "players": 0}

def fetch_lineups():
    # Replace with your real lineup fetcher.
    return {"status": "stub", "count": 0}

def join_lineups_with_baseline(lineups):
    # Replace with your real join logic.
    return pd.DataFrame()

def load_processed():
    # Replace with your real processed data loader.
    # Expected to return (teams_df, goalies_df, baseline_df, lineups_df, players_df)
    return None, None, None, None, pd.DataFrame()

def tag_missing_baseline(merged_lineups, players_df):
    # Replace with your real tagging logic.
    return merged_lineups

def players_missing_baseline(merged_lineups):
    # Replace with your real report logic.
    return pd.DataFrame()

def load_dk_salaries():
    # Replace this stub with your real DK salary loader that returns a DataFrame.
    # Must have at minimum: Name or Player, Salary (numeric), Team, Position.
    # We will create NormName inside the projection code.
    return pd.DataFrame()

def upload_to_sheets(sheet_name, tabs_dict):
    # Replace with your real Google Sheets upload logic if you use it.
    print("Sheets upload stub called; no data actually uploaded for sheet:", sheet_name)

# -------------------------------------------------------------------
# NORMALIZATION & SMALL HELPERS
# -------------------------------------------------------------------

def norm_name(raw):
    """Normalize player names to a consistent format for joining."""
    if raw is None:
        return ""
    s = str(raw).strip().lower()
    s = s.replace(".", "").replace(",", "")
    s = " ".join(s.split())
    return s

def guess_role(pos):
    """Very simple guess: treat D separately, everything else as F."""
    if pos is None:
        return "F"
    p = str(pos).upper()
    if "D" in p and "F" not in p:
        return "D"
    return "F"

def safe_get_from_series(s, keys):
    """Try multiple column names, return first numeric value that works."""
    for k in keys:
        if k in s.index and pd.notna(s[k]):
            try:
                return float(s[k])
            except Exception:
                try:
                    return float(str(s[k]).replace(",", ""))
                except Exception:
                    continue
    return None

# -------------------------------------------------------------------
# SCHEDULE / OPPONENT MAP
# -------------------------------------------------------------------

def get_today_schedule():
    """
    Fetch today's NHL schedule.
    This is a stubbed version that just tries to read a local CSV if present.
    Replace this with your real schedule fetch.
    """
    path = os.path.join(DATA_DIR, "schedule_today.csv")
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if "Home" in df.columns and "Away" in df.columns:
                return df
        except Exception as e:
            print("⚠️ Failed to read local schedule_today.csv:", e)

    print("ℹ️ Using empty schedule; no schedule_today.csv found or invalid.")
    return pd.DataFrame(columns=["Home", "Away"])

def build_opp_map(schedule_df):
    """
    Build team -> opponent map from schedule_df with Home/Away columns.
    """
    opp_map = {}
    if schedule_df is None or schedule_df.empty:
        return opp_map
    for _, row in schedule_df.iterrows():
        home = row.get("Home")
        away = row.get("Away")
        if pd.isna(home) or pd.isna(away):
            continue
        opp_map[str(home)] = str(away)
        opp_map[str(away)] = str(home)
    return opp_map

# -------------------------------------------------------------------
# NST / TEAM / GOALIE NORMALIZATION
# -------------------------------------------------------------------

def normalize_nst_team_stats(team_stats_raw):
    """Rename NST team columns to consistent /60 names."""
    if team_stats_raw is None or team_stats_raw.empty:
        return pd.DataFrame(columns=["Team", "SF/60", "xGA/60"])

    df = team_stats_raw.copy()

    col_ren = {}
    for c in df.columns:
        lc = str(c).lower()
        if "sf60" == lc or "sf/60" == lc:
            col_ren[c] = "SF/60"
        if "xga60" == lc or "xga/60" == lc:
            col_ren[c] = "xGA/60"
        if c.lower() == "team":
            col_ren[c] = "Team"

    df = df.rename(columns=col_ren)

    if "Team" not in df.columns:
        if "team" in df.columns:
            df = df.rename(columns={"team": "Team"})
        else:
            df["Team"] = ""

    if "SF/60" not in df.columns:
        df["SF/60"] = FALLBACK_SF60

    if "xGA/60" not in df.columns:
        df["xGA/60"] = FALLBACK_xGA60

    return df

def normalize_nst_skaters(nst_df_raw):
    """Ensure NST skater stats have NormName + flexible scoring columns."""
    if nst_df_raw is None or nst_df_raw.empty:
        return pd.DataFrame(columns=["NormName"])

    df = nst_df_raw.copy()

    name_col = None
    for c in df.columns:
        lc = str(c).lower()
        if lc in ["normname", "normalizedplayername"]:
            name_col = c
            break
    if name_col is not None:
        df["NormName"] = df[name_col].astype(str).apply(norm_name)
    else:
        for candidate in ["Player", "Player Name", "Name"]:
            if candidate in df.columns:
                name_col = candidate
                break
        if name_col is not None:
            df["NormName"] = df[name_col].astype(str).apply(norm_name)
        else:
            df["NormName"] = ""

    df["NormName"] = df["NormName"].fillna("").astype(str)

    return df

def normalize_goalie_stats(goalie_df_raw):
    """Ensure goalie stats have NormName and SV% column."""
    if goalie_df_raw is None or goalie_df_raw.empty:
        return pd.DataFrame(columns=["NormName", "SV%"])

    df = goalie_df_raw.copy()

    name_col = None
    for c in df.columns:
        lc = str(c).lower()
        if lc in ["normname", "normalizedplayername"]:
            name_col = c
            break
    if name_col is None:
        for candidate in ["Player", "Goalie", "Name", "GoalieName"]:
            if candidate in df.columns:
                name_col = candidate
                break

    if name_col is not None:
        df["NormName"] = df[name_col].astype(str).apply(norm_name)
    else:
        df["NormName"] = ""

    sv_col = None
    for c in df.columns:
        lc = str(c).lower()
        if "sv" in lc and "%" in lc:
            sv_col = c
            break
    if sv_col is None:
        for c in df.columns:
            if str(c).lower() in ["sv", "svpct", "sv%"]:
                sv_col = c
                break

    if sv_col is None:
        df["SV%"] = LEAGUE_AVG_SV
    else:
        df["SV%"] = pd.to_numeric(df[sv_col], errors="coerce").fillna(LEAGUE_AVG_SV)

    return df[["NormName", "SV%"]].copy()

# -------------------------------------------------------------------
# LINES / LINE ASSIGNMENTS
# -------------------------------------------------------------------

def get_all_lines(schedule_df):
    """
    Stub for line assignments. Replace with your real DFO / lines fetch.
    Must return DF with NormName and Assignment (like '5V5 LINE 1', 'PP1').
    """
    return pd.DataFrame(columns=["NormName", "Assignment"])

# -------------------------------------------------------------------
# PROJECTION BUILDERS
# -------------------------------------------------------------------

def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    """Build skater projections for all DK rows."""
    if dk_df is None or dk_df.empty:
        print("⚠️ DK salaries empty in build_skaters; returning empty DF.")
        return pd.DataFrame()

    skaters = dk_df.copy()

    if "NormName" not in skaters.columns:
        name_source = None
        for c in ["Name", "Player", "Player Name"]:
            if c in skaters.columns:
                name_source = c
                break
        if name_source is None:
            skaters["NormName"] = ""
        else:
            skaters["NormName"] = skaters[name_source].astype(str).apply(norm_name)

    if "Team" not in skaters.columns:
        for c in ["TeamAbbrev", "Team_Name", "TeamName"]:
            if c in skaters.columns:
                skaters["Team"] = skaters[c]
                break
    if "Team" not in skaters.columns:
        skaters["Team"] = ""

    if "Position" not in skaters.columns and "Pos" in skaters.columns:
        skaters["Position"] = skaters["Pos"]
    if "Position" not in skaters.columns:
        skaters["Position"] = ""

    if "Salary" not in skaters.columns:
        skaters["Salary"] = 0

    team_stats = normalize_nst_team_stats(team_stats)

    if lines_df is None or lines_df.empty:
        lines_df = pd.DataFrame(columns=["NormName", "Assignment"])
    if "NormName" not in lines_df.columns:
        if "Player" in lines_df.columns:
            lines_df["NormName"] = lines_df["Player"].astype(str).apply(norm_name)
        else:
            lines_df["NormName"] = ""
    if "Assignment" not in lines_df.columns:
        lines_df["Assignment"] = "NA"

    proj_rows = []

    for _, row in skaters.iterrows():
        nm = row.get("NormName", "")
        pos = row.get("Position", "")
        team = row.get("Team", "")
        sal = row.get("Salary", 0)
        name = row.get("Name", row.get("Player", ""))

        nst_match = nst_df[nst_df["NormName"] == nm]
        if not nst_match.empty:
            s = nst_match.iloc[0]
            g60 = safe_get_from_series(s, ["G/60", "G60", "G"])
            a60 = safe_get_from_series(s, ["A/60", "A60", "A"])
            s60 = safe_get_from_series(s, ["SOG/60", "S/60", "SOG", "S"])
            b60 = safe_get_from_series(s, ["BLK/60", "BLK", "BLK/60"])
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role == "D" else SETTINGS["F_fallback"]
            g60 = fb["G60"]
            a60 = fb["A60"]
            s60 = fb["SOG60"]
            b60 = fb["BLK60"]

        toi = 18.0
        team_row = team_stats[team_stats["Team"] == team]
        if not team_row.empty:
            try:
                sf60 = float(team_row["SF/60"].values[0])
                toi = 18.0 * (sf60 / FALLBACK_SF60)
            except Exception:
                pass

        exp_goals = g60 * toi / 60.0
        exp_assists = a60 * toi / 60.0
        exp_sog = s60 * toi / 60.0
        exp_blk = b60 * toi / 60.0

        line_info = "NA"
        ld = lines_df[lines_df["NormName"] == nm]
        if not ld.empty:
            line_info = ld["Assignment"].iloc[0]
        line_mult = SETTINGS["LineMult_byType"].get(line_info, 1.0)

        exp_goals *= line_mult
        exp_assists *= line_mult
        exp_sog *= line_mult
        exp_blk *= line_mult

        dk_points = (
            exp_goals * 8.5
            + exp_assists * 5.0
            + exp_sog * 1.5
            + exp_blk * 1.3
        )

        val = 0.0
        try:
            sal_f = float(sal)
            if sal_f > 0:
                val = dk_points / (sal_f / 1000.0)
        except Exception:
            val = 0.0

        opp = opp_map.get(str(team), "")

        proj_rows.append(
            {
                "Name": name,
                "NormName": nm,
                "Team": team,
                "Opponent": opp,
                "Position": pos,
                "Line": line_info,
                "Proj Goals": exp_goals,
                "Proj Assists": exp_assists,
                "Proj SOG": exp_sog,
                "Proj Blocks": exp_blk,
                "DK Points": dk_points,
                "Salary": sal,
                "DFS Value Score": val,
            }
        )

    proj_df = pd.DataFrame(proj_rows)
    out_path = os.path.join(DATA_DIR, "dfs_projections.csv")
    try:
        proj_df.to_csv(out_path, index=False)
        print("✅ Skater projections saved to", out_path)
    except Exception as e:
        print("⚠️ Failed to save skater projections:", e)

    return proj_df

def build_goalies(goalie_df, team_stats, opp_map):
    """Build goalie projections using SV% and opponent shot volume."""
    if goalie_df is None or goalie_df.empty:
        print("⚠️ Goalie stats empty; returning empty DF.")
        return pd.DataFrame()

    gdf = goalie_df.copy()
    if "Team" not in gdf.columns:
        gdf["Team"] = ""

    team_stats = normalize_nst_team_stats(team_stats)

    rows = []
    for _, row in gdf.iterrows():
        nm = row.get("NormName", "")
        name = row.get("Name", row.get("Player", ""))
        team = row.get("Team", "")
        sv = row.get("SV%", LEAGUE_AVG_SV)

        opp = opp_map.get(str(team), "")
        opp_row = team_stats[team_stats["Team"] == opp]
        if not opp_row.empty:
            try:
                sf60 = float(opp_row["SF/60"].values[0])
            except Exception:
                sf60 = FALLBACK_SF60
        else:
            sf60 = FALLBACK_SF60

        toi = 60.0
        shots_against = sf60 * (toi / 60.0)
        save_pct = float(sv)
        if save_pct > 1.0:
            save_pct = save_pct / 100.0

        ga = shots_against * (1.0 - save_pct)
        saves = shots_against - ga
        win_prob = 0.5
        shutout_prob = 0.05

        dk_points = saves * 0.7 - ga * 3.5 + win_prob * 6.0 + shutout_prob * 4.0

        rows.append(
            {
                "Name": name,
                "NormName": nm,
                "Team": team,
                "Opponent": opp,
                "SV%": save_pct,
                "Shots Against": shots_against,
                "Goals Against": ga,
                "Saves": saves,
                "DK Points": dk_points,
            }
        )

    proj = pd.DataFrame(rows)
    out_path = os.path.join(DATA_DIR, "goalies.csv")
    try:
        proj.to_csv(out_path, index=False)
        print("✅ Goalie projections saved to", out_path)
    except Exception as e:
        print("⚠️ Failed to save goalie projections:", e)

    return proj

def build_stacks(dfs_proj):
    """Very simple stack builder: sum DK points by team + line."""
    if dfs_proj is None or dfs_proj.empty:
        print("⚠️ Skater projections empty in build_stacks; returning empty DF.")
        return pd.DataFrame()

    df = dfs_proj.copy()
    grp = df.groupby(["Team", "Line"], as_index=False)["DK Points"].sum()
    grp = grp.rename(columns={"DK Points": "Stack DK Points"})
    grp = grp.sort_values("Stack DK Points", ascending=False)

    out_path = os.path.join(DATA_DIR, "top_stacks.csv")
    try:
        grp.to_csv(out_path, index=False)
        print("✅ Stack projections saved to", out_path)
    except Exception as e:
        print("⚠️ Failed to save stack projections:", e)

    return grp

# -------------------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------------------

def main():
    print("🚀 Starting NHL DFS pipeline")

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

    dk_df = load_dk_salaries()
    print("✅ DK salaries shape:", getattr(dk_df, "shape", None))

    schedule_df = get_today_schedule()
    if schedule_df.empty:
        print("ℹ️ No schedule entries for today; stopping projections.")
        return

    opp_map = build_opp_map(schedule_df)

    print("📊 Fetching NST team stats...")
    team_stats_raw = nst_scraper.get_team_stats(CURR_SEASON)
    team_stats = normalize_nst_team_stats(team_stats_raw)
    print("✅ Team stats rows:", len(team_stats))

    print("📊 Fetching NST skater stats...")
    nst_players_raw = []
    teams_today = pd.unique(schedule_df[["Home", "Away"]].values.ravel())
    for team in teams_today:
        try:
            nst_players_raw.append(nst_scraper.get_team_players(team, CURR_SEASON, tgp=10))
            nst_players_raw.append(nst_scraper.get_team_players(team, CURR_SEASON))
        except Exception as e:
            print("⚠️ NST skater fetch failed for", team, ":", e)

    if nst_players_raw:
        try:
            nst_df_raw = pd.concat(nst_players_raw, ignore_index=True)
        except Exception:
            nst_df_raw = pd.DataFrame()
    else:
        nst_df_raw = pd.DataFrame()

    nst_df = normalize_nst_skaters(nst_df_raw)
    print("✅ NST skaters rows:", len(nst_df))

    print("📊 Fetching NST goalie stats...")
    goalie_df_raw = nst_scraper.get_goalies(CURR_SEASON)
    goalie_df = normalize_goalie_stats(goalie_df_raw)
    print("✅ NST goalies rows:", len(goalie_df))

    print("📊 Fetching line assignments...")
    lines_df = get_all_lines(schedule_df)
    print("✅ Lines rows:", len(lines_df))

    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to", DATA_DIR)

    try:
        print("📊 Exporting results to Excel...")
        output_path = os.path.join(
            DATA_DIR, "projections_" + datetime.today().strftime("%Y%m%d") + ".xlsx"
        )
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            dfs_proj.to_excel(writer, sheet_name="Skaters", index=False)
            goalie_proj.to_excel(writer, sheet_name="Goalies", index=False)
            stack_proj.to_excel(writer, sheet_name="Stacks", index=False)
            team_stats.to_excel(writer, sheet_name="Teams", index=False)
            nst_df.to_excel(writer, sheet_name="NST_Raw", index=False)
        print("✅ Excel workbook ready:", output_path)
    except Exception as e:
        print("⚠️ Excel export failed:", e)

    try:
        print("📤 Uploading projections to Google Sheets...")
        tabs = {
            "Skaters": dfs_proj,
            "Goalies": goalie_proj,
            "Stacks": stack_proj,
            "Teams": team_stats,
            "NST_Raw": nst_df,
        }
        upload_to_sheets("NHL Projections", tabs)
    except Exception as e:
        print("⚠️ Upload to Google Sheets failed:", e)

    print("🏁 Done.")

if __name__ == "__main__":
    main()
