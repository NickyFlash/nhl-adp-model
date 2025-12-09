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

# DK scoring weights (adjust if you use different)
DK_WEIGHTS = {
    "goal": 8.5,
    "assist": 5.0,
    "shot": 1.5,
    "block": 1.3,
}

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

class NSTScraperStub(object):
    def get_team_stats(self, season):
        # Replace with your real NST team stats fetch
        return pd.DataFrame()

    def get_team_players(self, team, season, tgp=None):
        # Replace with your real NST player stats fetch
        return pd.DataFrame()

    def get_goalies(self, season):
        # Replace with your real NST goalie stats fetch
        return pd.DataFrame()

nst_scraper = NSTScraperStub()

def ingest_baseline_if_needed():
    return {"status": "stub", "players": 0}

def fetch_lineups():
    return {"status": "stub", "count": 0}

def join_lineups_with_baseline(lineups):
    return pd.DataFrame()

def load_processed():
    # processed_F, processed_D, processed_team, processed_goalies, players_df
    return None, None, None, None, pd.DataFrame()

def tag_missing_baseline(merged_lineups, players_df):
    return merged_lineups

def players_missing_baseline(merged_lineups):
    return pd.DataFrame()

def load_dk_salaries():
    # Replace with your real DK salary loader.
    # For now, return empty so code runs.
    return pd.DataFrame()

def upload_to_sheets(sheet_name, tabs_dict):
    # Replace with your real Google Sheets uploader.
    print("Sheets upload stub called for:", sheet_name)

# -------------------------------------------------------------------
# UTILS
# -------------------------------------------------------------------

def norm_name(name):
    if pd.isna(name):
        return ""
    s = str(name).strip().lower()
    s = s.replace(".", "").replace("'", "").replace("-", " ")
    s = " ".join(s.split())
    return s

def get_today_schedule():
    # Very simple stub: read schedule_today.csv if present, else empty
    schedule_path = os.path.join(DATA_DIR, "schedule_today.csv")
    if os.path.exists(schedule_path):
        return pd.read_csv(schedule_path)
    return pd.DataFrame()

def build_opp_map(schedule_df):
    opp_map = {}
    if schedule_df is None or schedule_df.empty:
        return opp_map
    for _, row in schedule_df.iterrows():
        h = str(row.get("Home", "")).strip().upper()
        a = str(row.get("Away", "")).strip().upper()
        if h and a:
            opp_map[h] = a
            opp_map[a] = h
    return opp_map

def get_all_lines(schedule_df):
    # Stub: no real line data, just empty DF with expected columns
    return pd.DataFrame(columns=["NormName", "Team", "Assignment"])

# -------------------------------------------------------------------
# NST NORMALIZATION
# -------------------------------------------------------------------

def normalize_nst_team_stats(team_stats_raw):
    if team_stats_raw is None or team_stats_raw.empty:
        cols = ["Team", "SF/60", "xGA/60"]
        return pd.DataFrame(columns=cols)

    df = team_stats_raw.copy()

    # Normalize team code
    if "Team" not in df.columns:
        for cand in ["team", "TEAM", "Team Name"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "Team"})
                break
    df["Team"] = df["Team"].astype(str).str.upper()

    rename_map = {}
    for col in df.columns:
        if col.upper() in ["CF60", "CORSI FOR/60"]:
            rename_map[col] = "CF/60"
        if col.upper() in ["FF60", "FENWICK FOR/60"]:
            rename_map[col] = "FF/60"
        if col.upper() in ["SF60", "SHOTS FOR/60"]:
            rename_map[col] = "SF/60"
        if col.upper() in ["XGF60", "EXPECTED GOALS FOR/60"]:
            rename_map[col] = "xGF/60"
        if col.upper() in ["XGA60", "EXPECTED GOALS AGAINST/60"]:
            rename_map[col] = "xGA/60"
    df = df.rename(columns=rename_map)

    for need in ["SF/60", "xGA/60"]:
        if need not in df.columns:
            df[need] = FALLBACK_SF60 if need == "SF/60" else FALLBACK_xGA60

    keep = ["Team", "SF/60", "xGA/60"]
    extra = [c for c in df.columns if c not in keep]
    df = df[keep + extra]
    return df

def normalize_nst_skaters(nst_df_raw):
    if nst_df_raw is None or nst_df_raw.empty:
        cols = [
            "NormName",
            "Player",
            "Team",
            "Position",
            "Goals",
            "Assists",
            "Shots",
            "Blocks",
            "TOI",
            "G60",
            "A60",
            "SOG60",
            "BLK60",
        ]
        return pd.DataFrame(columns=cols)

    df = nst_df_raw.copy()

    # Player name
    player_col = None
    for cand in ["Player", "Player Name", "Name"]:
        if cand in df.columns:
            player_col = cand
            break
    if not player_col:
        df["Player"] = ""
    else:
        df = df.rename(columns={player_col: "Player"})
    df["NormName"] = df["Player"].astype(str).apply(norm_name)

    # Team
    team_col = None
    for cand in ["Team", "Tm", "TEAM"]:
        if cand in df.columns:
            team_col = cand
            break
    if team_col and team_col != "Team":
        df = df.rename(columns={team_col: "Team"})
    if "Team" not in df.columns:
        df["Team"] = ""
    df["Team"] = df["Team"].astype(str).str.upper()

    # Position
    pos_col = None
    for cand in ["Pos", "Position"]:
        if cand in df.columns:
            pos_col = cand
            break
    if pos_col and pos_col != "Position":
        df = df.rename(columns={pos_col: "Position"})
    if "Position" not in df.columns:
        df["Position"] = ""

    # Raw counting stats
    def first_present(cols):
        for c in cols:
            if c in df.columns:
                return c
        return None

    g_col = first_present(["G", "Goals"])
    a_col = first_present(["A", "Assists"])
    s_col = first_present(["S", "Shots", "SOG"])
    b_col = first_present(["BLK", "Blocks"])
    toi_col = first_present(["TOI", "TOI (min)", "Minutes"])

    for name, src in [
        ("Goals", g_col),
        ("Assists", a_col),
        ("Shots", s_col),
        ("Blocks", b_col),
    ]:
        if src:
            df[name] = pd.to_numeric(df[src], errors="coerce").fillna(0.0)
        else:
            df[name] = 0.0

    if toi_col:
        df["TOI"] = pd.to_numeric(df[toi_col], errors="coerce").fillna(0.0)
    else:
        df["TOI"] = 0.0

    # Compute per 60 ourselves
    df["TOI_safe"] = df["TOI"].replace(0, pd.NA)
    df["G60"] = (df["Goals"] / df["TOI_safe"] * 60).fillna(0.0)
    df["A60"] = (df["Assists"] / df["TOI_safe"] * 60).fillna(0.0)
    df["SOG60"] = (df["Shots"] / df["TOI_safe"] * 60).fillna(0.0)
    df["BLK60"] = (df["Blocks"] / df["TOI_safe"] * 60).fillna(0.0)
    df = df.drop(columns=["TOI_safe"])

    keep = [
        "NormName",
        "Player",
        "Team",
        "Position",
        "Goals",
        "Assists",
        "Shots",
        "Blocks",
        "TOI",
        "G60",
        "A60",
        "SOG60",
        "BLK60",
    ]
    extra = [c for c in df.columns if c not in keep]
    df = df[keep + extra]

    # Drop duplicate players, keep first row
    df = df.drop_duplicates(subset=["NormName"], keep="first")
    return df

def normalize_goalie_stats(goalie_df_raw):
    if goalie_df_raw is None or goalie_df_raw.empty:
        cols = ["NormName", "Player", "Team", "SV%", "SA/60"]
        return pd.DataFrame(columns=cols)
    df = goalie_df_raw.copy()

    player_col = None
    for cand in ["Player", "Goalie", "Name"]:
        if cand in df.columns:
            player_col = cand
            break
    if not player_col:
        df["Player"] = ""
    else:
        df = df.rename(columns={player_col: "Player"})
    df["NormName"] = df["Player"].astype(str).apply(norm_name)

    team_col = None
    for cand in ["Team", "Tm", "TEAM"]:
        if cand in df.columns:
            team_col = cand
            break
    if team_col and team_col != "Team":
        df = df.rename(columns={team_col: "Team"})
    if "Team" not in df.columns:
        df["Team"] = ""
    df["Team"] = df["Team"].astype(str).str.upper()

    sv_col = None
    for cand in ["SV%", "Sv%", "SvPct", "SVPCT"]:
        if cand in df.columns:
            sv_col = cand
            break
    if sv_col:
        df["SV%"] = pd.to_numeric(df[sv_col], errors="coerce").fillna(LEAGUE_AVG_SV)
    else:
        df["SV%"] = LEAGUE_AVG_SV

    sa60_col = None
    for cand in ["SA/60", "SA60", "Shots Against/60"]:
        if cand in df.columns:
            sa60_col = cand
            break
    if sa60_col:
        df["SA/60"] = pd.to_numeric(df[sa60_col], errors="coerce").fillna(FALLBACK_SF60)
    else:
        df["SA/60"] = FALLBACK_SF60

    keep = ["NormName", "Player", "Team", "SV%", "SA/60"]
    extra = [c for c in df.columns if c not in keep]
    df = df[keep + extra]
    df = df.drop_duplicates(subset=["NormName"], keep="first")
    return df

# -------------------------------------------------------------------
# PROJECTION BUILDERS
# -------------------------------------------------------------------

def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    if dk_df is None or dk_df.empty:
        return pd.DataFrame(columns=[
            "Player", "Team", "Opponent", "Position", "Line",
            "Salary", "Proj Goals", "Proj Assists", "Proj SOG",
            "Proj Blocks", "DK Points", "Value"
        ])

    df = dk_df.copy()
    df["NormName"] = df["Name"].astype(str).apply(norm_name)

    # Merge NST per-60
    nst_cols = ["NormName", "G60", "A60", "SOG60", "BLK60"]
    for c in nst_cols:
        if c not in nst_df.columns and c != "NormName":
            nst_df[c] = 0.0
    nst_merge = nst_df[nst_cols]
    df = df.merge(nst_merge, on="NormName", how="left", suffixes=("", "_nst"))

    # Fallback by position
    def apply_fallback(row):
        pos = str(row.get("Position", "F"))
        is_d = "D" in pos
        fb = SETTINGS["D_fallback"] if is_d else SETTINGS["F_fallback"]
        for stat in ["G60", "A60", "SOG60", "BLK60"]:
            if pd.isna(row.get(stat, None)) or row.get(stat, 0) == 0:
                row[stat] = fb[stat]
        return row

    df = df.apply(apply_fallback, axis=1)

    # Expected TOI can be simple global (you can make this smarter later)
    df["ExpTOI"] = 18.0  # minutes per game baseline

    # Role / line multipliers
    if not lines_df.empty:
        # lines_df should have NormName, Team, Assignment
        lines_df["NormName"] = lines_df["NormName"].astype(str).apply(norm_name)
        lines_df["Team"] = lines_df["Team"].astype(str).str.upper()
        df = df.merge(
            lines_df[["NormName", "Assignment"]],
            on="NormName",
            how="left",
        )
    else:
        df["Assignment"] = "NA"

    def line_mult(row):
        assign = str(row.get("Assignment", "NA"))
        return SETTINGS["LineMult_byType"].get(assign, 1.0)

    df["LineMult"] = df.apply(line_mult, axis=1)
    df["AdjTOI"] = df["ExpTOI"] * df["LineMult"]

    # Per-game projections from per-60 and adj TOI
    df["Proj Goals"] = df["G60"] * (df["AdjTOI"] / 60.0)
    df["Proj Assists"] = df["A60"] * (df["AdjTOI"] / 60.0)
    df["Proj SOG"] = df["SOG60"] * (df["AdjTOI"] / 60.0)
    df["Proj Blocks"] = df["BLK60"] * (df["AdjTOI"] / 60.0)

    # DK points
    df["DK Points"] = (
        df["Proj Goals"] * DK_WEIGHTS["goal"]
        + df["Proj Assists"] * DK_WEIGHTS["assist"]
        + df["Proj SOG"] * DK_WEIGHTS["shot"]
        + df["Proj Blocks"] * DK_WEIGHTS["block"]
    )

    # Opponent from opp_map
    df["Team"] = df["TeamAbbrev"].astype(str).str.upper()
    df["Opponent"] = df["Team"].map(opp_map).fillna("")

    # Value metric
    df["Value"] = df["DK Points"] / (df["Salary"] / 1000.0)

    # Clean, sorted output
    out_cols = [
        "Name",
        "NormName",
        "Team",
        "Opponent",
        "Position",
        "Assignment",
        "Salary",
        "ExpTOI",
        "AdjTOI",
        "G60",
        "A60",
        "SOG60",
        "BLK60",
        "Proj Goals",
        "Proj Assists",
        "Proj SOG",
        "Proj Blocks",
        "DK Points",
        "Value",
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    df = df[out_cols].rename(columns={"Name": "Player", "Assignment": "Line"})
    df = df.sort_values("DK Points", ascending=False).reset_index(drop=True)
    return df

def build_goalies(goalie_df, team_stats, opp_map):
    if goalie_df is None or goalie_df.empty:
        cols = ["Player", "Team", "Opponent", "SV%", "SA/60", "DK Points"]
        return pd.DataFrame(columns=cols)

    df = goalie_df.copy()
    df["Team"] = df["Team"].astype(str).str.upper()
    df["Opponent"] = df["Team"].map(opp_map).fillna("")

    # Simple projection: SA/60 from team_stats opponent SF/60
    if not team_stats.empty:
        ts = team_stats[["Team", "SF/60"]].rename(columns={"Team": "OppTeam", "SF/60": "OppSF/60"})
        df = df.merge(ts, left_on="Opponent", right_on="OppTeam", how="left")
        df["SA/60"] = df["OppSF/60"].fillna(df["SA/60"])
    else:
        if "SA/60" not in df.columns:
            df["SA/60"] = FALLBACK_SF60

    df["ExpTOI"] = 60.0
    df["Proj Saves"] = df["SA/60"] * (df["ExpTOI"] / 60.0) * df["SV%"]
    df["Proj GA"] = df["SA/60"] * (df["ExpTOI"] / 60.0) * (1.0 - df["SV%"])

    # Very simple DK projection: 0.7 per save, -1 per GA
    df["DK Points"] = df["Proj Saves"] * 0.7 - df["Proj GA"]

    out_cols = [
        "Player",
        "Team",
        "Opponent",
        "SV%",
        "SA/60",
        "Proj Saves",
        "Proj GA",
        "DK Points",
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    df = df[out_cols].sort_values("DK Points", ascending=False).reset_index(drop=True)
    return df

def build_stacks(dfs_proj):
    if dfs_proj is None or dfs_proj.empty:
        cols = ["Team", "Line", "Players", "Stack DK Points", "Stack Salary", "Stack Value"]
        return pd.DataFrame(columns=cols)

    df = dfs_proj.copy()
    if "Line" not in df.columns:
        df["Line"] = "NA"
    df["Team"] = df["Team"].astype(str).str.upper()

    # Group by Team + Line
    group_cols = ["Team", "Line"]
    grouped = df.groupby(group_cols)

    rows = []
    for (team, line), g in grouped:
        players = ", ".join(g["Player"].astype(str).tolist())
        stack_dk = g["DK Points"].sum()
        stack_salary = g["Salary"].sum() if "Salary" in g.columns else 0
        stack_value = stack_dk / (stack_salary / 1000.0) if stack_salary > 0 else 0.0
        rows.append(
            {
                "Team": team,
                "Line": line,
                "Players": players,
                "Stack DK Points": stack_dk,
                "Stack Salary": stack_salary,
                "Stack Value": stack_value,
            }
        )

    stacks_df = pd.DataFrame(rows)
    if stacks_df.empty:
        cols = ["Team", "Line", "Players", "Stack DK Points", "Stack Salary", "Stack Value"]
        return pd.DataFrame(columns=cols)

    stacks_df = stacks_df.sort_values("Stack DK Points", ascending=False).reset_index(drop=True)
    return stacks_df

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    print("🚀 Starting NHL projections pipeline...")

    baseline_meta = ingest_baseline_if_needed()
    print("Baseline meta:", baseline_meta)

    lineups_resp = fetch_lineups()
    print("Lineups fetch:", lineups_resp)

    merged_lineups = join_lineups_with_baseline(lineups_resp)
    processed_F, processed_D, processed_team, processed_goalies, players_df = load_processed()

    if isinstance(merged_lineups, pd.DataFrame) and isinstance(players_df, pd.DataFrame):
        merged_lineups = tag_missing_baseline(merged_lineups, players_df)
        missing = players_missing_baseline(merged_lineups)
        print("Players missing baseline rows:", len(missing))

    dk_df = load_dk_salaries()
    if dk_df is None or dk_df.empty:
        print("⚠️ DK salaries empty. Skater projections will be empty.")
        dk_df = pd.DataFrame(
            columns=["Name", "TeamAbbrev", "Position", "Salary"]
        )

    print("📅 Loading schedule...")
    schedule_df = get_today_schedule()
    opp_map = build_opp_map(schedule_df)
    print("Opp map:", opp_map)

    print("📊 Fetching NST team stats...")
    team_stats_raw = nst_scraper.get_team_stats(CURR_SEASON)
    team_stats = normalize_nst_team_stats(team_stats_raw)
    print("Team stats rows:", len(team_stats))

    print("📊 Fetching NST skater stats for teams in DK salaries...")
    teams = dk_df.get("TeamAbbrev", pd.Series(dtype=str)).dropna().unique().tolist()
    nst_players_raw = []
    for team in teams:
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
    print("NST skaters rows:", len(nst_df))

    print("📊 Fetching NST goalie stats...")
    goalie_df_raw = nst_scraper.get_goalies(CURR_SEASON)
    goalie_df = normalize_goalie_stats(goalie_df_raw)
    print("NST goalies rows:", len(goalie_df))

    print("📊 Fetching line assignments...")
    lines_df = get_all_lines(schedule_df)
    print("Lines rows:", len(lines_df))

    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)
    print("Skaters rows:", len(dfs_proj))

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)
    print("Goalies rows:", len(goalie_proj))

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)
    print("Stacks rows:", len(stack_proj))

    print("✅ All CSV outputs saved to", DATA_DIR)

    adp_df = dfs_proj.copy()
    if not adp_df.empty and "DK Points" in adp_df.columns:
        adp_df["Rank"] = adp_df["DK Points"].rank(ascending=False, method="min")
        cols = [
            "Rank",
            "Player",
            "Team",
            "Opponent",
            "Position",
            "Line",
            "Salary",
            "Proj Goals",
            "Proj Assists",
            "Proj SOG",
            "Proj Blocks",
            "DK Points",
            "Value",
        ]
        cols = [c for c in cols if c in adp_df.columns]
        adp_df = adp_df[cols].sort_values("Rank")

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
            if not adp_df.empty:
                adp_df.to_excel(writer, sheet_name="ADP_View", index=False)
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
        if not adp_df.empty:
            tabs["ADP_View"] = adp_df
        upload_to_sheets("NHL Projections", tabs)
    except Exception as e:
        print("⚠️ Upload to Google Sheets failed:", e)

    print("🏁 Done.")

if __name__ == "__main__":
    main()
