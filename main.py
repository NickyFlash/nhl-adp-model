# main.py
# NHL DFS projections with optional DK salaries and line-aware stacks.

import os
from datetime import datetime

import pandas as pd

# -------------------------------------------------------------------
# BASIC SETTINGS
# -------------------------------------------------------------------

DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

CURR_SEASON = "2024-2025"

FALLBACK_SF60 = 30.0
FALLBACK_xGA60 = 2.8
LEAGUE_AVG_SV = 0.905

DK_WEIGHTS = {
    "goal": 8.0,
    "assist": 5.0,
    "shot": 1.6,
    "block": 1.3,
}

FALLBACK_PER60 = {
    "D": {"G60": 0.2, "A60": 0.8, "SOG60": 3.0, "BLK60": 2.0},
    "F": {"G60": 0.7, "A60": 0.7, "SOG60": 3.5, "BLK60": 0.5},
}

# -------------------------------------------------------------------
# STUBS FOR YOUR REAL DATA SOURCES
# -------------------------------------------------------------------

class NSTScraperStub(object):
    def get_team_stats(self, season):
        return pd.DataFrame()

    def get_team_players(self, team, season, tgp=None):
        return pd.DataFrame()

    def get_goalies(self, season):
        return pd.DataFrame()

nst_scraper = NSTScraperStub()

def load_dk_salaries():
    """
    OPTIONAL.
    Return DraftKings salaries as a DataFrame with columns:
    Player, Team, Position, Salary, Opponent (optional).

    For now we return an empty DF so projections can run without salaries.
    When you are ready, replace this with a real CSV loader.
    """
    return pd.DataFrame()

def get_today_schedule():
    """
    OPTIONAL real implementation.
    For now, return empty DF so we don't block projections.
    Ideally: columns Home, Away.
    """
    return pd.DataFrame(columns=["Home", "Away"])

def upload_to_sheets(sheet_name, tabs_dict):
    # Stub; safe no-op
    print("Sheets upload skipped (stub).")

def get_all_lines(schedule_df):
    """
    OPTIONAL real implementation.
    For now, returns empty DF so Line stays 'NA' but stacks still group by NA.
    Real version should have columns:
      NormName, Team, Line
    """
    return pd.DataFrame(columns=["NormName", "Team", "Line"])

# -------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------

def safe_get_from_series(s, keys, default=None):
    for k in keys:
        if k in s and pd.notnull(s[k]):
            return s[k]
    return default

def norm_name(name):
    if not isinstance(name, str):
        return ""
    return "".join(name.lower().split())

def build_opp_map(schedule_df):
    opp_map = {}
    if schedule_df is None or schedule_df.empty:
        return opp_map
    if not {"Home", "Away"}.issubset(schedule_df.columns):
        return opp_map
    for _, row in schedule_df.iterrows():
        h = str(row["Home"]).upper()
        a = str(row["Away"]).upper()
        if h and a:
            opp_map[h] = a
            opp_map[a] = h
    return opp_map

# -------------------------------------------------------------------
# NST NORMALIZATION
# -------------------------------------------------------------------

def normalize_nst_team_stats(team_stats_raw):
    if team_stats_raw is None or team_stats_raw.empty:
        cols = ["Team", "SF60", "xGF60", "xGA60"]
        return pd.DataFrame(columns=cols)

    df = team_stats_raw.copy()
    rename_map = {
        "Team": "Team",
        "team": "Team",
        "Tm": "Team",
        "SF60": "SF60",
        "SF/60": "SF60",
        "ShotsForPer60": "SF60",
        "xGF60": "xGF60",
        "xGF/60": "xGF60",
        "xGA60": "xGA60",
        "xGA/60": "xGA60",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
    if "Team" not in df.columns and "TeamID" in df.columns:
        df["Team"] = df["TeamID"]

    if "Team" not in df.columns:
        df["Team"] = ""
    df["Team"] = df["Team"].astype(str).str.upper()

    for col, fb in [("SF60", FALLBACK_SF60), ("xGF60", FALLBACK_SF60), ("xGA60", FALLBACK_xGA60)]:
        if col not in df.columns:
            df[col] = fb
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(fb)

    return df[["Team", "SF60", "xGF60", "xGA60"]]

def normalize_nst_skaters(nst_df_raw):
    if nst_df_raw is None or nst_df_raw.empty:
        cols = [
            "Player", "NormName", "Team", "Position",
            "Games", "TOI",
            "Goals", "Assists", "Shots", "Blocks",
            "G60", "A60", "SOG60", "BLK60",
        ]
        return pd.DataFrame(columns=cols)

    df = nst_df_raw.copy()

    name_cols = ["Name", "Player", "Player Name"]
    name_col = None
    for c in name_cols:
        if c in df.columns:
            name_col = c
            break
    if name_col is None:
        df["Player"] = ""
    else:
        df["Player"] = df[name_col].astype(str)
    df["NormName"] = df["Player"].apply(norm_name)

    team_cols = ["Team", "Tm", "Team Name"]
    tcol = None
    for c in team_cols:
        if c in df.columns:
            tcol = c
            break
    if tcol is None:
        df["Team"] = ""
    else:
        df["Team"] = df[tcol].astype(str).str.upper()

    pos_cols = ["Pos", "Position"]
    pcol = None
    for c in pos_cols:
        if c in df.columns:
            pcol = c
            break
    if pcol is None:
        df["Position"] = "F"
    else:
        df["Position"] = df[pcol].astype(str).str.upper().str[0]

    df["Games"] = pd.to_numeric(
        safe_get_from_series(df, ["GP", "Games"], default=0),
        errors="coerce"
    ).fillna(0)

    toi_raw = None
    for c in ["TOI", "TOI_Total", "TOI (min)", "Minutes"]:
        if c in df.columns:
            toi_raw = df[c]
            break
    if toi_raw is None:
        df["TOI"] = df["Games"] * 15.0
    else:
        if toi_raw.dtype == object:
            mins = []
            for v in toi_raw:
                if isinstance(v, str) and ":" in v:
                    parts = v.split(":")
                    try:
                        m = float(parts[0])
                        s = float(parts[1])
                        mins.append(m + s / 60.0)
                    except Exception:
                        mins.append(0.0)
                else:
                    try:
                        mins.append(float(v))
                    except Exception:
                        mins.append(0.0)
            df["TOI"] = mins
        else:
            df["TOI"] = pd.to_numeric(toi_raw, errors="coerce").fillna(0)

    df["Goals"] = pd.to_numeric(
        safe_get_from_series(df, ["G", "Goals"], default=0),
        errors="coerce"
    ).fillna(0)
    df["Assists"] = pd.to_numeric(
        safe_get_from_series(df, ["A", "Assists"], default=0),
        errors="coerce"
    ).fillna(0)
    df["Shots"] = pd.to_numeric(
        safe_get_from_series(df, ["S", "Shots", "Shots on Goal"], default=0),
        errors="coerce"
    ).fillna(0)
    df["Blocks"] = pd.to_numeric(
        safe_get_from_series(df, ["B", "Blocks"], default=0),
        errors="coerce"
    ).fillna(0)

    toi60 = df["TOI"].replace(0, 1e-6)
    df["G60"] = df["Goals"] / toi60 * 60.0
    df["A60"] = df["Assists"] / toi60 * 60.0
    df["SOG60"] = df["Shots"] / toi60 * 60.0
    df["BLK60"] = df["Blocks"] / toi60 * 60.0

    return df[
        [
            "Player",
            "NormName",
            "Team",
            "Position",
            "Games",
            "TOI",
            "Goals",
            "Assists",
            "Shots",
            "Blocks",
            "G60",
            "A60",
            "SOG60",
            "BLK60",
        ]
    ]

def normalize_goalie_stats(goalie_df_raw):
    if goalie_df_raw is None or goalie_df_raw.empty:
        cols = ["Player", "NormName", "Team", "SV%", "Minutes"]
        return pd.DataFrame(columns=cols)

    df = goalie_df_raw.copy()
    name_cols = ["Name", "Player", "Player Name"]
    name_col = None
    for c in name_cols:
        if c in df.columns:
            name_col = c
            break
    if name_col is None:
        df["Player"] = ""
    else:
        df["Player"] = df[name_col].astype(str)
    df["NormName"] = df["Player"].apply(norm_name)

    team_cols = ["Team", "Tm", "Team Name"]
    tcol = None
    for c in team_cols:
        if c in df.columns:
            tcol = c
            break
    if tcol is None:
        df["Team"] = ""
    else:
        df["Team"] = df[tcol].astype(str).str.upper()

    sv_cols = ["SV%", "Sv%", "Save%"]
    scol = None
    for c in sv_cols:
        if c in df.columns:
            scol = c
            break
    if scol is None:
        df["SV%"] = LEAGUE_AVG_SV
    else:
        df["SV%"] = pd.to_numeric(df[scol], errors="coerce").fillna(LEAGUE_AVG_SV)
        m = df["SV%"].mean()
        if m > 1.5:
            df["SV%"] = df["SV%"] / 100.0

    min_cols = ["TOI", "Minutes", "Min"]
    mcol = None
    for c in min_cols:
        if c in df.columns:
            mcol = c
            break
    if mcol is None:
        df["Minutes"] = 2000.0
    else:
        df["Minutes"] = pd.to_numeric(df[mcol], errors="coerce").fillna(2000.0)

    return df[["Player", "NormName", "Team", "SV%", "Minutes"]]

# -------------------------------------------------------------------
# PROJECTIONS – SKATERS
# -------------------------------------------------------------------

def build_line_multipliers(skaters_df, team_stats):
    """
    Build multiplier for each Team+Line based on how their summed
    per-60 offense compares to the team average.
    """
    if skaters_df is None or skaters_df.empty:
        return {}

    df = skaters_df.copy()
    df["Line"] = df.get("Line", "NA").fillna("NA")

    # Compute team baseline "offense index"
    team_baseline = (
        df.groupby("Team", dropna=False)[["G60", "SOG60"]]
        .mean()
        .reset_index()
    )
    team_baseline["team_index"] = team_baseline["G60"] + 0.1 * team_baseline["SOG60"]
    team_index_map = dict(
        zip(team_baseline["Team"], team_baseline["team_index"])
    )

    # Compute line "offense index"
    line_agg = (
        df.groupby(["Team", "Line"], dropna=False)[["G60", "SOG60"]]
        .sum()
        .reset_index()
    )
    line_agg["line_index"] = line_agg["G60"] + 0.1 * line_agg["SOG60"]

    line_agg["mult"] = 1.0
    for i, row in line_agg.iterrows():
        team = row["Team"]
        line_index = row["line_index"]
        team_index = team_index_map.get(team, None)
        if team_index is None or team_index <= 0:
            mult = 1.0
        else:
            raw_ratio = line_index / team_index
            mult = max(0.85, min(1.20, raw_ratio))
        line_agg.at[i, "mult"] = mult

    mult_map = {}
    for _, row in line_agg.iterrows():
        key = (row["Team"], row["Line"])
        mult_map[key] = row["mult"]

    return mult_map

def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    if nst_df is None or nst_df.empty:
        print("No NST skater data; returning empty skater projections.")
        return pd.DataFrame()

    df = nst_df.copy()

    # Attach lines if available
    if lines_df is not None and not lines_df.empty:
        lines_df = lines_df.copy()
        lines_df["NormName"] = lines_df["NormName"].astype(str)
        lines_df["Team"] = lines_df["Team"].astype(str).str.upper()
        df = df.merge(
            lines_df[["NormName", "Team", "Line"]],
            on=["NormName", "Team"],
            how="left",
        )
    else:
        df["Line"] = "NA"

    # Attach DK info if available
    if dk_df is not None and not dk_df.empty:
        dk_df = dk_df.copy()
        dk_df["NormName"] = dk_df["Player"].astype(str).apply(norm_name)
        dk_df["Team"] = dk_df["Team"].astype(str).str.upper()
        df = df.merge(
            dk_df[["NormName", "Team", "Position", "Salary"]],
            on=["NormName", "Team"],
            how="left",
            suffixes=("", "_DK"),
        )
    else:
        df["Position"] = df.get("Position", "F")
        df["Salary"] = pd.NA

    # Default TOI per game for projection
    df["TOI_per_game"] = df["TOI"].replace(0, 1e-6) / df["Games"].replace(0, 1.0)
    df["TOI_per_game"] = df["TOI_per_game"].replace(
        [float("inf"), -float("inf")], 15.0
    ).fillna(15.0)

    # Fill missing per 60 from fallbacks when necessary
    for i, row in df.iterrows():
        pos = row["Position"]
        if pos not in ["D", "F"]:
            pos = "F"
        fb = FALLBACK_PER60[pos]
        for stat, key in [
            ("G60", "G60"),
            ("A60", "A60"),
            ("SOG60", "SOG60"),
            ("BLK60", "BLK60"),
        ]:
            if pd.isna(row[stat]) or row[stat] == 0:
                df.at[i, stat] = fb[key]

    # Base game-level projections from per 60
    df["Proj Goals"] = df["G60"] * df["TOI_per_game"] / 60.0
    df["Proj Assists"] = df["A60"] * df["TOI_per_game"] / 60.0
    df["Proj SOG"] = df["SOG60"] * df["TOI_per_game"] / 60.0
    df["Proj Blocks"] = df["BLK60"] * df["TOI_per_game"] / 60.0

    df["DK Points Base"] = (
        df["Proj Goals"] * DK_WEIGHTS["goal"]
        + df["Proj Assists"] * DK_WEIGHTS["assist"]
        + df["Proj SOG"] * DK_WEIGHTS["shot"]
        + df["Proj Blocks"] * DK_WEIGHTS["block"]
    )

    # Opponent
    df["Opponent"] = df["Team"].map(opp_map) if opp_map else ""

    # Line multipliers
    line_mult_map = build_line_multipliers(df, team_stats)
    df["Line_Mult"] = df.apply(
        lambda r: line_mult_map.get((r["Team"], r["Line"]), 1.0), axis=1
    )
    df["DK Points"] = df["DK Points Base"] * df["Line_Mult"]

    # Value (only if Salary is present)
    if "Salary" in df.columns:
        df["Value"] = df.apply(
            lambda r: r["DK Points"] / (r["Salary"] / 1000.0)
            if pd.notna(r["Salary"]) and r["Salary"] > 0
            else pd.NA,
            axis=1,
        )
    else:
        df["Value"] = pd.NA

    cols = [
        "Player",
        "Team",
        "Opponent",
        "Position",
        "Line",
        "Games",
        "TOI_per_game",
        "G60",
        "A60",
        "SOG60",
        "BLK60",
        "Proj Goals",
        "Proj Assists",
        "Proj SOG",
        "Proj Blocks",
        "DK Points Base",
        "Line_Mult",
        "DK Points",
        "Salary",
        "Value",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].sort_values("DK Points", ascending=False)

    return df

# -------------------------------------------------------------------
# PROJECTIONS – GOALIES
# -------------------------------------------------------------------

def build_goalies(goalie_df, team_stats, opp_map):
    if goalie_df is None or goalie_df.empty:
        print("No goalie data; returning empty goalie projections.")
        return pd.DataFrame()

    df = goalie_df.copy()
    df["Opponent"] = df["Team"].map(opp_map) if opp_map else ""

    team_stats = team_stats.copy()
    idx = team_stats.set_index("Team")

    def get_opp_shots(row):
        opp = row["Opponent"]
        if opp in idx.index:
            return idx.loc[opp, "SF60"]
        return FALLBACK_SF60

    def get_team_xga(row):
        tm = row["Team"]
        if tm in idx.index:
            return idx.loc[tm, "xGA60"]
        return FALLBACK_xGA60

    df["Opp_SF60"] = df.apply(get_opp_shots, axis=1)
    df["Team_xGA60"] = df.apply(get_team_xga, axis=1)

    df["Proj Shots Against"] = df["Opp_SF60"]
    df["Proj Saves"] = df["Proj Shots Against"] * df["SV%"]
    df["Proj GA"] = df["Proj Shots Against"] * (1.0 - df["SV%"])

    df["DK Points"] = (
        df["Proj Saves"] * 0.7
        - df["Proj GA"] * 3.5
    )

    cols = [
        "Player",
        "Team",
        "Opponent",
        "Proj Shots Against",
        "Proj Saves",
        "Proj GA",
        "DK Points",
    ]
    df = df[cols].sort_values("DK Points", ascending=False)
    return df

# -------------------------------------------------------------------
# PROJECTIONS – STACKS
# -------------------------------------------------------------------

def build_stacks(dfs_proj):
    """
    Build stacks from line-adjusted player projections.
    Works even if Salary is missing; then Stack_Salary and Stack_Value are NA.
    """
    if dfs_proj is None or dfs_proj.empty:
        print("No skater projections; returning empty stacks.")
        return pd.DataFrame()

    df = dfs_proj.copy()
    if "Line" not in df.columns:
        df["Line"] = "NA"
    df["Line"] = df["Line"].fillna("NA")

    for col in ["DK Points", "Salary"]:
        if col not in df.columns:
            df[col] = pd.NA

    grouped = df.groupby(["Team", "Line"], dropna=False)

    stack_rows = []
    for (team, line), sub in grouped:
        players = ", ".join(sub["Player"].astype(str).tolist())
        stack_dk = sub["DK Points"].sum(skipna=True)
        stack_salary = sub["Salary"].sum(skipna=True) if "Salary" in sub.columns else pd.NA
        if pd.isna(stack_salary) or stack_salary <= 0:
            stack_value = pd.NA
        else:
            stack_value = stack_dk / (stack_salary / 1000.0)

        stack_rows.append(
            {
                "Team": team,
                "Line": line,
                "Players": players,
                "Stack_DK_Points": stack_dk,
                "Stack_Salary": stack_salary,
                "Stack_Value": stack_value,
            }
        )

    stack_df = pd.DataFrame(stack_rows)
    if not stack_df.empty:
        stack_df = stack_df.sort_values("Stack_DK_Points", ascending=False)

    return stack_df

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    print("Starting projections...")

    # 1) Schedule + Opponent map
    schedule_df = get_today_schedule()
    opp_map = build_opp_map(schedule_df)
    print("Schedule rows: " + str(len(schedule_df)))
    print("Opp map size: " + str(len(opp_map)))

    # 2) NST team stats
    team_stats_raw = nst_scraper.get_team_stats(CURR_SEASON)
    team_stats = normalize_nst_team_stats(team_stats_raw)
    print("Team stats rows: " + str(len(team_stats)))

    # 3) NST skaters
    print("Fetching NST skater stats for all teams (stub)...")
    nst_players_list = []
    for team in team_stats["Team"].unique():
        try:
            nst_players_list.append(nst_scraper.get_team_players(team, CURR_SEASON))
        except Exception as e:
            print("NST skater fetch failed for " + str(team) + ": " + str(e))

    if nst_players_list:
        try:
            nst_df_raw = pd.concat(nst_players_list, ignore_index=True)
        except Exception:
            nst_df_raw = pd.DataFrame()
    else:
        nst_df_raw = pd.DataFrame()
    nst_df = normalize_nst_skaters(nst_df_raw)
    print("NST skaters rows: " + str(len(nst_df)))

    # 4) NST goalies
    goalie_df_raw = nst_scraper.get_goalies(CURR_SEASON)
    goalie_df = normalize_goalie_stats(goalie_df_raw)
    print("NST goalies rows: " + str(len(goalie_df)))

    # 5) Lines
    lines_df = get_all_lines(schedule_df)
    print("Lines rows: " + str(len(lines_df)))

    # 6) DK salaries (optional)
    dk_df = load_dk_salaries()
    if dk_df is None:
        dk_df = pd.DataFrame()
    print("DK salaries rows: " + str(len(dk_df)))

    # 7) Projections
    print("Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)
    print("Skaters rows: " + str(len(dfs_proj)))

    print("Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)
    print("Goalies rows: " + str(len(goalie_proj)))

    print("Building stack projections...")
    stack_proj = build_stacks(dfs_proj)
    print("Stacks rows: " + str(len(stack_proj)))

    # 8) Save to CSV
    dfs_proj.to_csv(os.path.join(DATA_DIR, "dfs_projections.csv"), index=False)
    goalie_proj.to_csv(os.path.join(DATA_DIR, "goalie_projections.csv"), index=False)
    stack_proj.to_csv(os.path.join(DATA_DIR, "stack_projections.csv"), index=False)

    # 9) ADP-style view
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

    # 10) Excel export
    try:
        print("Exporting to Excel...")
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
        print("Excel ready: " + str(output_path))
    except Exception as e:
        print("Excel export failed: " + str(e))

    # 11) Optional Sheets export
    try:
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
        print("Sheets upload failed: " + str(e))

    print("Done.")

if __name__ == "__main__":
    main()
