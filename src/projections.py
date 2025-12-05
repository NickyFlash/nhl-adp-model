import pandas as pd
import numpy as np

########################################
# 1. PER-60 RATE STATS (NST → per 60)
########################################

def add_player_rate_stats(nst_player_totals_df):
    df = nst_player_totals_df.copy()

    # Make sure TOI is not zero
    df["TOI"] = df["TOI"].replace(0, np.)
    df["TOI_PP"] = df.get("TOI_PP", 0).replace(0, np.)

    base_cols = ["CF", "CA", "SF", "SA", "xGF", "xGA"]
    for col in base_cols:
        df[col + "_60"] = (df[col] / df["TOI"]) * 60

    # Offensive base rates
    df["CF_off60"] = df["CF_60"]
    df["SF_off60"] = df["SF_60"]
    df["xGF_off60"] = df["xGF_60"]

    # Power play per-60
    if "TOI_PP" in df.columns:
        for col in base_cols:
            df[col + "_PP60"] = (df[col] / df["TOI_PP"]) * 60

    # Time on ice per game
    df["TOI_per_game"] = df["TOI"] / df["GP"]

    return df


########################################
# 2. LINE CONTEXT (DFO + NST lines)
########################################

def build_line_context(player_rates_df, dfo_lines_df, nst_line_stats_df):
    df = player_rates_df.copy()
    dfo = dfo_lines_df.copy()
    lines = nst_line_stats_df.copy()

    # Use NormName everywhere if you have it, fall back to Player
    if "NormName" in dfo.columns and "NormName" in df.columns:
        key_player = "NormName"
    else:
        key_player = "Player"

    # Basic line + PP tags from DFO
    dfo_basic = dfo[[
        key_player,
        "Team",
        "LineType",
        "PPUnit"
    ]].drop_duplicates()

    df = df.merge(
        dfo_basic,
        how="left",
        left_on=[key_player, "Team"],
        right_on=[key_player, "Team"]
    )

    # Create simple flags for line/PP
    df["IsTopLine"] = (df["LineType"] == "L1").astype(int)
    df["IsSecondLine"] = (df["LineType"] == "L2").astype(int)
    df["IsPP1"] = (df["PPUnit"] == "PP1").astype(int)
    df["IsPP2"] = (df["PPUnit"] == "PP2").astype(int)

    # Quick-and-dirty line key (Team + LineType) to join with NST line stats
    if "Team" in lines.columns and "LineType" in lines.columns:
        lines["LineKey"] = lines["Team"] + "_" + lines["LineType"]
        df["LineKey"] = df["Team"] + "_" + df["LineType"]

        line_cols = [
            "LineKey",
            "Line_TOI",
            "Line_CF60",
            "Line_xGF60",
            "Line_SF60"
        ]
        # Expect these in nst_line_stats_df; if not, adjust names there once
        lines_small = lines[line_cols].drop_duplicates()

        df = df.merge(
            lines_small,
            how="left",
            on="LineKey"
        )
    else:
        df["Line_TOI"] = np.
        df["Line_CF60"] = np.
        df["Line_xGF60"] = np.
        df["Line_SF60"] = np.

    return df


########################################
# 3. OPPONENT CONTEXT (TEAM NST STATS)
########################################

def add_opponent_context(df, team_stats_df):
    sk = df.copy()
    teams = team_stats_df.copy()

    # Expect columns like: Team, CF60, CA60, xGF60, xGA60
    # We will merge opponent stats in
    if "Opponent" not in sk.columns:
        # If you don't have Opponent yet, you can skip or set dummy for now
        sk["Opponent"] = sk["Team"]

    opp = teams.rename(columns={
        "Team": "Opponent",
        "CF60": "Opp_CF60",
        "CA60": "Opp_CA60",
        "xGF60": "Opp_xGF60",
        "xGA60": "Opp_xGA60"
    })

    sk = sk.merge(opp, how="left", on="Opponent")

    return sk


########################################
# 4. CORE SKATER PROJECTION FORMULA
########################################

def build_skater_projections(
    df,
    weight_shots=1.0,
    weight_goals=0.8,
    weight_assists=0.6,
    line_boost_weight=0.15,
    pp_boost_weight=0.20,
    opponent_def_weight=0.25
):
    sk = df.copy()

    # 1. Base "volume" score (shots + shot quality)
    sk["BaseShotScore"] = (
        sk["SF_off60"].fillna(0) * weight_shots
        + sk["xGF_off60"].fillna(0) * weight_goals
    )

    # 2. Simple scoring rate proxy
    if "G" in sk.columns and "A" in sk.columns:
        sk["Goals_per60"] = (sk["G"] / sk["TOI"]) * 60
        sk["Assists_per60"] = (sk["A"] / sk["TOI"]) * 60
    else:
        sk["Goals_per60"] = 0.05
        sk["Assists_per60"] = 0.08

    sk["ScoringScore"] = (
        sk["Goals_per60"].fillna(0) * weight_goals
        + sk["Assists_per60"].fillna(0) * weight_assists
    )

    # 3. Line boost (better line → more opportunity)
    sk["LineBoost"] = 0

    if "Line_xGF60" in sk.columns:
        line_xgf = sk["Line_xGF60"].fillna(sk["Line_xGF60"].median())
        line_xgf_norm = (line_xgf - line_xgf.min()) / (
            line_xgf.max() - line_xgf.min() + 1e-9
        )
        sk["LineBoost"] = line_xgf_norm * line_boost_weight

    # PP boost
    sk["PPBoost"] = (
        sk["IsPP1"].fillna(0) * 1.0
        + sk["IsPP2"].fillna(0) * 0.5
    ) * pp_boost_weight

    # 4. Opponent defensive adjustment
    if "Opp_xGA60" in sk.columns:
        opp_def = sk["Opp_xGA60"].fillna(sk["Opp_xGA60"].median())
        opp_def_norm = (opp_def - opp_def.min()) / (
            opp_def.max() - opp_def.min() + 1e-9
        )
        sk["OpponentAdj"] = opp_def_norm * opponent_def_weight
    else:
        sk["OpponentAdj"] = 0

    # 5. Expected fantasy projection (arbitrary but consistent scale)
    sk["ProjectionRaw"] = (
        sk["BaseShotScore"]
        + sk["ScoringScore"]
        + sk["LineBoost"]
        + sk["PPBoost"]
        + sk["OpponentAdj"]
    )

    # Scale by TOI per game (more ice = more stats)
    sk["Projection"] = sk["ProjectionRaw"] * (sk["TOI_per_game"].fillna(15) / 15.0)

    return sk


########################################
# 5. MERGE DRAFTKINGS SALARIES
########################################

def merge_dk_salaries(skater_proj_df, dk_salaries_df):
    sk = skater_proj_df.copy()
    dk = dk_salaries_df.copy()

    # Try to use NormName if you have it in both
    if "NormName" in sk.columns and "Name" in dk.columns:
        sk = sk.merge(
            dk,
            how="left",
            left_on="NormName",
            right_on="Name"
        )
    else:
        sk = sk.merge(
            dk,
            how="left",
            left_on="Player",
            right_on="Name"
        )

    # Value metric: projection per 1000 salary
    if "Salary" in sk.columns:
        sk["Value"] = sk["Projection"] / (sk["Salary"] / 1000.0)
    else:
        sk["Value"] = np.

    return sk


########################################
# 6. SIMPLE ADP RANKINGS
########################################

def build_adp_rankings(skater_proj_df):
    adp = skater_proj_df.copy()

    # Rank by projection
    adp["ProjRank"] = adp["Projection"].rank(ascending=False, method="min")

    # Optional: add "consistency" later (rolling std, etc.)
    adp["ADPScore"] = adp["Projection"]
    adp["ADPRank"] = adp["ADPScore"].rank(ascending=False, method="min")

    adp = adp.sort_values("ADPRank")

    return adp[[
        "Player",
        "Team",
        "Position",
        "Projection",
        "ADPScore",
        "ADPRank"
    ]]


########################################
# 7. WRITE OUTPUTS (CSV + EXCEL)
########################################

def write_projection_outputs(
    skater_proj_df,
    goalie_proj_df,
    adp_df,
    lines_df,
    team_stats_df,
    base_path="data/outputs"
):
    import os

    os.makedirs(base_path, exist_ok=True)

    skater_csv = os.path.join(base_path, "skater_projections.csv")
    goalie_csv = os.path.join(base_path, "goalie_projections.csv")
    adp_csv = os.path.join(base_path, "ADP_rankings.csv")
    lines_csv = os.path.join(base_path, "line_combinations.csv")
    team_csv = os.path.join(base_path, "team_stats.csv")
    excel_path = os.path.join(base_path, "nhl_projections_full.xlsx")

    skater_proj_df.to_csv(skater_csv, index=False)
    goalie_proj_df.to_csv(goalie_csv, index=False)
    adp_df.to_csv(adp_csv, index=False)
    lines_df.to_csv(lines_csv, index=False)
    team_stats_df.to_csv(team_csv, index=False)

    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        skater_proj_df.to_excel(writer, sheet_name="SkaterProjections", index=False)
        goalie_proj_df.to_excel(writer, sheet_name="GoalieProjections", index=False)
        adp_df.to_excel(writer, sheet_name="ADP", index=False)
        lines_df.to_excel(writer, sheet_name="Lines", index=False)
        team_stats_df.to_excel(writer, sheet_name="TeamStats", index=False)
