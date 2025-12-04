# main.py
# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script (full)
Outputs: data/dfs_projections.csv, data/goalies.csv, data/top_stacks.csv and Excel
"""

import os
import time
import json
import requests
import pandas as pd
from datetime import date, datetime

# --- ADP NHL baseline + lineups helpers (assume these exist in your package) ---
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

today = date.today()
year, month = today.year, today.month
if month < 7:
    start, end = year - 1, year
else:
    start, end = year, year + 1

CURR_SEASON = f"{start}{end}"
LAST_SEASON = f"{start-1}{start}"

LEAGUE_AVG_SV = 0.905
GOALIE_TOI_MIN = 60.0

# Defaults / fallbacks (kept in sync with scraper defaults)
FALLBACK_SF60, FALLBACK_xGF60 = 31.0, 2.95
FALLBACK_CA60, FALLBACK_xGA60 = 58.0, 2.65

SETTINGS = {
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3},
    "LineMult_byType": {
        "5V5 LINE 1": 1.12, "5V5 LINE 2": 1.04, "5V5 LINE 3": 0.97, "5V5 LINE 4": 0.92,
        "D PAIR 1": 1.05, "D PAIR 2": 1.00, "D PAIR 3": 0.96,
        "PP1": 1.12, "PP2": 1.03, "PK1": 0.92, "PK2": 0.95
    },
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.2, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
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
    """
    Flexible loader for DraftKings CSV formats.
    Tries to read common DK CSV shapes and returns DataFrame with:
    Name, TeamAbbrev, Position, Salary, NormName
    """
    path = os.path.join(DATA_DIR, "dk_salaries.csv")
    if not os.path.exists(path):
        # try common alternate filenames (uploaded by user)
        for fname in os.listdir(DATA_DIR):
            if "dk" in fname.lower() and "salary" in fname.lower() and fname.lower().endswith(".csv"):
                path = os.path.join(DATA_DIR, fname)
                break
    if not os.path.exists(path):
        print("❌ Missing dk_salaries.csv. Download from DraftKings and save to /data/")
        return pd.DataFrame()

    # read trying common encodings/separators
    try:
        df = pd.read_csv(path)
    except Exception:
        try:
            df = pd.read_csv(path, sep=";", engine="python")
        except Exception as e:
            print(f"❌ Failed to read dk_salaries: {e}")
            return pd.DataFrame()

    # If file includes header rows above data, attempt to find starting row by searching for a column header
    if "Name" not in df.columns and "name" not in [c.lower() for c in df.columns]:
        # attempt to locate header row (first row with 'Name' text)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        header_row_idx = None
        for i, line in enumerate(lines[:20]):
            if "Name" in line or "Position" in line and "Salary" in line:
                header_row_idx = i
                break
        if header_row_idx is not None:
            try:
                df = pd.read_csv(path, header=header_row_idx)
            except Exception:
                pass

    ren = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "name": ren[c] = "Name"
        if lc in ("teamabbrev", "team"): ren[c] = "TeamAbbrev"
        if lc == "position": ren[c] = "Position"
        if "salary" in lc: ren[c] = "Salary"
    df = df.rename(columns=ren)

    # If the sheet matches the DK structure you described (players start ~ 8th row and salary ~ 10th column),
    # we already attempted header detection above. If not, we attempt heuristics:
    if "Name" not in df.columns:
        # try to pick a column that has many realistic names (contains space and letters)
        for c in df.columns:
            sample = df[c].astype(str).dropna().head(50).tolist()
            if sum(1 for s in sample if len(s.split()) >= 2) > 10:
                df = df.rename(columns={c: "Name"})
                break

    required = ["Name","TeamAbbrev","Position","Salary"]
    missing = [r for r in required if r not in df.columns]
    if missing:
        print("❌ dk_salaries.csv missing columns:", missing)
        # return partial DataFrame with what we have
        df["NormName"] = df.get("Name", "").astype(str).apply(norm_name)
        return df

    # normalize
    df["NormName"] = df["Name"].astype(str).apply(norm_name)
    # ensure Salary numeric
    df["Salary"] = pd.to_numeric(df["Salary"].astype(str).str.replace("[^0-9]", "", regex=True), errors="coerce").fillna(0).astype(int)
    return df[["Name","TeamAbbrev","Position","Salary","NormName"]]

# ---------------------------- NHL SCHEDULE ----------------------------
def get_today_schedule():
    """
    Pull today's NHL schedule. Try the new api-web endpoint first, then fall back to statsapi.web.nhl.com.
    Returns DataFrame with columns Home, Away. Empty DF if none.
    """
    date_str = datetime.today().strftime("%Y-%m-%d")

    endpoints = [
        f"https://api-web.nhle.com/v1/schedule/{date_str}",  # new-ish
        f"https://statsapi.web.nhl.com/api/v1/schedule?date={date_str}",  # classic
    ]
    last_exc = None
    for endpoint in endpoints:
        try:
            resp = requests.get(endpoint, timeout=15)
            resp.raise_for_status()
            js = resp.json()
            games = []
            # handle different json schemas
            if "gameWeek" in js:
                # new nhle schema
                gw = js.get("gameWeek", [])
                if gw:
                    glist = gw[0].get("games", [])
                    for g in glist:
                        home = g.get("homeTeam", {}).get("abbrev") or g.get("homeTeam", {}).get("triCode")
                        away = g.get("awayTeam", {}).get("abbrev") or g.get("awayTeam", {}).get("triCode")
                        if home and away:
                            games.append({"Home": home, "Away": away})
            elif "dates" in js:
                # statsapi schema
                for d in js.get("dates", []):
                    for g in d.get("games", []):
                        home = g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation") or g.get("teams", {}).get("home", {}).get("team", {}).get("triCode")
                        away = g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation") or g.get("teams", {}).get("away", {}).get("team", {}).get("triCode")
                        # fallback: use team id -> map later if needed
                        if not home:
                            try:
                                home = g["teams"]["home"]["team"]["name"][:3].upper()
                            except Exception:
                                home = None
                        if not away:
                            try:
                                away = g["teams"]["away"]["team"]["name"][:3].upper()
                            except Exception:
                                away = None
                        if home and away:
                            games.append({"Home": home, "Away": away})
            else:
                # try other plausible shapes
                if isinstance(js, dict):
                    # try to find any games list
                    maybe_games = js.get("games") or js.get("data") or js.get("game")
                    if isinstance(maybe_games, list):
                        for g in maybe_games:
                            home = (g.get("homeTeam") or g.get("home") or {}).get("abbrev") if isinstance(g, dict) else None
                            away = (g.get("awayTeam") or g.get("away") or {}).get("abbrev") if isinstance(g, dict) else None
                            if home and away:
                                games.append({"Home": home, "Away": away})

            df = pd.DataFrame(games)
            if df.empty:
                print("ℹ️ No NHL games today (from endpoint):", endpoint)
            else:
                df.to_csv(os.path.join(DATA_DIR, "schedule_today.csv"), index=False)
            return df
        except Exception as e:
            last_exc = e
            # small backoff and try next endpoint
            time.sleep(1)
            continue

    print("⚠️ Schedule fetch failed (both endpoints). Last error:", last_exc)
    return pd.DataFrame()

def build_opp_map(schedule_df: pd.DataFrame):
    opp = {}
    for _, g in schedule_df.iterrows():
        h, a = g["Home"], g["Away"]
        opp[h], opp[a] = a, h
    return opp

# ---------------------------- LINE ASSIGNMENTS (Option A) ----------------------------
def get_all_lines(schedule_df):
    """
    Pull line/shift assignment data for today's teams from the lineups endpoint (Option A).
    This function is defensive: accepts a few JSON shapes and always returns DataFrame with:
    Team, Assignment, PlayerRaw, NormName
    """
    if schedule_df is None or schedule_df.empty:
        return pd.DataFrame(columns=["Team","Assignment","PlayerRaw","NormName"])

    # gather unique team abbreviations playing today
    games = pd.concat([schedule_df["Home"], schedule_df["Away"]]).unique()
    all_rows = []

    for team in games:
        try:
            # example: https://vhd27npae1.execute-api.us-east-1.amazonaws.com/lineups/{team}
            url = f"https://vhd27npae1.execute-api.us-east-1.amazonaws.com/lineups/{team}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            js = resp.json()

            # defensive: sometimes the top-level is a list or dict
            if isinstance(js, list):
                # try to find the first dict that looks like line data
                js = js[0] if js else {}

            # collect forwards and defense lists under possibly different keys
            forwards = js.get("forwards") or js.get("fwd") or js.get("forwardsLines") or []
            defense = js.get("defense") or js.get("def") or js.get("defensePairs") or []
            goalies = js.get("goalies") or js.get("goalie") or []

            # some endpoints return a 'lines' list with nested dictionaries
            if not forwards and "lines" in js and isinstance(js["lines"], list):
                for item in js["lines"]:
                    if not isinstance(item, dict):
                        continue
                    # each item may contain 'type' and 'players'
                    atype = item.get("type") or item.get("line") or item.get("situation")
                    players = item.get("players") or item.get("names") or []
                    for p in players:
                        name = p.get("name") if isinstance(p, dict) else str(p)
                        all_rows.append({"Team": team, "Assignment": atype or "LINE", "PlayerRaw": name, "NormName": norm_name(name)})

            # standard forwards/defense handling
            for line in forwards + defense:
                # line may be dict ('line': '5V5 LINE 1', 'players':[...]) or a list of dicts
                if isinstance(line, dict):
                    assignment = line.get("line") or line.get("type") or line.get("situation") or "LINE"
                    players = line.get("players") or line.get("playerList") or []
                    for p in players:
                        if isinstance(p, dict):
                            name = p.get("name") or p.get("player") or ""
                        else:
                            name = str(p)
                        all_rows.append({"Team": team, "Assignment": assignment, "PlayerRaw": name, "NormName": norm_name(name)})
                elif isinstance(line, list):
                    # a list of player dicts or names
                    for p in line:
                        if isinstance(p, dict):
                            name = p.get("name") or p.get("player") or ""
                        else:
                            name = str(p)
                        all_rows.append({"Team": team, "Assignment": "LINE", "PlayerRaw": name, "NormName": norm_name(name)})
                else:
                    # unknown shape: string maybe
                    continue

            # goalies
            for g in goalies:
                if isinstance(g, dict):
                    name = g.get("name") or g.get("player") or ""
                else:
                    name = str(g)
                all_rows.append({"Team": team, "Assignment": "Goalie", "PlayerRaw": name, "NormName": norm_name(name)})

            # small sleep to be polite
            time.sleep(SETTINGS["sleep_lines"])

        except Exception as e:
            print(f"⚠️ Failed to fetch lines for {team}: {e}")
            continue

    df = pd.DataFrame(all_rows)
    # make sure expected columns exist
    if df.empty:
        return pd.DataFrame(columns=["Team","Assignment","PlayerRaw","NormName"])
    # unify assignment text
    df["Assignment"] = df["Assignment"].astype(str).str.strip().str.upper()
    df.to_csv(os.path.join(DATA_DIR, "lines_today.csv"), index=False)
    return df

# ---------------------------- BUILD PROJECTIONS ----------------------------
def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    players = []

    # choose source list: prefer DK salary file (current slate) else use NST players (season list)
    source_df = dk_df if (dk_df is not None and not dk_df.empty) else (nst_df if nst_df is not None else pd.DataFrame())
    if source_df is None or source_df.empty:
        print("⚠️ No source players available for building skaters.")
        return pd.DataFrame()

    # ensure team_stats has Team column
    if "Team" not in team_stats.columns:
        print("⚠️ Warning: Team column missing in team_stats, using fallbacks")
        team_stats = team_stats.copy()
        team_stats["Team"] = team_stats.get("Team", team_stats.index.astype(str))

    # lines_df guard
    if lines_df is None:
        lines_df = pd.DataFrame(columns=["Team","Assignment","PlayerRaw","NormName"])
    # ensure NormName exists in lines_df
    if "NormName" not in lines_df.columns:
        lines_df["NormName"] = lines_df["PlayerRaw"].astype(str).apply(norm_name)

    for _, row in source_df.iterrows():
        nm = row.get("NormName") or norm_name(row.get("Name") or row.get("PlayerRaw") or "")
        team = row.get("TeamAbbrev") if "TeamAbbrev" in row else row.get("Team") or row.get("team") or ""
        pos = row.get("Position", "F")
        if isinstance(team, float) and pd.isna(team):
            team = ""
        opp = opp_map.get(team) if team else None

        # find NST per-60 row
        nst_row = pd.DataFrame()
        try:
            if nst_df is not None and not nst_df.empty:
                nst_row = nst_df[nst_df["NormName"] == nm].head(1)
        except Exception:
            nst_row = pd.DataFrame()

        if not nst_row.empty:
            # reading via pandas .get may not work; access by column key safely
            g60 = safe_get_from_series(nst_row.iloc[0], ["G/60","G60","G"])
            a60 = safe_get_from_series(nst_row.iloc[0], ["A/60","A60","A"])
            s60 = safe_get_from_series(nst_row.iloc[0], ["SOG/60","S/60","SOG","S"])
            b60 = safe_get_from_series(nst_row.iloc[0], ["BLK/60","BLK","BLK/60"])
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role == "D" else SETTINGS["F_fallback"]
            g60,a60,s60,b60 = fb["G60"],fb["A60"],fb["SOG60"],fb["BLK60"]

        # team defensive context
        opp_stats = team_stats[team_stats.Team == opp] if (opp is not None and not team_stats.empty) else pd.DataFrame()
        sog_factor = (float(opp_stats["SF/60"].values[0]) / FALLBACK_SF60) if (not opp_stats.empty and "SF/60" in opp_stats.columns and pd.notna(opp_stats["SF/60"].values[0])) else 1.0
        xga_factor = (float(opp_stats["xGA/60"].values[0]) / FALLBACK_xGA60) if (not opp_stats.empty and "xGA/60" in opp_stats.columns and pd.notna(opp_stats["xGA/60"].values[0])) else 1.0

        # line multiplier from lines_df
        line_row = lines_df[(lines_df.NormName == nm) & (lines_df.Team == team)]
        line_info = line_row["Assignment"].iloc[0] if not line_row.empty else "NA"
        line_mult = SETTINGS["LineMult_byType"].get(line_info, 1.0)

        proj_goals  = (g60 or 0.0) * xga_factor * line_mult
        proj_assists= (a60 or 0.0) * xga_factor * line_mult
        proj_sog    = (s60 or 0.0) * sog_factor * line_mult
        proj_blocks = (b60 or 0.0) * line_mult

        dk_points = (proj_goals*SETTINGS["DK_points"]["Goal"] +
                     proj_assists*SETTINGS["DK_points"]["Assist"] +
                     proj_sog*SETTINGS["DK_points"]["SOG"] +
                     proj_blocks*SETTINGS["DK_points"]["Block"])

        player_dict = {
            "Player": row.get("Name") or row.get("PlayerRaw") or nm,
            "Team": team,
            "Opponent": opp or "",
            "Position": pos,
            "Line": line_info,
            "Proj Goals": round(proj_goals, 3),
            "Proj Assists": round(proj_assists, 3),
            "Proj SOG": round(proj_sog, 3),
            "Proj Blocks": round(proj_blocks, 3),
            "DK Points": round(dk_points, 3)
        }

        if (dk_df is not None) and ("Salary" in row and pd.notna(row["Salary"])):
            player_dict["Salary"] = int(row["Salary"])
            player_dict["DFS Value Score"] = round((dk_points / (row["Salary"] or 1)) * 1000, 3)

        players.append(player_dict)

    out_df = pd.DataFrame(players)
    out_df.to_csv(os.path.join(DATA_DIR, "dfs_projections.csv"), index=False)
    return out_df

def safe_get_from_series(s, keys):
    for k in keys:
        if k in s and pd.notna(s[k]):
            try:
                return float(s[k])
            except Exception:
                try:
                    return float(str(s[k]).replace(",",""))
                except Exception:
                    return None
    return None

def build_goalies(goalie_df, team_stats, opp_map):
    goalies = []
    if goalie_df is None or goalie_df.empty:
        print("⚠️ No goalie stats found, skipping goalie projections...")
        return pd.DataFrame()

    # ensure Team column in team_stats for lookups
    if "Team" not in team_stats.columns:
        team_stats["Team"] = team_stats.get("Team", team_stats.index.astype(str))

    for _, row in goalie_df.iterrows():
        pname = row.get("PlayerRaw") or row.get("NormName")
        team = row.get("Team") or ""
        # if team not present in goalie_df, try to infer from players file (not implemented here)
        if not team:
            # best-effort: skip if no team
            continue
        opp = opp_map.get(team, "")
        # Use blended SV% if available (season/recent/last)
        sv_season = safe_get_from_series(row, ["SV_season","SV%","SV_season"])
        sv_recent = safe_get_from_series(row, ["SV_recent"])
        sv_last   = safe_get_from_series(row, ["SV_last"])
        # Blend: prefer recent (weight 0.6), then season 0.3, last 0.1
        sv_pct = ( (sv_recent or 0)*0.6 + (sv_season or LEAGUE_AVG_SV)*0.3 + (sv_last or 0)*0.1 )
        if sv_pct == 0:
            sv_pct = LEAGUE_AVG_SV

        opp_stats = team_stats[team_stats.Team == opp] if (not team_stats.empty and opp) else pd.DataFrame()
        opp_sf = (opp_stats["SF/60"].values[0] if (not opp_stats.empty and "SF/60" in opp_stats.columns and pd.notna(opp_stats["SF/60"].values[0])) else FALLBACK_SF60)

        # Project saves roughly: opp shots for * sv_pct * game_minutes/60
        proj_saves = opp_sf * sv_pct
        proj_ga    = opp_sf * (1 - sv_pct)
        # example DK scoring for goalies (you may tune)
        dk_points  = proj_saves * 0.7 - proj_ga * 3.5

        goalie_dict = {
            "Goalie": pname,
            "Team": team,
            "Opponent": opp,
            "Proj Saves": round(proj_saves, 2),
            "Proj GA": round(proj_ga, 2),
            "DK Points": round(dk_points, 3)
        }
        goalies.append(goalie_dict)

    df = pd.DataFrame(goalies)
    df.to_csv(os.path.join(DATA_DIR, "goalies.csv"), index=False)
    return df

def build_stacks(dfs_proj):
    stacks = []
    if dfs_proj is None or dfs_proj.empty:
        print("⚠️ No skater projections available, skipping stacks...")
        return pd.DataFrame()

    for team, grp in dfs_proj.groupby("Team"):
        for line, players in grp.groupby("Line"):
            if not line or line == "NA": continue
            pts = players["DK Points"].sum()
            stack_dict = {
                "Team": team,
                "Line": line,
                "Players": ", ".join(players["Player"].astype(str).tolist()),
                "ProjPts": round(pts, 3)
            }
            if "Salary" in players.columns:
                cost = players["Salary"].sum()
                val = (pts / cost * 1000) if cost > 0 else 0
                stack_dict["Cost"] = int(cost)
                stack_dict["StackValue"] = round(val, 3)
            stacks.append(stack_dict)

    df = pd.DataFrame(stacks)
    df.to_csv(os.path.join(DATA_DIR, "top_stacks.csv"), index=False)
    return df

# ---------------------------- MAIN ----------------------------
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

    # load DK salaries (optional)
    dk_df = load_dk_salaries()

    # schedule
    schedule_df = get_today_schedule()
    if schedule_df.empty:
        # still continue if user wants outputs from past data, but here we stop
        print("ℹ️ No schedule entries for today; stopping.")
        return
    opp_map = build_opp_map(schedule_df)

    # NST team stats
    print("📊 Fetching NST team stats...")
    team_stats = nst_scraper.get_team_stats(CURR_SEASON)
    if team_stats.empty:
        print("⚠️ team_stats empty - projections will use fallbacks.")

    # NST player stats (use schedule teams to limit calls)
    print("📊 Fetching NST skater stats...")
    nst_players = []
    # pull last-10 and season splits for teams playing today
    teams_today = pd.unique(schedule_df[["Home","Away"]].values.ravel())
    for team in teams_today:
        try:
            nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON, tgp=10))
            nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON))
        except Exception as e:
            print(f"⚠️ NST skater fetch failed for {team}: {e}")
    if nst_players:
        try:
            nst_df = pd.concat(nst_players, ignore_index=True).drop_duplicates(subset=["NormName"], keep="first")
        except Exception:
            nst_df = pd.DataFrame()
    else:
        nst_df = pd.DataFrame()

    print("📊 Fetching NST goalie stats...")
    goalie_df = nst_scraper.get_goalies(CURR_SEASON)

    print("📊 Fetching line assignments...")
    lines_df = get_all_lines(schedule_df)

    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

    # Save Excel workbook
    try:
        print("📊 Exporting results to Excel...")
        output_path = os.path.join(DATA_DIR, f"projections_{datetime.today().strftime('%Y%m%d')}.xlsx")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            dfs_proj.to_excel(writer, sheet_name="Skaters", index=False)
            goalie_proj.to_excel(writer, sheet_name="Goalies", index=False)
            stack_proj.to_excel(writer, sheet_name="Stacks", index=False)
            team_stats.to_excel(writer, sheet_name="Teams", index=False)
            nst_df.to_excel(writer, sheet_name="NST_Raw", index=False)
        print(f"✅ Excel workbook ready: {output_path}")
    except Exception as e:
        print("⚠️ Excel export failed:", e)

    # Upload to Google Sheets (optional — requires GCP_CREDENTIALS env)
    try:
        print("📤 Uploading projections to Google Sheets...")
        tabs = {
            "Skaters": dfs_proj,
            "Goalies": goalie_proj,
            "Stacks": stack_proj,
            "Teams": team_stats,
            "NST_Raw": nst_df
        }
        upload_to_sheets("ADP NHL Projections", tabs)
    except Exception as e:
        print("⚠️ Upload to Google Sheets failed:", e)

if __name__=="__main__":
    main()
