# -*- coding: utf-8 -*-
"""
ADP NHL DFS / Betting Model - Master Script
Outputs: dfs_projections.csv, goalies.csv, top_stacks.csv (+ helper snapshots)
This version is resilient to:
- NST layout changes (team table cols missing)
- Lines API returning unexpected list/dict shapes
- Missing DraftKings salary file (will still project)
- DraftKings "wide" export (header ~8 rows down)
"""

import os, re, glob, time, requests
import pandas as pd
from datetime import date, datetime

# ---------------------------- CONFIG ----------------------------
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (ADP Free Model)"}
TIMEOUT = 60

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

# League fallback averages (used if NST not available)
FALLBACK_SF60, FALLBACK_xGF60 = 31.0, 2.95
FALLBACK_CA60, FALLBACK_xGA60 = 58.0, 2.65

SETTINGS = {
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3},
    "LineMult_byType": {
        # keep conservative bumps
        "5V5 LINE 1": 1.12, "5V5 LINE 2": 1.04, "5V5 LINE 3": 0.97, "5V5 LINE 4": 0.92,
        "D PAIR 1": 1.05, "D PAIR 2": 1.00, "D PAIR 3": 0.96,
        "PP1": 1.12, "PP2": 1.03, "PK1": 0.92, "PK2": 0.95
    },
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.2, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
    "sleep_lines": 0.7
}

# ---------------------------- UTILITIES ----------------------------
def norm_name(s: str) -> str:
    s = str(s or "").lower()
    s = re.sub(r"[^a-z\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def safe_read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        try:
            return pd.read_csv(path, sep=";", **kwargs)
        except Exception:
            return pd.DataFrame()

# ---------------------------- DRAFTKINGS ----------------------------
def _parse_dk_simple_csv(path: str) -> pd.DataFrame:
    df = safe_read_csv(path)
    if df.empty:
        return df
    # unify column names if possible
    ren = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "name": ren[c] = "Name"
        if lc == "teamabbrev": ren[c] = "TeamAbbrev"
        if lc == "position": ren[c] = "Position"
        if lc == "salary": ren[c] = "Salary"
        if lc == "avgpointspregame" or lc == "avgpointspergame": ren[c] = "AvgPointsPerGame"
    df = df.rename(columns=ren)
    cols = [c for c in ["Name","TeamAbbrev","Position","Salary","AvgPointsPerGame"] if c in df.columns]
    if "Name" in df.columns:
        df["NormName"] = df["Name"].apply(norm_name)
    return df[cols + (["NormName"] if "Name" in df.columns else [])]

def _parse_dk_wide_export(path: str) -> pd.DataFrame:
    """
    Your note: players start ~8 rows down; columns around col 12 include:
    Position, Name + ID, Name, ID, Roster Position, Salary, Game Info, TeamAbbrev, AvgPointsPerGame
    We'll read with no header, drop first 7 rows, then assign names by position if found.
    """
    raw = safe_read_csv(path, header=None)
    if raw.empty:
        return pd.DataFrame()
    # find header row by searching for 'Name' and 'Salary'
    header_idx = None
    for i in range(min(len(raw), 15)):
        row_lower = raw.iloc[i].astype(str).str.lower().tolist()
        if any("name" == x for x in row_lower) and any("salary" == x for x in row_lower):
            header_idx = i
            break
    if header_idx is None:
        # fallback to typical "skip 7" heuristic
        header_idx = 7 if len(raw) > 8 else 0

    df = safe_read_csv(path, header=header_idx)
    if df.empty:
        return pd.DataFrame()

    # unify names
    ren_map = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in ("name", "player name"):
            ren_map[c] = "Name"
        elif "team" in lc and "abbr" in lc:
            ren_map[c] = "TeamAbbrev"
        elif lc in ("position", "pos"):
            ren_map[c] = "Position"
        elif "salary" in lc:
            ren_map[c] = "Salary"
        elif lc in ("avgpointspergame", "avg points per game", "fppg"):
            ren_map[c] = "AvgPointsPerGame"
    df = df.rename(columns=ren_map)

    # If DK wide has split "Name + ID", prefer plain "Name" if present
    if "Name" not in df.columns:
        # try "Name + ID" like "Player Name (123456)"
        name_like = [c for c in df.columns if "name" in str(c).lower()]
        if name_like:
            col = name_like[0]
            df["Name"] = df[col].astype(str).str.replace(r"\s*\(\d+\)\s*$", "", regex=True)

    # ensure required
    needed = ["Name","TeamAbbrev","Position","Salary"]
    if not all(col in df.columns for col in needed):
        keep = [c for c in df.columns if c in needed + ["AvgPointsPerGame"]]
        if not keep:
            return pd.DataFrame()
        df = df[keep]

    # clean salary (strip $ and commas)
    if "Salary" in df.columns:
        df["Salary"] = (
            df["Salary"]
            .astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .replace("", pd.NA)
            .astype("float")
        )

    df["NormName"] = df["Name"].apply(norm_name)
    cols = ["Name","TeamAbbrev","Position","Salary","AvgPointsPerGame","NormName"]
    return df[[c for c in cols if c in df.columns]]

def load_dk_salaries() -> pd.DataFrame:
    """
    Accept either:
    - data/dk_salaries.csv (simple export)
    - data/DKSalaries*.csv (wide DK download with headers embedded)
    """
    # 1) Wide format first (your usual “DKSalaries <date>.csv”)
    wide_candidates = sorted(glob.glob(os.path.join(DATA_DIR, "DKSalaries*.csv")))
    for path in wide_candidates:
        parsed = _parse_dk_wide_export(path)
        if not parsed.empty:
            print(f"✅ Loaded DraftKings salaries (wide): {os.path.basename(path)} rows={len(parsed)}")
            return parsed

    # 2) Simple csv fallback
    simple_path = os.path.join(DATA_DIR, "dk_salaries.csv")
    if os.path.exists(simple_path):
        parsed = _parse_dk_simple_csv(simple_path)
        if not parsed.empty:
            print(f"✅ Loaded DraftKings salaries (simple): dk_salaries.csv rows={len(parsed)}")
            return parsed

    print("❌ Missing DraftKings salaries file. Place a DKSalaries*.csv in /data or dk_salaries.csv")
    return pd.DataFrame()

# ---------------------------- NHL SCHEDULE ----------------------------
def get_today_schedule() -> pd.DataFrame:
    """
    Pull today's NHL schedule from api-web.nhle.com (stable).
    """
    date_str = datetime.today().strftime("%Y-%m-%d")
    url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            js = r.json()
            games = []
            for wk in js.get("gameWeek", []):
                for g in wk.get("games", []):
                    home = g.get("homeTeam", {}).get("abbrev")
                    away = g.get("awayTeam", {}).get("abbrev")
                    if home and away:
                        games.append({"Home": home, "Away": away})
            df = pd.DataFrame(games)
            if df.empty:
                print("ℹ️ No NHL games on the schedule today.")
            df.to_csv(os.path.join(DATA_DIR, "schedule_today.csv"), index=False)
            return df
        except Exception as e:
            print(f"⚠️ Schedule fetch attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return pd.DataFrame()

def build_opp_map(schedule_df: pd.DataFrame):
    opp = {}
    for _, g in schedule_df.iterrows():
        h, a = g["Home"], g["Away"]
        opp[h], opp[a] = a, h
    return opp

# ---------------------------- LINE ASSIGNMENTS ----------------------------
def get_all_lines(schedule_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pull line assignments from lineups API for today's teams.
    Resilient to list/dict response shapes. Falls back to CSV if present.
    Expected output columns: Team, Assignment, PlayerRaw, NormName
    """
    fallback_lines = os.path.join(DATA_DIR, "lines.csv")
    if os.path.exists(fallback_lines):
        df = safe_read_csv(fallback_lines)
        # Normalize likely column names
        # Try to find player-name column
        name_col = None
        for cand in ["Player", "player", "name", "Name", "PlayerRaw"]:
            if cand in df.columns:
                name_col = cand
                break
        if name_col:
            df["PlayerRaw"] = df[name_col]
        if "Team" not in df.columns:
            # try a few common keys
            for cand in ["team", "TeamAbbrev", "TeamCode"]:
                if cand in df.columns:
                    df["Team"] = df[cand]
                    break
        if "Assignment" not in df.columns:
            df["Assignment"] = "NA"
        df["NormName"] = df["PlayerRaw"].apply(norm_name)
        return df[["Team","Assignment","PlayerRaw","NormName"]].copy()

    if schedule_df.empty:
        return pd.DataFrame(columns=["Team","Assignment","PlayerRaw","NormName"])

    games = pd.unique(pd.concat([schedule_df["Home"], schedule_df["Away"]], ignore_index=True))
    all_rows = []
    for team in games:
        url = f"https://vhd27npae1.execute-api.us-east-1.amazonaws.com/lineups/{team}"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            js = resp.json()

            # API sometimes returns a list; use first item if so
            if isinstance(js, list) and js:
                js = js[0]
            if not isinstance(js, dict):
                raise ValueError("Lineups API returned unexpected structure")

            fwds = js.get("forwards", []) or []
            defs = js.get("defense", []) or []
            gls  = js.get("goalies", []) or []

            # normalize skaters
            for section in fwds + defs:
                line_name = section.get("line") or section.get("name") or "NA"
                for player in section.get("players", []) or []:
                    pname = player.get("name") or ""
                    all_rows.append({
                        "Team": team,
                        "Assignment": line_name,
                        "PlayerRaw": pname,
                        "NormName": norm_name(pname)
                    })
            # normalize goalies
            for g in gls:
                pname = g.get("name") or ""
                all_rows.append({
                    "Team": team,
                    "Assignment": "Goalie",
                    "PlayerRaw": pname,
                    "NormName": norm_name(pname)
                })
        except Exception as e:
            print(f"⚠️ Failed to fetch lines for {team}: {e}")
        time.sleep(SETTINGS["sleep_lines"])

    return pd.DataFrame(all_rows, columns=["Team","Assignment","PlayerRaw","NormName"])

# ---------------------------- NST ----------------------------
# Use your existing module; we guard downstream in case it returns empty/changed
try:
    from adp_nhl.utils import nst_scraper
except Exception:
    nst_scraper = None

# ---------------------------- BUILDERS ----------------------------
def guess_role(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    players = []

    # Ensure lines has NormName even if empty
    if lines_df is None or lines_df.empty:
        lines_df = pd.DataFrame(columns=["Team","Assignment","PlayerRaw","NormName"])
    if "NormName" not in lines_df.columns:
        # try to build it from a likely player column
        cand = None
        for c in ["PlayerRaw","Player","name","Name"]:
            if c in lines_df.columns:
                cand = c
                break
        if cand is not None:
            lines_df["NormName"] = lines_df[cand].apply(norm_name)
        else:
            lines_df["NormName"] = ""

    # team_stats guards (NST sometimes breaks)
    if team_stats is None or team_stats.empty or "Team" not in team_stats.columns:
        print("⚠️ Using league-average team rates (NST team table missing).")
        team_stats = pd.DataFrame({"Team": [], "SF/60": [], "xGA/60": []})

    source_df = dk_df if not dk_df.empty else nst_df.copy()
    if dk_df.empty:
        print("⚠️ DK salaries not found, building projections without salaries...")

    for _, row in source_df.iterrows():
        nm   = row.get("NormName")
        team = row.get("TeamAbbrev") if "TeamAbbrev" in row else row.get("Team")
        pos  = row.get("Position", "F")
        opp  = opp_map.get(team)

        # per-60 from NST if available
        nst_row = nst_df[nst_df.get("NormName", pd.Series(dtype=str)) == nm].head(1)
        if not nst_row.empty:
            g60 = nst_row.get("G/60", pd.Series([None])).values[0]
            a60 = nst_row.get("A/60", pd.Series([None])).values[0]
            s60 = nst_row.get("SOG/60", pd.Series([None])).values[0]
            b60 = nst_row.get("BLK/60", pd.Series([None])).values[0]
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role == "D" else SETTINGS["F_fallback"]
            g60,a60,s60,b60 = fb["G60"],fb["A60"],fb["SOG60"],fb["BLK60"]

        # guard NAs
        def _nz(v, fb): 
            try:
                return float(v) if pd.notna(v) else fb
            except Exception:
                return fb

        role = guess_role(pos)
        fb = SETTINGS["D_fallback"] if role == "D" else SETTINGS["F_fallback"]
        g60 = _nz(g60, fb["G60"])
        a60 = _nz(a60, fb["A60"])
        s60 = _nz(s60, fb["SOG60"])
        b60 = _nz(b60, fb["BLK60"])

        # team context
        opp_stats = team_stats[team_stats.get("Team", "") == opp] if "Team" in team_stats.columns else pd.DataFrame()
        sog_factor = (opp_stats["SF/60"].values[0] / FALLBACK_SF60) if not opp_stats.empty and "SF/60" in opp_stats else 1.0
        xga_factor = (opp_stats["xGA/60"].values[0] / FALLBACK_xGA60) if not opp_stats.empty and "xGA/60" in opp_stats else 1.0

        # line context
        line_row = lines_df[(lines_df["NormName"] == nm) & (lines_df["Team"] == team)]
        line_info = line_row["Assignment"].iloc[0] if not line_row.empty else "NA"
        line_mult = SETTINGS["LineMult_byType"].get(line_info, 1.0)

        # expected rates (per 60 → per game proxy ~ 1.0 by default; we keep as rate-based DK calc)
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

        if "Salary" in row and pd.notna(row["Salary"]):
            sal = float(row["Salary"])
            player_dict["Salary"] = sal
            player_dict["DFS Value Score"] = dk_points / sal * 1000 if sal > 0 else 0

        players.append(player_dict)

    df = pd.DataFrame(players)
    df.to_csv(os.path.join(DATA_DIR, "dfs_projections.csv"), index=False)
    return df

def build_goalies(goalie_df, team_stats, opp_map):
    goalies = []
    if goalie_df is None or goalie_df.empty:
        print("⚠️ No goalie stats found, skipping goalie projections...")
        return pd.DataFrame()

    # team_stats guard
    if team_stats is None or team_stats.empty or "Team" not in team_stats.columns:
        print("⚠️ Using league-average team rates for goalies (NST team table missing).")
        team_stats = pd.DataFrame({"Team": [], "SF/60": []})

    for _, row in goalie_df.iterrows():
        team = row.get("Team", "")
        if not team:
            continue
        opp = opp_map.get(team)
        sv_pct = row.get("SV%", LEAGUE_AVG_SV)
        try:
            sv_pct = float(sv_pct) if pd.notna(sv_pct) else LEAGUE_AVG_SV
        except Exception:
            sv_pct = LEAGUE_AVG_SV

        opp_stats = team_stats[team_stats.get("Team","") == opp] if "Team" in team_stats.columns else pd.DataFrame()
        opp_sf = opp_stats["SF/60"].values[0] if not opp_stats.empty and "SF/60" in opp_stats else FALLBACK_SF60

        proj_saves = opp_sf * sv_pct
        proj_ga    = opp_sf * (1 - sv_pct)
        dk_points  = proj_saves * 0.7 - proj_ga * 3.5  # Example DK goalie scoring

        goalie_dict = {
            "Goalie": row.get("PlayerRaw", row.get("NormName")),
            "Team": team, "Opponent": opp,
            "Proj Saves": proj_saves, "Proj GA": proj_ga,
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
            stack = {
                "Team": team,
                "Line": line,
                "Players": ", ".join(players["Player"]),
                "ProjPts": pts
            }
            if "Salary" in players.columns:
                cost = players["Salary"].sum()
                val = pts / cost * 1000 if cost > 0 else 0
                stack["Cost"] = cost
                stack["StackValue"] = val
            stacks.append(stack)

    df = pd.DataFrame(stacks)
    df.to_csv(os.path.join(DATA_DIR, "top_stacks.csv"), index=False)
    return df

# ---------------------------- MAIN ----------------------------
def main():
    # --- (A) Light baseline print (kept from your earlier logs) ---
    try:
        from adp_nhl.utils.etl import ingest_baseline_if_needed
        from adp_nhl.utils.lineups_api import fetch_lineups
        from adp_nhl.utils.joins import join_lineups_with_baseline, load_processed
        from adp_nhl.utils.warnings import tag_missing_baseline, players_missing_baseline
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
    except Exception:
        print("ℹ️ Baseline helpers unavailable; continuing...")

    print("🚀 Starting ADP NHL DFS Model")

    # (1) Salaries
    dk_df = load_dk_salaries()

    # (2) Schedule & opponent map
    schedule_df = get_today_schedule()
    if schedule_df.empty:
        # We still continue, but projections will have no Opponent info.
        opp_map = {}
    else:
        opp_map = build_opp_map(schedule_df)

    # (3) Team stats (NST) with guard
    team_stats = pd.DataFrame()
    if nst_scraper is not None:
        print("📊 Fetching NST team stats...")
        try:
            ts = nst_scraper.get_team_stats(CURR_SEASON)
            # only keep columns if present; don't crash if missing
            if not ts.empty:
                keep = ["Team","SF/60","xGA/60"]
                existing = [c for c in keep if c in ts.columns]
                if existing:
                    team_stats = ts[existing + [c for c in ts.columns if c not in existing]]
                else:
                    team_stats = pd.DataFrame(columns=["Team","SF/60","xGA/60"])
            else:
                team_stats = pd.DataFrame(columns=["Team","SF/60","xGA/60"])
        except Exception as e:
            print(f"❌ Parse NST team stats failed (guarded): {e}")
            team_stats = pd.DataFrame(columns=["Team","SF/60","xGA/60"])
    else:
        print("ℹ️ nst_scraper unavailable; using league averages.")
        team_stats = pd.DataFrame(columns=["Team","SF/60","xGA/60"])

    # (4) Skater per-60 (NST) (optional)
    nst_df = pd.DataFrame()
    if nst_scraper is not None:
        try:
            print("📊 Fetching NST skater stats...")
            teams_for_pull = pd.unique(schedule_df[["Home","Away"]].values.ravel()) if not schedule_df.empty else []
            pool = []
            for t in teams_for_pull:
                pool.append(nst_scraper.get_team_players(t, CURR_SEASON, tgp=10))
                pool.append(nst_scraper.get_team_players(t, CURR_SEASON))
            if pool:
                nst_df = pd.concat(pool, ignore_index=True)
                if "NormName" not in nst_df.columns and "PlayerRaw" in nst_df.columns:
                    nst_df["NormName"] = nst_df["PlayerRaw"].apply(norm_name)
        except Exception as e:
            print(f"⚠️ NST skater stats pull failed (guarded): {e}")
            nst_df = pd.DataFrame()

    # (5) Goalies (NST) (optional)
    goalie_df = pd.DataFrame()
    if nst_scraper is not None:
        try:
            print("📊 Fetching NST goalie stats...")
            gdf = nst_scraper.get_goalies(CURR_SEASON)
            goalie_df = gdf if isinstance(gdf, pd.DataFrame) else pd.DataFrame()
        except Exception as e:
            print(f"⚠️ NST goalie stats pull failed (guarded): {e}")
            goalie_df = pd.DataFrame()

    # (6) Lines (robust)
    print("📊 Fetching line assignments...")
    lines_df = get_all_lines(schedule_df)
    # force schema
    for col in ["Team","Assignment","PlayerRaw","NormName"]:
        if col not in lines_df.columns:
            lines_df[col] = "" if col != "Assignment" else "NA"

    # (7) Build outputs
    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

    # (8) Excel bundle
    print("📊 Exporting results to Excel...")
    xls_path = os.path.join(DATA_DIR, f"projections_{datetime.today().strftime('%Y%m%d')}.xlsx")
    with pd.ExcelWriter(xls_path, engine="openpyxl") as writer:
        dfs_proj.to_excel(writer, sheet_name="Skaters", index=False)
        goalie_proj.to_excel(writer, sheet_name="Goalies", index=False)
        stack_proj.to_excel(writer, sheet_name="Stacks", index=False)
        team_stats.to_excel(writer, sheet_name="Teams", index=False)
        if not nst_df.empty:
            nst_df.to_excel(writer, sheet_name="NST_Raw", index=False)
        if not lines_df.empty:
            lines_df.to_excel(writer, sheet_name="Lines", index=False)
        if not schedule_df.empty:
            schedule_df.to_excel(writer, sheet_name="Schedule", index=False)
    print(f"✅ Excel workbook ready: {xls_path}")

    # (9) Optional Google Sheets push (only if credentials exist)
    if os.environ.get("GCP_CREDENTIALS"):
        try:
            from adp_nhl.utils.export_sheets import upload_to_sheets
            tabs = {
                "Skaters": dfs_proj,
                "Goalies": goalie_proj,
                "Stacks": stack_proj,
                "Teams": team_stats,
                "NST_Raw": nst_df,
                "Lines": lines_df,
                "Schedule": schedule_df
            }
            upload_to_sheets("ADP NHL Projections", tabs)
            print("📤 Google Sheets updated.")
        except Exception as e:
            print(f"⚠️ Sheets export skipped (error guarded): {e}")
    else:
        print("ℹ️ Skipping Google Sheets export (GCP_CREDENTIALS not set).")

if __name__ == "__main__":
    main()
