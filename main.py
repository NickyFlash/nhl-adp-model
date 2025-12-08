# ---------------------------- SCHEDULE HELPERS ----------------------------
def get_today_schedule():
    """
    Fetch today's NHL schedule from primary/backup endpoints and write schedule_today.csv.
    Expected columns in output: Home, Away (team abbreviations).
    """
    endpoints = [
        "https://adp-nhl-schedule-primary.example.com/today",   # placeholder
        "https://adp-nhl-schedule-backup.example.com/today",    # placeholder
    ]
    last_exc = None
    for endpoint in endpoints:
        try:
            resp = requests.get(endpoint, timeout=15)
            resp.raise_for_status()
            games = resp.json()
            if isinstance(games, dict):
                games = games.get("games", [])
            df = pd.DataFrame(games)
            if df.empty:
                print("ℹ️ No NHL games today (from endpoint):", endpoint)
            else:
                # Ensure expected columns exist
                col_map = {}
                if "home" in df.columns and "away" in df.columns:
                    col_map = {"home": "Home", "away": "Away"}
                elif "HOME" in df.columns and "AWAY" in df.columns:
                    col_map = {"HOME": "Home", "AWAY": "Away"}
                df = df.rename(columns=col_map)
                if "Home" not in df.columns or "Away" not in df.columns:
                    print("⚠️ Schedule dataframe missing Home/Away columns, got:", df.columns.tolist())
                df.to_csv(os.path.join(DATA_DIR, "schedule_today.csv"), index=False)
            return df
        except Exception as e:
            last_exc = e
            time.sleep(1)
            continue
    print("⚠️ Schedule fetch failed (both endpoints). Last error:", last_exc)
    return pd.DataFrame()


def build_opp_map(schedule_df: pd.DataFrame):
    """
    Build a mapping team -> opponent from schedule_df.
    Requires columns Home, Away.
    """
    opp = {}
    if schedule_df is None or schedule_df.empty:
        return opp
    for _, g in schedule_df.iterrows():
        if "Home" not in g or "Away" not in g:
            continue
        h, a = g["Home"], g["Away"]
        if pd.isna(h) or pd.isna(a):
            continue
        opp[h], opp[a] = a, h
    return opp


# ---------------------------- LINE ASSIGNMENTS (Option A) ----------------------------
def get_all_lines(schedule_df):
    """
    Pull line/shift assignment data for today's teams from the lineups endpoint (Option A).
    Always returns DataFrame with columns:
        Team, Assignment, PlayerRaw, NormName
    """
    if schedule_df is None or schedule_df.empty:
        return pd.DataFrame(columns=["Team", "Assignment", "PlayerRaw", "NormName"])

    # gather unique team abbreviations playing today
    games = pd.concat([schedule_df["Home"], schedule_df["Away"]]).unique()
    all_rows = []

    for team in games:
        try:
            url = "https://vhd27npae1.execute-api.us-east-1.amazonaws.com/lineups/" + str(team)
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            js = resp.json()

            if isinstance(js, list):
                js = js[0] if js else {}

            forwards = js.get("forwards") or js.get("fwd") or js.get("forwardsLines") or []
            defense = js.get("defense") or js.get("def") or js.get("defensePairs") or []
            goalies = js.get("goalies") or js.get("goalie") or []

            # handle generic 'lines' key
            if (not forwards) and ("lines" in js) and isinstance(js["lines"], list):
                for item in js["lines"]:
                    if not isinstance(item, dict):
                        continue
                    atype = item.get("type") or item.get("line") or item.get("situation")
                    players = item.get("players") or item.get("names") or []
                    for p in players:
                        name = p.get("name") if isinstance(p, dict) else str(p)
                        all_rows.append(
                            {
                                "Team": team,
                                "Assignment": atype or "LINE",
                                "PlayerRaw": name,
                                "NormName": norm_name(name),
                            }
                        )

            # standard forwards/defense handling
            for line in forwards + defense:
                if isinstance(line, dict):
                    assignment = (
                        line.get("line")
                        or line.get("type")
                        or line.get("situation")
                        or "LINE"
                    )
                    players = line.get("players") or line.get("playerList") or []
                    for p in players:
                        if isinstance(p, dict):
                            name = p.get("name") or p.get("player") or ""
                        else:
                            name = str(p)
                        all_rows.append(
                            {
                                "Team": team,
                                "Assignment": assignment,
                                "PlayerRaw": name,
                                "NormName": norm_name(name),
                            }
                        )
                elif isinstance(line, list):
                    for p in line:
                        if isinstance(p, dict):
                            name = p.get("name") or p.get("player") or ""
                        else:
                            name = str(p)
                        all_rows.append(
                            {
                                "Team": team,
                                "Assignment": "LINE",
                                "PlayerRaw": name,
                                "NormName": norm_name(name),
                            }
                        )
                else:
                    continue

            # goalies
            for g in goalies:
                if isinstance(g, dict):
                    name = g.get("name") or g.get("player") or ""
                else:
                    name = str(g)
                all_rows.append(
                    {
                        "Team": team,
                        "Assignment": "Goalie",
                        "PlayerRaw": name,
                        "NormName": norm_name(name),
                    }
                )

            time.sleep(SETTINGS["sleep_lines"])
        except Exception as e:
            print("⚠️ Failed to fetch lines for " + str(team) + ": " + str(e))
            continue

    df = pd.DataFrame(all_rows)

    if df.empty:
        return pd.DataFrame(columns=["Team", "Assignment", "PlayerRaw", "NormName"])

    # unify assignment text and make sure columns exist
    df["Assignment"] = df["Assignment"].astype(str).str.strip().str.upper()

    if "NormName" not in df.columns:
        df["NormName"] = df["PlayerRaw"].astype(str).apply(norm_name)

    df.to_csv(os.path.join(DATA_DIR, "lines_today.csv"), index=False)
    return df


# ---------------------------- PROJECTION HELPERS ----------------------------
def safe_get_from_series(s, keys):
    """
    Robustly try to get a float value from a pandas Series using multiple alternative keys.
    """
    for k in keys:
        if k in s and pd.notna(s[k]):
            try:
                return float(s[k])
            except Exception:
                try:
                    return float(str(s[k]).replace(",", ""))
                except Exception:
                    return None
    return None


def build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map):
    """
    Build skater projections using DK salaries (if available), NST per-60 data, team context, and line assignments.
    """

    players = []

    # choose source list: prefer DK salary file (current slate) else use NST players (season list)
    source_df = dk_df if (dk_df is not None and not dk_df.empty) else (nst_df if nst_df is not None else pd.DataFrame())
    if source_df is None or source_df.empty:
        print("⚠️ No source players available for building skaters.")
        return pd.DataFrame()

    # normalize team_stats columns
    if team_stats is None:
        team_stats = pd.DataFrame()
    else:
        team_stats = team_stats.copy()
    if "Team" not in team_stats.columns:
        team_stats["Team"] = team_stats.get("Team", team_stats.index.astype(str))
    # standardize SF/xGA columns
    col_rename = {}
    for c in team_stats.columns:
        if c.lower() in ["sf60", "sf_60", "shotsfor60", "shots_for_60"]:
            col_rename[c] = "SF/60"
        if c.lower() in ["xga60", "xga_60", "xga_per_60"]:
            col_rename[c] = "xGA/60"
    team_stats = team_stats.rename(columns=col_rename)

    # lines_df guard
    if lines_df is None:
        lines_df = pd.DataFrame(columns=["Team", "Assignment", "PlayerRaw", "NormName"])
    else:
        lines_df = lines_df.copy()

    if "NormName" not in lines_df.columns:
        lines_df["NormName"] = lines_df["PlayerRaw"].astype(str).apply(norm_name)

    # ensure NST has NormName
    if nst_df is None:
        nst_df = pd.DataFrame()
    else:
        nst_df = nst_df.copy()
    if (not nst_df.empty) and ("NormName" not in nst_df.columns):
        name_col = "Player"
        if "Player Name" in nst_df.columns:
            name_col = "Player Name"
        if "Name" in nst_df.columns:
            name_col = "Name"
        nst_df["NormName"] = nst_df[name_col].astype(str).apply(norm_name)

    for _, row in source_df.iterrows():
        nm = row.get("NormName")
        if not nm:
            nm = norm_name(row.get("Name") or row.get("PlayerRaw") or "")

        team = row.get("TeamAbbrev") if "TeamAbbrev" in row else (row.get("Team") or row.get("team") or "")
        pos = row.get("Position", "F")

        if isinstance(team, float) and pd.isna(team):
            team = ""

        opp = opp_map.get(team) if team else None

        # find NST per-60 row by NormName
        nst_row = pd.DataFrame()
        try:
            if nst_df is not None and not nst_df.empty:
                nst_row = nst_df[nst_df["NormName"] == nm].head(1)
        except Exception:
            nst_row = pd.DataFrame()

        if not nst_row.empty:
            row0 = nst_row.iloc[0]
            g60 = safe_get_from_series(row0, ["G/60", "G60", "G_per60", "G"])
            a60 = safe_get_from_series(row0, ["A/60", "A60", "A_per60", "A"])
            s60 = safe_get_from_series(row0, ["SOG/60", "S/60", "SOG60", "SOG", "S"])
            b60 = safe_get_from_series(row0, ["BLK/60", "BLK60", "BLK"])
        else:
            role = guess_role(pos)
            fb = SETTINGS["D_fallback"] if role == "D" else SETTINGS["F_fallback"]
            g60, a60, s60, b60 = fb["G60"], fb["A60"], fb["SOG60"], fb["BLK60"]

        # team defensive context
        if opp is not None and (not team_stats.empty):
            opp_stats = team_stats[team_stats["Team"] == opp]
        else:
            opp_stats = pd.DataFrame()

        if (not opp_stats.empty) and ("SF/60" in opp_stats.columns) and pd.notna(opp_stats["SF/60"].values[0]):
            sog_factor = float(opp_stats["SF/60"].values[0]) / float(FALLBACK_SF60)
        else:
            sog_factor = 1.0

        if (not opp_stats.empty) and ("xGA/60" in opp_stats.columns) and pd.notna(opp_stats["xGA/60"].values[0]):
            xga_factor = float(opp_stats["xGA/60"].values[0]) / float(FALLBACK_xGA60)
        else:
            xga_factor = 1.0

        # line multiplier from lines_df
        if team and (not lines_df.empty):
            line_row = lines_df[(lines_df["NormName"] == nm) & (lines_df["Team"] == team)]
        else:
            line_row = pd.DataFrame()

        line_info = line_row["Assignment"].iloc[0] if not line_row.empty else "NA"
        # Settings expected to have entries like "5V5 LINE 1", "5V5 LINE 2", "PP1", etc. (uppercased).
        line_info_std = str(line_info).upper().strip()
        line_mult = SETTINGS["LineMult_byType"].get(line_info_std, 1.0)

        proj_goals = (g60 or 0.0) * xga_factor * line_mult
        proj_assists = (a60 or 0.0) * xga_factor * line_mult
        proj_sog = (s60 or 0.0) * sog_factor * line_mult
        proj_blocks = (b60 or 0.0) * line_mult

        dk_points = (
            proj_goals * SETTINGS["DK_points"]["Goal"]
            + proj_assists * SETTINGS["DK_points"]["Assist"]
            + proj_sog * SETTINGS["DK_points"]["SOG"]
            + proj_blocks * SETTINGS["DK_points"]["Block"]
        )

        player_dict = {
            "Player": row.get("Name") or row.get("PlayerRaw") or nm,
            "NormName": nm,
            "Team": team,
            "Opponent": opp or "",
            "Position": pos,
            "Line": line_info_std,
            "Proj Goals": round(proj_goals, 3),
            "Proj Assists": round(proj_assists, 3),
            "Proj SOG": round(proj_sog, 3),
            "Proj Blocks": round(proj_blocks, 3),
            "DK Points": round(dk_points, 3),
        }

        if (dk_df is not None) and ("Salary" in row) and pd.notna(row["Salary"]):
            try:
                sal = int(row["Salary"])
            except Exception:
                try:
                    sal = int(str(row["Salary"]).replace(",", ""))
                except Exception:
                    sal = 0
            player_dict["Salary"] = sal
            if sal > 0:
                player_dict["DFS Value Score"] = round((dk_points / float(sal)) * 1000.0, 3)

        players.append(player_dict)

    out_df = pd.DataFrame(players)
    out_df.to_csv(os.path.join(DATA_DIR, "dfs_projections.csv"), index=False)
    return out_df


def build_goalies(goalie_df, team_stats, opp_map):
    """
    Build goalie projections using blended SV% and opponent SF/60 context.
    """

    goalies = []
    if goalie_df is None or goalie_df.empty:
        print("⚠️ No goalie stats found, skipping goalie projections...")
        return pd.DataFrame()

    if team_stats is None:
        team_stats = pd.DataFrame()
    else:
        team_stats = team_stats.copy()
    if "Team" not in team_stats.columns:
        team_stats["Team"] = team_stats.get("Team", team_stats.index.astype(str))

    # normalize SF column
    col_rename = {}
    for c in team_stats.columns:
        if c.lower() in ["sf60", "sf_60", "shotsfor60", "shots_for_60"]:
            col_rename[c] = "SF/60"
    team_stats = team_stats.rename(columns=col_rename)

    for _, row in goalie_df.iterrows():
        pname = row.get("PlayerRaw") or row.get("NormName") or row.get("Player") or ""
        team = row.get("Team") or row.get("TeamAbbrev") or ""

        if not team:
            continue

        opp = opp_map.get(team, "")

        # blended SV%
        sv_season = safe_get_from_series(row, ["SV_season", "SV%", "SV_season"])
        sv_recent = safe_get_from_series(row, ["SV_recent"])
        sv_last = safe_get_from_series(row, ["SV_last"])

        sv_pct = ( (sv_recent or 0) * 0.6 + (sv_season or LEAGUE_AVG_SV) * 0.3 + (sv_last or 0) * 0.1 )
        if not sv_pct:
            sv_pct = LEAGUE_AVG_SV

        if (not team_stats.empty) and opp:
            opp_stats = team_stats[team_stats["Team"] == opp]
        else:
            opp_stats = pd.DataFrame()

        if (not opp_stats.empty) and ("SF/60" in opp_stats.columns) and pd.notna(opp_stats["SF/60"].values[0]):
            opp_sf = float(opp_stats["SF/60"].values[0])
        else:
            opp_sf = float(FALLBACK_SF60)

        proj_saves = opp_sf * sv_pct
        proj_ga = opp_sf * (1.0 - sv_pct)

        dk_points = proj_saves * 0.7 - proj_ga * 3.5

        goalie_dict = {
            "Goalie": pname,
            "Team": team,
            "Opponent": opp,
            "Proj Saves": round(proj_saves, 2),
            "Proj GA": round(proj_ga, 2),
            "DK Points": round(dk_points, 3),
        }
        goalies.append(goalie_dict)

    df = pd.DataFrame(goalies)
    df.to_csv(os.path.join(DATA_DIR, "goalies.csv"), index=False)
    return df


def build_stacks(dfs_proj):
    """
    Aggregate skater projections into stacks by Team + Line.
    """

    stacks = []
    if dfs_proj is None or dfs_proj.empty:
        print("⚠️ No skater projections available, skipping stacks...")
        return pd.DataFrame()

    for team, grp in dfs_proj.groupby("Team"):
        for line, players in grp.groupby("Line"):
            if not line or line == "NA":
                continue

            pts = players["DK Points"].sum()
            stack_dict = {
                "Team": team,
                "Line": line,
                "Players": ", ".join(players["Player"].astype(str).tolist()),
                "ProjPts": round(pts, 3),
            }

            if "Salary" in players.columns:
                cost = players["Salary"].sum()
                val = (pts / cost * 1000.0) if cost > 0 else 0
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

    # DraftKings salaries (optional but preferred source)
    dk_df = load_dk_salaries()

    # Schedule + opponent map
    schedule_df = get_today_schedule()
    if schedule_df.empty:
        print("ℹ️ No schedule entries for today; stopping.")
        return
    opp_map = build_opp_map(schedule_df)

    # NST team stats
    print("📊 Fetching NST team stats...")
    team_stats = nst_scraper.get_team_stats(CURR_SEASON)
    if team_stats.empty:
        print("⚠️ team_stats empty - projections will use fallbacks.")

    # NST player stats (skaters) – last-10 + season for teams playing today
    print("📊 Fetching NST skater stats...")
    nst_players = []
    teams_today = pd.unique(schedule_df[["Home", "Away"]].values.ravel())
    for team in teams_today:
        try:
            nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON, tgp=10))
            nst_players.append(nst_scraper.get_team_players(team, CURR_SEASON))
        except Exception as e:
            print("⚠️ NST skater fetch failed for " + str(team) + ": " + str(e))

    if nst_players:
        try:
            nst_df = pd.concat(nst_players, ignore_index=True)
            if "NormName" in nst_df.columns:
                nst_df = nst_df.drop_duplicates(subset=["NormName"], keep="first")
            else:
                # create NormName from best-available player name column then dedupe
                name_col = "Player"
                if "Player Name" in nst_df.columns:
                    name_col = "Player Name"
                if "Name" in nst_df.columns:
                    name_col = "Name"
                nst_df["NormName"] = nst_df[name_col].astype(str).apply(norm_name)
                nst_df = nst_df.drop_duplicates(subset=["NormName"], keep="first")
        except Exception:
            nst_df = pd.DataFrame()
    else:
        nst_df = pd.DataFrame()

    # NST goalie stats
    print("📊 Fetching NST goalie stats...")
    goalie_df = nst_scraper.get_goalies(CURR_SEASON)

    # Line assignments
    print("📊 Fetching line assignments...")
    lines_df = get_all_lines(schedule_df)

    # Projections
    print("🛠️ Building skater projections...")
    dfs_proj = build_skaters(dk_df, nst_df, team_stats, lines_df, opp_map)

    print("🛠️ Building goalie projections...")
    goalie_proj = build_goalies(goalie_df, team_stats, opp_map)

    print("🛠️ Building stack projections...")
    stack_proj = build_stacks(dfs_proj)

    print("✅ All outputs saved to /data")

    # Excel export
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
        print("✅ Excel workbook ready: " + output_path)
    except Exception as e:
        print("⚠️ Excel export failed:", e)

    # Google Sheets export (optional)
    try:
        print("📤 Uploading projections to Google Sheets...")
        tabs = {
            "Skaters": dfs_proj,
            "Goalies": goalie_proj,
            "Stacks": stack_proj,
            "Teams": team_stats,
            "NST_Raw": nst_df,
        }
        upload_to_sheets("ADP NHL Projections", tabs)
    except Exception as e:
        print("⚠️ Upload to Google Sheets failed:", e)


if __name__ == "__main__":
    main()
