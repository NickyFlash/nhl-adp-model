import os, io, re, csv, time, json, math, requests
from datetime import datetime, timezone
from dateutil import parser as dtp
import pandas as pd

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "").strip()
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"}

TEAMS = [
    "ANA","UTA","BOS","BUF","CGY","CAR","CHI","COL","CBJ","DAL","DET","EDM","FLA",
    "LAK","MIN","MTL","NSH","NJD","NYI","NYR","OTT","PHI","PIT","SJS","SEA","STL",
    "TBL","TOR","VAN","VGK","WSH","WPG"
]
DFO_SLUG = {
    "ANA":"ducks","UTA":"utah-mammoth","BOS":"bruins","BUF":"sabres","CGY":"flames",
    "CAR":"hurricanes","CHI":"blackhawks","COL":"avalanche","CBJ":"blue-jackets","DAL":"stars",
    "DET":"red-wings","EDM":"oilers","FLA":"panthers","LAK":"kings","MIN":"wild",
    "MTL":"canadiens","NSH":"predators","NJD":"devils","NYI":"islanders","NYR":"rangers",
    "OTT":"senators","PHI":"flyers","PIT":"penguins","SJS":"sharks","SEA":"kraken",
    "STL":"blues","TBL":"lightning","TOR":"maple-leafs","VAN":"canucks","VGK":"golden-knights",
    "WSH":"capitals","WPG":"jets"
}

os.makedirs("data", exist_ok=True)

# -----------------------------
# DraftKings salaries (Classic/fallback)
# -----------------------------
def fetch_dk_salaries():
    try:
        url = "https://www.draftkings.com/lobby/getcontests?sport=NHL"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        contests = r.json().get("Contests", [])
        def looks_classic(c):
            for k in ("ContestType","Type","contestType","ContestName","Name","name"):
                v = c.get(k)
                if isinstance(v,str) and "classic" in v.lower():
                    return True
            return False
        cand = [c for c in contests if looks_classic(c)]
        if not cand and contests:
            cand = [contests[0]]
        if not cand:
            return pd.DataFrame()
        dfs = []
        for c in cand:
            cid = c.get("ContestId")
            if not cid: continue
            csv_url = f"https://www.draftkings.com/lineup/getavailableplayerscsv?contestId={cid}"
            cr = requests.get(csv_url, headers=HEADERS, timeout=30)
            if cr.status_code != 200: continue
            df = pd.read_csv(io.StringIO(cr.text))
            df.insert(0, "Contest", c.get("ContestName",""))
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as e:
        print("DK error:", e)
        return pd.DataFrame()

# -----------------------------
# Odds API (h2h, totals, props)
# -----------------------------
def fetch_odds():
    if not ODDS_API_KEY:
        return pd.DataFrame([{"Error":"Missing ODDS_API_KEY"}])
    try:
        url = (
            "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
            f"?regions=us&markets=h2h,totals,player_points,player_assists,player_goals&oddsFormat=decimal&apiKey={ODDS_API_KEY}"
        )
        r = requests.get(url, headers=HEADERS, timeout=45)
        if r.status_code != 200:
            return pd.DataFrame([{"Error": f"{r.status_code}", "Body": r.text[:250]}])
        data = r.json()
        rows = []
        for game in data:
            matchup = f"{game.get('home_team','')} vs {game.get('away_team','')}"
            t = game.get("commence_time","")
            for bk in game.get("bookmakers",[]):
                bkey = bk.get("key","")
                for mk in bk.get("markets",[]):
                    mkey = mk.get("key","")
                    for out in mk.get("outcomes",[]):
                        rows.append({
                            "Game": matchup,
                            "Market": mkey,
                            "Outcome": out.get("name",""),
                            "Odds": out.get("price",""),
                            "Bookmaker": bkey,
                            "CommenceTimeUTC": t
                        })
        return pd.DataFrame(rows)
    except Exception as e:
        print("Odds error:", e)
        return pd.DataFrame()

# -----------------------------
# NST player map (robust + throttle)
# -----------------------------
def fetch_nst_player_map():
    rows = []
    for abbr in TEAMS:
        try:
            url = f"https://www.naturalstattrick.com/playerteams.php?team={abbr}&sit=all"
            r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code != 200 and abbr == "UTA":
                # In case NST hasn't updated the team code yet
                url = "https://www.naturalstattrick.com/playerteams.php?team=ARI&sit=all"
                r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code != 200:
                rows.append({"Player Name": f"HTTP {r.status_code}", "Team": abbr, "NST_ID":"", "NameKey":""})
                time.sleep(5); continue
            html = r.text
            pattern = re.compile(r'player(?:\.php\?id=|id=)(\d+)[^>]*>([^<]+)', re.IGNORECASE)
            matches = pattern.findall(html)
            if not matches:
                rows.append({"Player Name":"No players parsed", "Team": abbr, "NST_ID":"", "NameKey":""})
                time.sleep(5); continue
            for pid, name in matches:
                name = name.strip()
                key = re.sub(r'[\W_]+',' ',name).strip().upper()
                rows.append({"Player Name": name, "Team": abbr, "NST_ID": pid, "NameKey": f"{abbr}_{key}"})
            time.sleep(5)  # throttle to be polite
        except Exception as e:
            rows.append({"Player Name": f"ERR {e}", "Team": abbr, "NST_ID":"", "NameKey":""})
            time.sleep(5)
    df = pd.DataFrame(rows)
    good = df[df["NST_ID"].astype(str).str.len()>0].copy()
    return good

# -----------------------------
# Lines: DFO → LWL fallback
# -----------------------------
def parse_fixed_blocks(names):
    defs = [
        ("5v5 Line 1", 3), ("5v5 Line 2", 3), ("5v5 Line 3", 3), ("5v5 Line 4", 3),
        ("D Pair 1", 2), ("D Pair 2", 2), ("D Pair 3", 2),
        ("PP1", 5), ("PP2", 5),
        ("PK1", 4), ("PK2", 4),
    ]
    out, idx = [], 0
    for typ, size in defs:
        group = names[idx:idx+size]; idx += size
        if len(group) == size:
            out.append((typ, " – ".join(group)))
    return out

def fetch_lines():
    rows = []
    for abbr in TEAMS:
        got = False
        slug = DFO_SLUG.get(abbr,"")
        # Try DailyFaceoff
        try:
            if slug:
                url = f"https://www.dailyfaceoff.com/teams/{slug}/line-combinations/"
                r = requests.get(url, headers=HEADERS, timeout=45)
                if r.status_code == 200:
                    names = re.findall(r'data-player-name="([^"]+)"', r.text)
                    names = [n.strip() for n in names]
                    if names:
                        for typ, players in parse_fixed_blocks(names):
                            rows.append({"Team": abbr, "Line Type": typ, "Line ID": f"{abbr}_{typ}", "Players": players, "Source": "DFO"})
                        got = True
        except Exception as e:
            pass
        # Fallback LeftWingLock
        if not got:
            try:
                url = f"https://www.leftwinglock.com/line-combinations/team.php?team={abbr}"
                r = requests.get(url, headers=HEADERS, timeout=45)
                if r.status_code == 200:
                    names = re.findall(r'<td class="line-combination-player">([^<]+)', r.text)
                    names = [n.strip() for n in names]
                    if names:
                        for typ, players in parse_fixed_blocks(names):
                            rows.append({"Team": abbr, "Line Type": typ, "Line ID": f"{abbr}_{typ}", "Players": players, "Source": "LWL"})
                        got = True
            except Exception as e:
                pass
        if not got:
            rows.append({"Team": abbr, "Line Type": "ERROR", "Line ID": f"{abbr}_ERR", "Players": "No data (DFO/LWL)", "Source": "NONE"})
        time.sleep(2)
    return pd.DataFrame(rows)

# -----------------------------
# Helper: simple projection settings
# (neutral until NST per-60 plugged in)
# -----------------------------
SETTINGS = {
    "F_baseline": {"G60": 0.70, "A60": 0.90, "SOG60": 6.5, "BLK60": 1.2},
    "D_baseline": {"G60": 0.30, "A60": 0.80, "SOG60": 4.0, "BLK60": 4.5},
    "TOI_default": {"F": 16.0, "D": 22.0},
    "Opp_SOG_factor": 1.00,
    "Opp_xGA_factor": 1.00,
    "Opp_CA_factor": 1.00,
    "PP_Edge": 1.00,
    "LineMult_byType": {
        "5v5 Line 1": 1.08, "5v5 Line 2": 1.03, "5v5 Line 3": 0.98, "5v5 Line 4": 0.94,
        "PP1": 1.12, "PP2": 1.03
    },
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3}
}

def guess_role_from_pos(pos):
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

# -----------------------------
# Build DFS projections from DK + Lines
# -----------------------------
def build_projections(dk_df, lines_df):
    if dk_df is None or dk_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Normalize DK schema we need
    dk = dk_df.copy()
    rename_map = {}
    for c in dk.columns:
        lc = c.lower()
        if lc == "name": rename_map[c] = "Name"
        if lc == "teamabbrev": rename_map[c] = "TeamAbbrev"
        if lc == "position": rename_map[c] = "Position"
        if lc == "salary": rename_map[c] = "Salary"
    dk = dk.rename(columns=rename_map)
    need = [c for c in ("Name","TeamAbbrev","Position","Salary") if c in dk.columns]
    dk = dk[need].copy()

    # Split skaters & goalies
    sk = dk[~dk["Position"].str.contains("G", case=False, na=False)].copy()
    gk = dk[ dk["Position"].str.contains("G", case=False, na=False)].copy()

    # Prepare line-player lookup
    if lines_df is not None and not lines_df.empty:
        def split_players(s):
            return [p.strip() for p in re.split(r"–|-|,|\u2013", str(s)) if p.strip()]
        exploded = []
        for _, r in lines_df.iterrows():
            players = split_players(r.get("Players",""))
            for p in players:
                exploded.append({
                    "Team": r.get("Team",""),
                    "Line Type": r.get("Line Type",""),
                    "Line ID": r.get("Line ID",""),
                    "Player": p
                })
        line_players = pd.DataFrame(exploded)
    else:
        line_players = pd.DataFrame(columns=["Team","Line Type","Line ID","Player"])

    # SKATERS: join lines (5v5 + PP)
    sk["Player"] = sk["Name"].astype(str).str.strip()
    sk["Team"]   = sk["TeamAbbrev"].fillna("")
    sk["Position"] = sk["Position"].fillna("")
    sk["DK Salary"] = pd.to_numeric(sk["Salary"], errors="coerce")
    sk["Opponent"] = ""  # we can enrich later from odds

    def norm(n): return re.sub(r"[\W_]+"," ", str(n)).strip().upper()
    sk["_n"] = sk["Player"].apply(norm)

    if not line_players.empty:
        lp5 = line_players[line_players["Line Type"].str.startswith("5v5", na=False)].copy()
        lpp = line_players[line_players["Line Type"].str.startswith("PP",   na=False)].copy()
        lp5["_n"] = lp5["Player"].apply(norm)
        lpp["_n"] = lpp["Player"].apply(norm)

        j = sk.merge(lp5[["_n","Line Type","Line ID"]], on="_n", how="left")
        j = j.merge(lpp[["_n","Line Type","Line ID"]].rename(columns={"Line Type":"PP Line","Line ID":"PP Line ID"}), on="_n", how="left")
        sk = j.drop(columns=["_n"])
        sk["Line"] = sk["Line Type"].fillna("")
        sk["PP Line"] = sk["PP Line"].fillna("")
        sk["Line ID"] = sk["Line ID"].fillna("")
        sk["Line Multiplier (xGF)"] = sk["Line"].map(SETTINGS["LineMult_byType"]).fillna(1.0)
    else:
        sk["Line"] = ""; sk["PP Line"] = ""; sk["Line ID"] = ""; sk["Line Multiplier (xGF)"] = 1.0

    # Expected TOI
    sk["Expected TOI (min)"] = sk["Position"].apply(lambda p: SETTINGS["TOI_default"]["D"] if "D" in str(p).upper() else SETTINGS["TOI_default"]["F"])

    # Neutral per-60 baselines (replace later with NST per-60)
    def baseline_rates(pos):
        role = guess_role_from_pos(pos)
        base = SETTINGS["D_baseline"] if role=="D" else SETTINGS["F_baseline"]
        return pd.Series([base["G60"], base["A60"], base["SOG60"], base["BLK60"]],
                         index=["Weighted Goals/60","Weighted Assists/60","Weighted SOG/60","Weighted Blocks/60"])
    rates = sk["Position"].apply(baseline_rates)
    sk = pd.concat([sk, rates], axis=1)

    # Opponent/PP factors (neutral for now)
    sk["Opp_SOG_factor"] = SETTINGS["Opp_SOG_factor"]
    sk["Opp_xGA_factor"] = SETTINGS["Opp_xGA_factor"]
    sk["Opp_CA_factor"]  = SETTINGS["Opp_CA_factor"]
    sk["PP_Edge"] = sk["PP Line"].apply(lambda x: SETTINGS["PP_Edge"] if x in ("PP1","PP2") else 1.0)

    # Raw projections (per-60 * TOI/60)
    toi_ratio = pd.to_numeric(sk["Expected TOI (min)"], errors="coerce").fillna(0)/60.0
    sk["Proj Goals (raw)"]   = sk["Weighted Goals/60"]  * toi_ratio
    sk["Proj Assists (raw)"] = sk["Weighted Assists/60"]* toi_ratio
    sk["Proj SOG (raw)"]     = sk["Weighted SOG/60"]    * toi_ratio
    sk["Proj Blocks (raw)"]  = sk["Weighted Blocks/60"] * toi_ratio

    # Adjusted projections
    sk["Adj Proj Goals"]   = sk["Proj Goals (raw)"]   * sk["Opp_xGA_factor"] * sk["Line Multiplier (xGF)"] * sk["PP_Edge"]
    sk["Adj Proj Assists"] = sk["Proj Assists (raw)"] * sk["Opp_xGA_factor"] * sk["Line Multiplier (xGF)"] * sk["PP_Edge"]
    sk["Adj Proj SOG"]     = sk["Proj SOG (raw)"]     * sk["Opp_SOG_factor"] * sk["Line Multiplier (xGF)"]
    sk["Adj Proj Blocks"]  = sk["Proj Blocks (raw)"]  * (0.5 + 0.5*sk["Opp_CA_factor"])

    # DK points & value
    pts = SETTINGS["DK_points"]
    sk["Projected DK Points"] = (
        sk["Adj Proj Goals"]*pts["Goal"] +
        sk["Adj Proj Assists"]*pts["Assist"] +
        sk["Adj Proj SOG"]*pts["SOG"] +
        sk["Adj Proj Blocks"]*pts["Block"]
    )
    sk["DFS Value Score"] = (sk["Projected DK Points"] / pd.to_numeric(sk["DK Salary"], errors="coerce").replace(0, math.nan)) * 1000.0
    sk["Proj Points (raw)"] = (
        sk["Proj Goals (raw)"]*pts["Goal"] +
        sk["Proj Assists (raw)"]*pts["Assist"] +
        sk["Proj SOG (raw)"]*pts["SOG"] +
        sk["Proj Blocks (raw)"]*pts["Block"]
    )

    # Final DFS projections columns (your exact layout)
    cols = [
        "Player","Team","Opponent","Position","Line","PP Line","Expected TOI (min)","DK Salary",
        "LxG Goals/60","LxG Assists/60","LxG SOG/60","LxG Blocks/60",
        "LxD Goals/60","LxD Assists/60","LxD SOG/60","LxD Blocks/60",
        "Season Goals/60","Season Assists/60","Season SOG/60","Season Blocks/60",
        "Weighted Goals/60","Weighted Assists/60","Weighted SOG/60","Weighted Blocks/60",
        "Opp_SOG_factor","Opp_xGA_factor","Opp_CA_factor","PP_Edge",
        "Proj Goals (raw)","Proj Assists (raw)","Proj SOG (raw)","Proj Blocks (raw)",
        "Projected DK Points","DFS Value Score","Proj Points (raw)","Line ID","Line Multiplier (xGF)",
        "Adj Proj Goals","Adj Proj Assists","Adj Proj SOG"
    ]
    for c in cols:
        if c not in sk.columns: sk[c] = ""
    dfs_proj = sk[cols].copy()

    # TOP STACKS (5v5 lines)
    if line_players.empty if 'line_players' in locals() else True:
        top_stacks = pd.DataFrame()
    else:
        tri = line_players[line_players["Line Type"].str.match(r"^5v5 Line \d$", na=False)].copy()
        if tri.empty:
            top_stacks = pd.DataFrame()
        else:
            # Join to projected points & salary
            proj_small = dfs_proj[["Player","Team","Projected DK Points","DK Salary","Line","Line ID"]].copy()
            proj_small["Player_norm"] = proj_small["Player"].str.replace(r"[\W_]+"," ", regex=True).str.upper().str.strip()
            tri["Player_norm"] = tri["Player"].str.replace(r"[\W_]+"," ", regex=True).str.upper().str.strip()
            merged = tri.merge(proj_small, on="Player_norm", how="left")
            agg = merged.groupby(["Team","Line ID","Line Type"], dropna=False).agg(
                Stack_Points=("Projected DK Points","sum"),
                Stack_Salary=("DK Salary","sum"),
                Players=("Player", lambda s: ", ".join(sorted(set(s)))),
            ).reset_index()
            agg["Stack_Value"] = (agg["Stack_Points"]/agg["Stack_Salary"])*1000.0
            top_stacks = agg.sort_values(["Stack_Value","Stack_Points"], ascending=[False,False]).reset_index(drop=True)

    # GOALIES (starter)
    if gk.empty:
        goalies = pd.DataFrame(columns=["Goalie","Team","DK Salary","Proj Saves"])
    else:
        gk = gk.rename(columns={"Name":"Goalie","TeamAbbrev":"Team","Salary":"DK Salary"})
        gk["Proj Saves"] = 29.0  # neutral baseline; upgrade later with shots-against model
        goalies = gk[["Goalie","Team","DK Salary","Proj Saves"]].copy()

    return dfs_proj, top_stacks, goalies

# -----------------------------
# MAIN
# -----------------------------
def main():
    # Scrape sources
    dk = fetch_dk_salaries()
    dk.to_csv("data/dk_salaries.csv", index=False)

    odds = fetch_odds()
    odds.to_csv("data/sportsbook_odds.csv", index=False)

    nst_map = fetch_nst_player_map()
    nst_map.to_csv("data/nst_player_map.csv", index=False)

    lines = fetch_lines()
    lines.to_csv("data/line_context.csv", index=False)

    # Build projections
    dfs_proj, top_stacks, goalies = build_projections(dk, lines)
    dfs_proj.to_csv("data/dfs_projections.csv", index=False)
    top_stacks.to_csv("data/top_stacks.csv", index=False)
    goalies.to_csv("data/goalies.csv", index=False)

    print("All CSVs updated in /data")

if __name__ == "__main__":
    main()
