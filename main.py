import os, io, re, time, math, requests
import pandas as pd
from datetime import datetime

# ========= CONFIG / CONSTANTS =========
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

SETTINGS = {
    # If a player lacks NST data (rookie/low sample), we backfill with gentle priors
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.0, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
    # TOI defaults (until you wire real TOI model)
    "TOI_default": {"F": 16.0, "D": 22.0},
    # Line multipliers (chemistry baseline)
    "LineMult_byType": {
        "5v5 Line 1": 1.08, "5v5 Line 2": 1.03, "5v5 Line 3": 0.98, "5v5 Line 4": 0.94,
        "D Pair 1": 1.05, "D Pair 2": 1.00, "D Pair 3": 0.96,
        "PP1": 1.12, "PP2": 1.03, "PK1": 0.92, "PK2": 0.95
    },
    # Opponent neutral factors (1.0) — can be replaced later with NST team-defense inputs
    "Opp_SOG_factor": 1.00,
    "Opp_xGA_factor": 1.00,
    "Opp_CA_factor":  1.00,
    # DraftKings scoring
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3},
    # polite throttling
    "sleep_nst": 4.0,
    "sleep_lines": 2.0
}

os.makedirs("data", exist_ok=True)


# ========= HELPERS =========
def norm_name(s: str) -> str:
    s = re.sub(r"[\u2013\u2014\u2019]", "-", str(s))      # normalize dashes/quotes
    s = re.sub(r"[^A-Za-z0-9\-\' ]+", " ", s)            # strip weird chars
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def guess_role_from_pos(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"


# ========= SCRAPE: DraftKings Salaries =========
def fetch_dk_salaries() -> pd.DataFrame:
    try:
        url = "https://www.draftkings.com/lobby/getcontests?sport=NHL"
        r = requests.get(url, headers=HEADERS, timeout=40)
        r.raise_for_status()
        contests = r.json().get("Contests", [])
        def looks_classic(c):
            for k in ("ContestType","Type","contestType","ContestName","Name","name"):
                v = c.get(k)
                if isinstance(v, str) and "classic" in v.lower():
                    return True
            return False
        classic = [c for c in contests if looks_classic(c)]
        if not classic and contests:
            classic = [contests[0]]

        dfs = []
        for c in classic:
            cid = c.get("ContestId")
            if not cid: continue
            csv_url = f"https://www.draftkings.com/lineup/getavailableplayerscsv?contestId={cid}"
            cr = requests.get(csv_url, headers=HEADERS, timeout=50)
            if cr.status_code != 200: continue
            df = pd.read_csv(io.StringIO(cr.text))
            df.insert(0, "Contest", c.get("ContestName",""))
            dfs.append(df)
        out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if not out.empty:
            # Normalize column names we care about
            ren = {}
            for col in out.columns:
                lc = col.lower()
                if lc == "name": ren[col] = "Name"
                if lc == "teamabbrev": ren[col] = "TeamAbbrev"
                if lc == "position": ren[col] = "Position"
                if lc == "salary": ren[col] = "Salary"
            out = out.rename(columns=ren)
        return out
    except Exception as e:
        print("DK error:", e)
        return pd.DataFrame()


# ========= SCRAPE: Odds API =========
def fetch_odds() -> pd.DataFrame:
    if not ODDS_API_KEY:
        return pd.DataFrame([{"Error": "Missing ODDS_API_KEY"}])
    try:
        url = (
            "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
            f"?regions=us&markets=h2h,totals,player_points,player_assists,player_goals&oddsFormat=decimal&apiKey={ODDS_API_KEY}"
        )
        r = requests.get(url, headers=HEADERS, timeout=50)
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


# ========= SCRAPE: NST Per-Player Stats (per-60) =========
# Pulls per-team player table and extracts common per-60s and danger/xG context.
# Endpoint pattern: https://www.naturalstattrick.com/playerteams.php?team=TOR&sit=all
def fetch_nst_player_stats() -> pd.DataFrame:
    cols = ["Player","Team","GP","TOI","G/60","A/60","SOG/60","BLK/60","CF/60","CA/60","xGF/60","xGA/60","HDCF/60","HDCA/60"]
    all_rows = []
    for abbr in TEAMS:
        url = f"https://www.naturalstattrick.com/playerteams.php?team={abbr}&sit=all"
        # Utah Mammoth fallback (if NST hasn’t updated code yet)
        if abbr == "UTA":
            r = requests.get(url, headers=HEADERS, timeout=50)
            if r.status_code != 200:
                url = "https://www.naturalstattrick.com/playerteams.php?team=ARI&sit=all"
        try:
            r = requests.get(url, headers=HEADERS, timeout=50)
            if r.status_code != 200:
                print(f"NST stats HTTP {r.status_code} for {abbr}")
                time.sleep(SETTINGS["sleep_nst"]); continue
            html = r.text
            # Rough row parse: capture table rows that include player link, then grab columns nearby
            # We'll pull names and then search per-60 numeric columns following the row.
            # Fallback: find all player anchors, then search rightward for numeric per-60s.
            players = re.findall(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', html, flags=re.IGNORECASE)
            # Grab all numbers that look like per-60 stats; later we align by heuristic (best-effort)
            # Better approach would be full HTML table parsing with BeautifulSoup; kept regexy to stay lightweight.
            rows_text = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
            # Build a quick map Player -> row_text
            plist = []
            for row in rows_text:
                m = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
                if m:
                    pname = m.group(1).strip()
                    plist.append((pname, row))

            def pick_num(pattern, s):
                m = re.search(pattern, s, flags=re.IGNORECASE)
                if m:
                    try: return float(m.group(1))
                    except: return None
                return None

            for pname, row in plist:
                # Heuristics: find per-60 columns by nearby titles or numeric positions
                # We try multiple patterns; if nothing, leave as None
                # (These patterns should be adapted if NST markup shifts.)
                g60   = pick_num(r'G/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                a60   = pick_num(r'A/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                sog60 = pick_num(r'(?:S|Shots)/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                blk60 = pick_num(r'Blk/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                cf60  = pick_num(r'CF/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                ca60  = pick_num(r'CA/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                xgf60 = pick_num(r'xGF/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                xga60 = pick_num(r'xGA/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                hdcf60= pick_num(r'HDCF/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                hdca60= pick_num(r'HDCA/60[^0-9\-]*([0-9]*\.?[0-9]+)', row)
                toistr= re.search(r'(?:TOI|TOI/GP)[^0-9]*([0-9]*\.?[0-9]+)', row, flags=re.IGNORECASE)
                toival= float(toistr.group(1)) if toistr else None

                all_rows.append({
                    "Player": pname.strip(),
                    "Team": abbr,
                    "GP": None,
                    "TOI": toival,
                    "G/60": g60, "A/60": a60, "SOG/60": sog60, "BLK/60": blk60,
                    "CF/60": cf60, "CA/60": ca60, "xGF/60": xgf60, "xGA/60": xga60,
                    "HDCF/60": hdcf60, "HDCA/60": hdca60,
                    "NameKey": f"{abbr}_{norm_name(pname)}"
                })
            time.sleep(SETTINGS["sleep_nst"])
        except Exception as e:
            print(f"NST stats err for {abbr}:", e)
            time.sleep(SETTINGS["sleep_nst"])
            continue

    df = pd.DataFrame(all_rows)
    # Drop rows with no player name
    df = df[df["Player"].astype(str).str.len() > 0]
    return df


# ========= SCRAPE: DFO → LWL Lines (and build line lists) =========
def parse_fixed_blocks(names):
    # Assumes order on page; robust enough for our purposes
    layout = [
        ("5v5 Line 1", 3), ("5v5 Line 2", 3), ("5v5 Line 3", 3), ("5v5 Line 4", 3),
        ("D Pair 1", 2), ("D Pair 2", 2), ("D Pair 3", 2),
        ("PP1", 5), ("PP2", 5),
        ("PK1", 4), ("PK2", 4)
    ]
    out, idx = [], 0
    for typ, size in layout:
        group = names[idx:idx+size]; idx += size
        if len(group) == size:
            out.append((typ, " – ".join(group)))
    return out

def fetch_lines_df() -> pd.DataFrame:
    rows = []
    for abbr in TEAMS:
        got = False
        slug = DFO_SLUG.get(abbr, "")
        # Try DFO
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
        except Exception:
            pass
        # Fallback LWL
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
            except Exception:
                pass
        if not got:
            rows.append({"Team": abbr, "Line Type": "ERROR", "Line ID": f"{abbr}_ERR", "Players": "No data", "Source": "NONE"})
        time.sleep(SETTINGS["sleep_lines"])
    return pd.DataFrame(rows)

def explode_line_players(lines_df: pd.DataFrame) -> pd.DataFrame:
    # Split "A – B – C" into rows
    def split_players(s):
        return [p.strip() for p in re.split(r"–|-|,|\u2013", str(s)) if p.strip()]
    out = []
    for _, r in lines_df.iterrows():
        ps = split_players(r.get("Players",""))
        for p in ps:
            out.append({
                "Team": r.get("Team",""),
                "Line Type": r.get("Line Type",""),
                "Line ID": r.get("Line ID",""),
                "Player": p,
                "PlayerKey": f"{r.get('Team','')}_{norm_name(p)}"
            })
    return pd.DataFrame(out)


# ========= LINE CHEMISTRY from NST + Lines =========
def build_line_chemistry(nst_players: pd.DataFrame, lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each Line ID, aggregate NST per-60 of the unit (sum where appropriate).
    We'll compute:
      - G/60, A/60, SOG/60, BLK/60 (sum of the unit’s members)
      - xGF/60, xGA/60, CF/60, CA/60, HDCF/60, HDCA/60 (sum)
    These become line-level multipliers / context.
    """
    if nst_players is None or nst_players.empty or lines_df is None or lines_df.empty:
        return pd.DataFrame()

    lp = explode_line_players(lines_df)
    if lp.empty: return pd.DataFrame()

    # Join NST stats onto each line-player
    nst = nst_players.copy()
    # Index by PlayerKey to join
    nst["PlayerKey"] = nst.apply(lambda r: f"{r.get('Team','')}_{norm_name(r.get('Player',''))}", axis=1)

    merged = lp.merge(
        nst[["PlayerKey","G/60","A/60","SOG/60","BLK/60","CF/60","CA/60","xGF/60","xGA/60","HDCF/60","HDCA/60"]],
        on="PlayerKey", how="left"
    )

    # Aggregate per line
    agg = merged.groupby(["Team","Line ID","Line Type"], dropna=False).agg({
        "G/60":"sum","A/60":"sum","SOG/60":"sum","BLK/60":"sum",
        "CF/60":"sum","CA/60":"sum","xGF/60":"sum","xGA/60":"sum",
        "HDCF/60":"sum","HDCA/60":"sum"
    }).reset_index()

    # Attach line multiplier defaults
    agg["Base Line Multiplier"] = agg["Line Type"].map(SETTINGS["LineMult_byType"]).fillna(1.0)

    return agg


# ========= PROJECTIONS (line-aware, opponent-neutral baseline) =========
def build_projections(dk_df: pd.DataFrame, nst_players: pd.DataFrame, lines_df: pd.DataFrame, line_chem: pd.DataFrame):
    if dk_df is None or dk_df.empty:  # must have DK
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    dk = dk_df.copy()
    ren = {}
    for c in dk.columns:
        lc = c.lower()
        if lc == "name": ren[c] = "Name"
        if lc == "teamabbrev": ren[c] = "Team"
        if lc == "position": ren[c] = "Position"
        if lc == "salary": ren[c] = "DK Salary"
    dk = dk.rename(columns=ren)
    keep = [c for c in ["Name","Team","Position","DK Salary"] if c in dk.columns]
    dk = dk[keep].copy()

    # Split skaters / goalies
    sk = dk[~dk["Position"].str.contains("G", case=False, na=False)].copy()
    gk = dk[ dk["Position"].str.contains("G", case=False, na=False)].copy()

    # Join NST per-player onto skaters
    if nst_players is not None and not nst_players.empty:
        nst = nst_players.copy()
        nst["PlayerKey"] = nst.apply(lambda r: f"{r.get('Team','')}_{norm_name(r.get('Player',''))}", axis=1)
        sk["PlayerKey"]   = sk.apply(lambda r: f"{r.get('Team','')}_{norm_name(r.get('Name',''))}", axis=1)
        sk = sk.merge(nst[["PlayerKey","G/60","A/60","SOG/60","BLK/60"]], on="PlayerKey", how="left")
    else:
        sk["G/60"] = sk["A/60"] = sk["SOG/60"] = sk["BLK/60"] = None

    # Fallback for missing NST player rows (rookies)
    def backfill_rates(row):
        role = guess_role_from_pos(row.get("Position",""))
        base = SETTINGS["D_fallback"] if role=="D" else SETTINGS["F_fallback"]
        return pd.Series({
            "G/60": row["G/60"] if pd.notna(row["G/60"]) else base["G60"],
            "A/60": row["A/60"] if pd.notna(row["A/60"]) else base["A60"],
            "SOG/60": row["SOG/60"] if pd.notna(row["SOG/60"]) else base["SOG60"],
            "BLK/60": row["BLK/60"] if pd.notna(row["BLK/60"]) else base["BLK60"]
        })
    rates = sk.apply(backfill_rates, axis=1)
    sk[["G/60","A/60","SOG/60","BLK/60"]] = rates

    # Attach lines to skaters (5v5 + PP)
    if lines_df is not None and not lines_df.empty:
        lp = explode_line_players(lines_df)
        # 5v5 line
        fivev5 = lp[lp["Line Type"].str.startswith("5v5", na=False)].copy()
        # PP line
        ppl    = lp[lp["Line Type"].str.startswith("PP", na=False)].copy()

        # Join by PlayerKey
        sk = sk.merge(fivev5[["PlayerKey","Line Type","Line ID"]].rename(
            columns={"Line Type":"Line","Line ID":"Line ID_5v5"}), on="PlayerKey", how="left")
        sk = sk.merge(ppl[["PlayerKey","Line Type","Line ID"]].rename(
            columns={"Line Type":"PP Line","Line ID":"PP Line ID"}), on="PlayerKey", how="left")
    else:
        sk["Line"] = ""; sk["Line ID_5v5"] = ""; sk["PP Line"] = ""; sk["PP Line ID"] = ""

    # Line multiplier from unit type (default 1.0)
    sk["Line Multiplier (xGF)"] = sk["Line"].map(SETTINGS["LineMult_byType"]).fillna(1.0)

    # Expected TOI (minutes)
    sk["Expected TOI (min)"] = sk["Position"].apply(lambda p: SETTINGS["TOI_default"]["D"] if "D" in str(p).upper() else SETTINGS["TOI_default"]["F"])
    toi_ratio = pd.to_numeric(sk["Expected TOI (min)"], errors="coerce").fillna(0) / 60.0

    # Opponent factors (wire team-defense later)
    sk["Opp_SOG_factor"] = SETTINGS["Opp_SOG_factor"]
    sk["Opp_xGA_factor"] = SETTINGS["Opp_xGA_factor"]
    sk["Opp_CA_factor"]  = SETTINGS["Opp_CA_factor"]
    sk["PP_Edge"] = sk["PP Line"].apply(lambda x: 1.10 if x in ("PP1","PP2") else 1.0)

    # Line chemistry influence:
    # Map each player’s 5v5 Line ID to line-level xGF/60 sum (normalize to 3 forwards by design)
    if line_chem is not None and not line_chem.empty:
        chem = line_chem[["Line ID","Line Type","xGF/60","xGA/60","CF/60","CA/60","HDCF/60","HDCA/60","G/60","A/60","SOG/60","BLK/60","Base Line Multiplier"]].copy()
        chem = chem.rename(columns={"Line ID":"Line ID_5v5"})
        sk = sk.merge(chem, on="Line ID_5v5", how="left", suffixes=("","_LINE"))
        # Chemistry multiplier: blend Base Line Multiplier with relative line xGF signal
        # Normalize line xGF/60 by a soft baseline (e.g., 5.0 for a trio); tweakable later
        sk["Chemistry Mult"] = sk["Base Line Multiplier"].fillna(1.0) * (1.0 + ((sk["xGF/60"].fillna(5.0) - 5.0) / 25.0))
    else:
        sk["Chemistry Mult"] = 1.0

    # Raw per-60 → projections
    sk["Proj Goals (raw)"]   = sk["G/60"]   * toi_ratio
    sk["Proj Assists (raw)"] = sk["A/60"]   * toi_ratio
    sk["Proj SOG (raw)"]     = sk["SOG/60"] * toi_ratio
    sk["Proj Blocks (raw)"]  = sk["BLK/60"] * toi_ratio

    # Adjusted with opponent + line + chemistry
    sk["Adj Proj Goals"]   = sk["Proj Goals (raw)"]   * sk["Opp_xGA_factor"] * sk["Line Multiplier (xGF)"] * sk["PP_Edge"] * sk["Chemistry Mult"]
    sk["Adj Proj Assists"] = sk["Proj Assists (raw)"] * sk["Opp_xGA_factor"] * sk["Line Multiplier (xGF)"] * sk["PP_Edge"] * sk["Chemistry Mult"]
    sk["Adj Proj SOG"]     = sk["Proj SOG (raw)"]     * sk["Opp_SOG_factor"] * sk["Line Multiplier (xGF)"] * sk["Chemistry Mult"]
    sk["Adj Proj Blocks"]  = sk["Proj Blocks (raw)"]  * (0.5 + 0.5*sk["Opp_CA_factor"])

    # DK points & value
    pts = SETTINGS["DK_points"]
    sk["Projected DK Points"] = (
        sk["Adj Proj Goals"]*pts["Goal"] +
        sk["Adj Proj Assists"]*pts["Assist"] +
        sk["Adj Proj SOG"]*pts["SOG"] +
        sk["Adj Proj Blocks"]*pts["Block"]
    )
    salary = pd.to_numeric(sk["DK Salary"], errors="coerce")
    sk["DFS Value Score"] = (sk["Projected DK Points"] / salary.replace(0, math.nan)) * 1000.0
    sk["Proj Points (raw)"] = (
        sk["Proj Goals (raw)"]*pts["Goal"] +
        sk["Proj Assists (raw)"]*pts["Assist"] +
        sk["Proj SOG (raw)"]*pts["SOG"] +
        sk["Proj Blocks (raw)"]*pts["Block"]
    )

    # Final DFS projections in your exact column order
    desired = [
        "Name","Team","", "Position","Line","PP Line","Expected TOI (min)","DK Salary",
        "","","","","","","","","","","","",  # placeholders for LxG/LxD/Season columns you may add later
        "G/60","A/60","SOG/60","BLK/60",
        "Opp_SOG_factor","Opp_xGA_factor","Opp_CA_factor","PP_Edge",
        "Proj Goals (raw)","Proj Assists (raw)","Proj SOG (raw)","Proj Blocks (raw)",
        "Projected DK Points","DFS Value Score","Proj Points (raw)","Line ID_5v5","Line Multiplier (xGF)",
        "Adj Proj Goals","Adj Proj Assists","Adj Proj SOG"
    ]
    # Build a mapping to your labeled headers:
    rename_for_sheet = {
        "Name":"Player",
        "": "Opponent",  # left blank; you can wire opponent mapping later
        "G/60":"Weighted Goals/60",
        "A/60":"Weighted Assists/60",
        "SOG/60":"Weighted SOG/60",
        "BLK/60":"Weighted Blocks/60"
    }
    sk = sk.rename(columns=rename_for_sheet)
    # Ensure all expected columns exist
    cols = [
        "Player","Team","Opponent","Position","Line","PP Line","Expected TOI (min)","DK Salary",
        "LxG Goals/60","LxG Assists/60","LxG SOG/60","LxG Blocks/60",
        "LxD Goals/60","LxD Assists/60","LxD SOG/60","LxD Blocks/60",
        "Season Goals/60","Season Assists/60","Season SOG/60","Season Blocks/60",
        "Weighted Goals/60","Weighted Assists/60","Weighted SOG/60","Weighted Blocks/60",
        "Opp_SOG_factor","Opp_xGA_factor","Opp_CA_factor","PP_Edge",
        "Proj Goals (raw)","Proj Assists (raw)","Proj SOG (raw)","Proj Blocks (raw)",
        "Projected DK Points","DFS Value Score","Proj Points (raw)","Line ID_5v5","Line Multiplier (xGF)",
        "Adj Proj Goals","Adj Proj Assists","Adj Proj SOG"
    ]
    for c in cols:
        if c not in sk.columns: sk[c] = ""
    dfs_proj = sk[cols].copy()

    # TOP STACKS: sum trio DK points and salary for 5v5 lines
    if lines_df is not None and not lines_df.empty:
        lp = explode_line_players(lines_df)
        trio = lp[lp["Line Type"].str.match(r"^5v5 Line \d$", na=False)].copy()
        if trio.empty:
            top_stacks = pd.DataFrame()
        else:
            # join to projections
            psmall = dfs_proj[["Player","Team","Projected DK Points","DK Salary","Line ID_5v5"]].copy()
            psmall["Player_norm"] = psmall["Player"].apply(norm_name)
            trio["Player_norm"]   = trio["Player"].apply(norm_name)
            m = trio.merge(psmall, on="Player_norm", how="left")
            agg = m.groupby(["Team","Line ID","Line Type"], dropna=False).agg(
                Stack_Points=("Projected DK Points","sum"),
                Stack_Salary=("DK Salary","sum"),
                Players=("Player", lambda s: ", ".join(sorted(set(s))))
            ).reset_index()
            agg["Stack_Value"] = (agg["Stack_Points"] / agg["Stack_Salary"].replace(0, math.nan)) * 1000.0
            top_stacks = agg.sort_values(["Stack_Value","Stack_Points"], ascending=[False,False]).reset_index(drop=True)
    else:
        top_stacks = pd.DataFrame()

    # GOALIES: baseline saves (upgrade later with SOG against model)
    if not gk.empty:
        gk2 = gk.rename(columns={"Name":"Goalie","Team":"Team","DK Salary":"DK Salary"})
        gk2["Proj Saves"] = 29.0
        goalies = gk2[["Goalie","Team","DK Salary","Proj Saves"]].copy()
    else:
        goalies = pd.DataFrame(columns=["Goalie","Team","DK Salary","Proj Saves"])

    return dfs_proj, top_stacks, goalies


# ========= MAIN PIPELINE =========
def main():
    print("Fetching DK salaries...")
    dk = fetch_dk_salaries()
    dk.to_csv("data/dk_salaries.csv", index=False)

    print("Fetching Odds...")
    odds = fetch_odds()
    odds.to_csv("data/sportsbook_odds.csv", index=False)

    print("Fetching NST per-player stats...")
    nst_players = fetch_nst_player_stats()
    nst_players.to_csv("data/nst_player_stats.csv", index=False)

    print("Fetching lines (DFO → LWL fallback)...")
    lines_df = fetch_lines_df()
    lines_df.to_csv("data/line_context.csv", index=False)

    print("Building line chemistry...")
    line_chem = build_line_chemistry(nst_players, lines_df)
    line_chem.to_csv("data/line_chemistry.csv", index=False)

    print("Building projections (line-aware)...")
    dfs_proj, top_stacks, goalies = build_projections(dk, nst_players, lines_df, line_chem)
    dfs_proj.to_csv("data/dfs_projections.csv", index=False)
    top_stacks.to_csv("data/top_stacks.csv", index=False)
    goalies.to_csv("data/goalies.csv", index=False)

    print("All CSVs written to /data")

if __name__ == "__main__":
    main()
