# -*- coding: utf-8 -*-
import os, io, re, time, math, requests
import pandas as pd

# ================== CONFIG ==================
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"}
os.makedirs("data", exist_ok=True)

# Seasons (update when needed)
CURR_SEASON = "20242025"
LAST_SEASON = "20232024"

# League baselines / knobs
LEAGUE_AVG_SV = 0.905
GOALIE_SKATER_DAMPEN_MAX = 0.10     # cap ±10% on skater G/A from goalie effect
GOALIE_SKATER_DAMPEN_WEIGHT = 0.25  # how strongly SV% diff → dampener
GOALIE_TOI_MIN = 60.0               # goalie minutes for projections

FALLBACK_SF60 = 31.0
FALLBACK_xGF60 = 2.95

SETTINGS = {
    "DK_points": {"Goal": 8.5, "Assist": 5.0, "SOG": 1.5, "Block": 1.3},
    "LineMult_byType": {
        "5v5 Line 1": 1.12, "5v5 Line 2": 1.04, "5v5 Line 3": 0.97, "5v5 Line 4": 0.92,
        "D Pair 1": 1.05, "D Pair 2": 1.00, "D Pair 3": 0.96,
        "PP1": 1.12, "PP2": 1.03, "PK1": 0.92, "PK2": 0.95
    },
    # Gentle rookie/low-sample priors
    "F_fallback": {"G60": 0.45, "A60": 0.80, "SOG60": 5.0, "BLK60": 1.0},
    "D_fallback": {"G60": 0.20, "A60": 0.70, "SOG60": 3.2, "BLK60": 4.0},
    # Opp factors (neutral for now unless you wire team-defense)
    "Opp_SOG_factor": 1.00,
    "Opp_xGA_factor": 1.00,
    "Opp_CA_factor":  1.00,
    # polite throttling
    "sleep_nst": 3.5,
    "sleep_lines": 2.0
}

TEAMS = [
    "ANA","UTA","BOS","BUF","CGY","CAR","CHI","COL","CBJ","DAL","DET","EDM","FLA",
    "LAK","MIN","MTL","NSH","NJD","NYI","NYR","OTT","PHI","PIT","SJS","SEA","STL",
    "TBL","TOR","VAN","VGK","WSH","WPG"
]
# DFO slugs (Utah Mammoth included)
DFO_SLUG = {
    "ANA":"ducks","UTA":"utah-mammoth","BOS":"bruins","BUF":"sabres","CGY":"flames",
    "CAR":"hurricanes","CHI":"blackhawks","COL":"avalanche","CBJ":"blue-jackets","DAL":"stars",
    "DET":"red-wings","EDM":"oilers","FLA":"panthers","LAK":"kings","MIN":"wild",
    "MTL":"canadiens","NSH":"predators","NJD":"devils","NYI":"islanders","NYR":"rangers",
    "OTT":"senators","PHI":"flyers","PIT":"penguins","SJS":"sharks","SEA":"kraken",
    "STL":"blues","TBL":"lightning","TOR":"maple-leafs","VAN":"canucks","VGK":"golden-knights",
    "WSH":"capitals","WPG":"jets"
}

# ================== HELPERS ==================
def norm_name(s: str) -> str:
    s = re.sub(r"[\u2013\u2014\u2019]", "-", str(s))
    s = re.sub(r"[^A-Za-z0-9\-\' ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def clamp(val, lo, hi):
    return max(lo, min(hi, val))

def light_goalie_dampener(weighted_sv):
    if pd.isna(weighted_sv) or weighted_sv <= 0 or weighted_sv >= 1:
        return 0.0
    adj = (LEAGUE_AVG_SV - float(weighted_sv)) * GOALIE_SKATER_DAMPEN_WEIGHT
    return clamp(adj, -GOALIE_SKATER_DAMPEN_MAX, GOALIE_SKATER_DAMPEN_MAX)

def blend_three_layer(stat_recent, stat_season, stat_lastyear, w_recent=0.50, w_season=0.35, w_last=0.15):
    vals = [stat_recent, stat_season, stat_lastyear]
    if all(pd.isna(v) for v in vals):
        return None
    r = 0.0; totw = 0.0
    if pd.notna(stat_recent): r += w_recent * stat_recent; totw += w_recent
    if pd.notna(stat_season): r += w_season * stat_season; totw += w_season
    if pd.notna(stat_lastyear): r += w_last * stat_lastyear; totw += w_last
    return r / totw if totw > 0 else None

def guess_role_from_pos(pos: str) -> str:
    if not isinstance(pos, str): return "F"
    p = pos.upper()
    if "G" in p: return "G"
    if "D" in p: return "D"
    return "F"

# ================== DRAFTKINGS (light) ==================
def fetch_dk_salaries_csv_if_present() -> pd.DataFrame:
    """
    Reads data/dk_salaries.csv (recommended daily export).
    If you have a DK fetcher elsewhere, keep writing to this path.
    """
    p = "data/dk_salaries.csv"
    if os.path.exists(p):
        try: return pd.read_csv(p)
        except Exception as e:
            print("DK CSV read error:", e)
    print("WARNING: data/dk_salaries.csv not found; projections will be sparse.")
    return pd.DataFrame(columns=["Name","TeamAbbrev","Position","Salary"])

# ================== NST SCRAPERS ==================
def _nst_get(url):
    r = requests.get(url, headers=HEADERS, timeout=60)
    return r

def _parse_nst_player_rows(html):
    """
    Parse NST player table rows into list of dicts with per-60 columns if present.
    This is regex-based but resilient; falls back to None if a column is not found.
    """
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
    out = []
    for row in rows:
        m_name = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
        if not m_name: 
            continue
        pname = m_name.group(1).strip()

        def pick_num(label):
            # try label/60 variants
            pats = [
                rf'{label}\s*/\s*60[^0-9\-]*([0-9]*\.?[0-9]+)',
                rf'{label}60[^0-9\-]*([0-9]*\.?[0-9]+)',
                rf'{label}[^0-9\-]*([0-9]*\.?[0-9]+)'
            ]
            for pat in pats:
                m = re.search(pat, row, flags=re.IGNORECASE)
                if m:
                    try: return float(m.group(1))
                    except: pass
            return None

        g60   = pick_num("G")
        a60   = pick_num("A")
        sog60 = pick_num("(?:S|Shots)")
        blk60 = pick_num("Blk")
        cf60  = pick_num("CF")
        ca60  = pick_num("CA")
        xgf60 = pick_num("xGF")
        xga60 = pick_num("xGA")
        hdcf60= pick_num("HDCF")
        hdca60= pick_num("HDCA")
        toistr= None
        for lab in ("TOI/GP","TOI"):
            m = re.search(rf'{lab}[^0-9]*([0-9]*\.?[0-9]+)', row, flags=re.IGNORECASE)
            if m: 
                toistr = m.group(1)
                break
        toival= float(toistr) if toistr else None
        out.append({
            "Player": pname, "G/60": g60, "A/60": a60, "SOG/60": sog60, "BLK/60": blk60,
            "CF/60": cf60, "CA/60": ca60, "xGF/60": xgf60, "xGA/60": xga60,
            "HDCF/60": hdcf60, "HDCA/60": hdca60, "TOI": toival
        })
    return out

def fetch_nst_player_stats_multi() -> pd.DataFrame:
    """
    For EACH team, fetch player tables for:
      - This Season
      - Recent (last 10 games)
      - Last Season
    Then three-layer blend per-60s: recent / season / last.
    """
    all_rows = []
    for abbr in TEAMS:
        # Utah Mammoth NST may still use ARI in some places; try UTA then fallback ARI.
        team_code_list = [abbr] if abbr != "UTA" else ["UTA","ARI"]
        for team_code in team_code_list:
            # ---- This season ----
            url_season = f"https://www.naturalstattrick.com/playerteams.php?team={team_code}&sit=all&fromseason={CURR_SEASON}&thruseason={CURR_SEASON}"
            r = _nst_get(url_season)
            if r.status_code != 200:
                print(f"NST season {abbr} HTTP {r.status_code}")
                continue
            season_rows = _parse_nst_player_rows(r.text)
            if not season_rows and team_code == "UTA":
                continue  # try fallback ARI in next loop
            # index by name
            by_name = {norm_name(x["Player"]): x for x in season_rows}

            # ---- Recent last 10 ----
            url_recent = f"https://www.naturalstattrick.com/playerteams.php?team={team_code}&sit=all&fromseason={CURR_SEASON}&thruseason={CURR_SEASON}&tgp=10"
            r2 = _nst_get(url_recent); time.sleep(SETTINGS["sleep_nst"])
            recent_rows = _parse_nst_player_rows(r2.text) if r2.status_code == 200 else []
            by_name_recent = {norm_name(x["Player"]): x for x in recent_rows}

            # ---- Last season ----
            url_last = f"https://www.naturalstattrick.com/playerteams.php?team={team_code}&sit=all&fromseason={LAST_SEASON}&thruseason={LAST_SEASON}"
            r3 = _nst_get(url_last); time.sleep(SETTINGS["sleep_nst"])
            last_rows = _parse_nst_player_rows(r3.text) if r3.status_code == 200 else []
            by_name_last = {norm_name(x["Player"]): x for x in last_rows}

            # blend
            for nkey, srow in by_name.items():
                name = srow["Player"]
                rrow = by_name_recent.get(nkey, {})
                lrow = by_name_last.get(nkey, {})

                def b3(col):
                    return blend_three_layer(
                        rrow.get(col), srow.get(col), lrow.get(col)
                    )

                all_rows.append({
                    "Team": abbr,
                    "Player": name,
                    "NameKey": f"{abbr}_{norm_name(name)}",
                    "G/60": b3("G/60"),
                    "A/60": b3("A/60"),
                    "SOG/60": b3("SOG/60"),
                    "BLK/60": b3("BLK/60"),
                    "CF/60": b3("CF/60"),
                    "CA/60": b3("CA/60"),
                    "xGF/60": b3("xGF/60"),
                    "xGA/60": b3("xGA/60"),
                    "HDCF/60": b3("HDCF/60"),
                    "HDCA/60": b3("HDCA/60"),
                    "TOI": srow.get("TOI")  # keep season TOI/GP as a rough base
                })
            break  # succeeded for this team (UTA or fallback ARI)
    df = pd.DataFrame(all_rows)
    return df

# ================== LINES (DFO → LWL fallback) ==================
def _parse_fixed_blocks(names):
    layout = [
        ("5v5 Line 1", 3), ("5v5 Line 2", 3), ("5v5 Line 3", 3), ("5v5 Line 4", 3),
        ("D Pair 1", 2), ("D Pair 2", 2), ("D Pair 3", 2),
        ("PP1", 5), ("PP2", 5), ("PK1", 4), ("PK2", 4)
    ]
    out, idx = [], 0
    for typ, size in layout:
        group = names[idx:idx+size]; idx += size
        if len(group) == size:
            out.append((typ, " – ".join([g.strip() for g in group])))
    return out

def fetch_lines_df() -> pd.DataFrame:
    rows = []
    for abbr in TEAMS:
        got = False
        slug = DFO_SLUG.get(abbr, "")
        # DFO
        try:
            if slug:
                url = f"https://www.dailyfaceoff.com/teams/{slug}/line-combinations/"
                r = requests.get(url, headers=HEADERS, timeout=50)
                if r.status_code == 200:
                    names = re.findall(r'data-player-name="([^"]+)"', r.text)
                    names = [n.strip() for n in names]
                    if names:
                        for typ, players in _parse_fixed_blocks(names):
                            rows.append({"Team": abbr, "Line Type": typ, "Line ID": f"{abbr}_{typ}", "Players": players, "Source": "DFO"})
                        got = True
        except Exception:
            pass
        # LWL fallback
        if not got:
            try:
                url = f"https://www.leftwinglock.com/line-combinations/team.php?team={abbr}"
                r = requests.get(url, headers=HEADERS, timeout=50)
                if r.status_code == 200:
                    names = re.findall(r'<td class="line-combination-player">([^<]+)', r.text)
                    names = [n.strip() for n in names]
                    if names:
                        for typ, players in _parse_fixed_blocks(names):
                            rows.append({"Team": abbr, "Line Type": typ, "Line ID": f"{abbr}_{typ}", "Players": players, "Source": "LWL"})
                        got = True
            except Exception:
                pass
        if not got:
            rows.append({"Team": abbr, "Line Type": "ERROR", "Line ID": f"{abbr}_ERR", "Players": "No data", "Source": "NONE"})
        time.sleep(SETTINGS["sleep_lines"])
    return pd.DataFrame(rows)

def explode_line_players(lines_df: pd.DataFrame) -> pd.DataFrame:
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

def build_line_chemistry(nst_players: pd.DataFrame, lines_df: pd.DataFrame) -> pd.DataFrame:
    if nst_players is None or nst_players.empty or lines_df is None or lines_df.empty:
        return pd.DataFrame()
    lp = explode_line_players(lines_df)
    if lp.empty: return pd.DataFrame()

    # Join NST stats onto each line-player
    nst = nst_players.copy()
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
    agg["Base Line Multiplier"] = agg["Line Type"].map(SETTINGS["LineMult_byType"]).fillna(1.0)
    return agg

# ================== GOALIES (NST) ==================
def parse_nst_goalie_table(html):
    """
    Parse NST goalie table for SV%. Tries to find rows with a player (goalie) link and SV% column.
    Returns list of dicts: {"Goalie": name, "SV": sv_as_fraction}
    """
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE|re.DOTALL)
    out = []
    for row in rows:
        m = re.search(r'goalie(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
        if not m:
            # many NST tables don’t use separate goalie link; fallback to generic anchor
            m = re.search(r'player(?:\.php\?id=|id=)\d+[^>]*>([^<]+)</a>', row, flags=re.IGNORECASE)
        if not m:
            continue
        name = m.group(1).strip()
        # find SV% value
        msv = re.search(r'(?:SV%|SV)\s*</?[^>]*>\s*([0-9]{1,2}\.?[0-9]*)', row, flags=re.IGNORECASE)
        sv = None
        if msv:
            try:
                v = float(msv.group(1))
                sv = v/100.0 if v > 1.5 else v
            except:
                sv = None
        if sv is not None:
            out.append({"Goalie": name, "SV": sv})
    return out

def fetch_nst_goalie_sv_three_layer() -> pd.DataFrame:
    """
    Pull SV% for Last 10 (recent), This Season, Last Season; three-layer blend per goalie.
    NST has a goalies table; paths vary, so we try multiple endpoints.
    """
    def get_table(from_season, thru_season, tgp=None):
        # Try a couple of common endpoints; stop at first success with data.
        qs = f"fromseason={from_season}&thruseason={thru_season}&sit=all"
        if tgp: qs += f"&tgp={tgp}"
        urls = [
            f"https://www.naturalstattrick.com/goalies.php?{qs}",
            f"https://www.naturalstattrick.com/playerlist.php?{qs}&pos=G"  # fallback attempt
        ]
        for u in urls:
            try:
                r = _nst_get(u)
                if r.status_code == 200:
                    rows = parse_nst_goalie_table(r.text)
                    if rows:
                        return rows
            except Exception:
                pass
        return []

    # season
    season = get_table(CURR_SEASON, CURR_SEASON)
    time.sleep(SETTINGS["sleep_nst"])
    # recent last 10
    recent = get_table(CURR_SEASON, CURR_SEASON, tgp=10)
    time.sleep(SETTINGS["sleep_nst"])
    # last season
    last = get_table(LAST_SEASON, LAST_SEASON)
    time.sleep(SETTINGS["sleep_nst"])

    by_name_s = {norm_name(x["Goalie"]): x["SV"] for x in season}
    by_name_r = {norm_name(x["Goalie"]): x["SV"] for x in recent}
    by_name_l = {norm_name(x["Goalie"]): x["SV"] for x in last}

    names = set(by_name_s.keys()) | set(by_name_r.keys()) | set(by_name_l.keys())
    rows = []
    for n in names:
        wsv = blend_three_layer(by_name_r.get(n), by_name_s.get(n), by_name_l.get(n))
        if wsv is None:
            wsv = LEAGUE_AVG_SV
        rows.append({"Goalie_norm": n, "Weighted_SV": wsv})
    return pd.DataFrame(rows)

# ================== PROJECTIONS ==================
def build_projections(dk_df: pd.DataFrame,
                      nst_players: pd.DataFrame,
                      lines_df: pd.DataFrame,
                      line_chem: pd.DataFrame):
    """
    Skaters: line-aware, opponent-neutral (for now); light goalie dampener placeholder (league avg).
    Goalies: built separately with NST three-layer SV% and opponent proxies.
    """
    if dk_df is None or dk_df.empty:
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

    # Split skaters / goalies
    sk = dk[~dk["Position"].str.contains("G", case=False, na=False)].copy()
    gk = dk[ dk["Position"].str.contains("G", case=False, na=False)].copy()

    # Attach NST blended per-60s to skaters
    if nst_players is not None and not nst_players.empty:
        nst = nst_players.copy()
        nst["PlayerKey"] = nst.apply(lambda r: f"{r.get('Team','')}_{norm_name(r.get('Player',''))}", axis=1)
        sk["PlayerKey"]  = sk.apply(lambda r: f"{r.get('Team','')}_{norm_name(r.get('Name',''))}", axis=1)
        sk = sk.merge(nst[["PlayerKey","G/60","A/60","SOG/60","BLK/60"]], on="PlayerKey", how="left")
    else:
        sk["G/60"] = sk["A/60"] = sk["SOG/60"] = sk["BLK/60"] = None

    # Fallbacks for rookies / missing
    def backfill_rates(row):
        role = guess_role_from_pos(row.get("Position",""))
        base = SETTINGS["D_fallback"] if role=="D" else SETTINGS["F_fallback"]
        return pd.Series({
            "G/60":   row["G/60"]   if pd.notna(row["G/60"])   else base["G60"],
            "A/60":   row["A/60"]   if pd.notna(row["A/60"])   else base["A60"],
            "SOG/60": row["SOG/60"] if pd.notna(row["SOG/60"]) else base["SOG60"],
            "BLK/60": row["BLK/60"] if pd.notna(row["BLK/60"]) else base["BLK60"]
        })
    sk[["G/60","A/60","SOG/60","BLK/60"]] = sk.apply(backfill_rates, axis=1)

    # Lines (5v5 + PP) and chemistry
    if lines_df is not None and not lines_df.empty:
        lp = explode_line_players(lines_df)
        fivev5 = lp[lp["Line Type"].str.startswith("5v5", na=False)].copy()
        ppl    = lp[lp["Line Type"].str.startswith("PP",   na=False)].copy()
        # join
        sk = sk.merge(fivev5[["PlayerKey","Line Type","Line ID"]].rename(
            columns={"Line Type":"Line","Line ID":"Line ID_5v5"}), on="PlayerKey", how="left")
        sk = sk.merge(ppl[["PlayerKey","Line Type","Line ID"]].rename(
            columns={"Line Type":"PP Line","Line ID":"PP Line ID"}), on="PlayerKey", how="left")
    else:
        sk["Line"] = ""; sk["Line ID_5v5"] = ""; sk["PP Line"] = ""; sk["PP Line ID"] = ""

    # Chemistry multiplier (base)
    if line_chem is not None and not line_chem.empty:
        chem = line_chem.rename(columns={"Line ID":"Line ID_5v5"})
        sk = sk.merge(chem[["Line ID_5v5","Line Type","xGF/60","SOG/60","Base Line Multiplier"]],
                      on="Line ID_5v5", how="left", suffixes=("","_LINE"))
        sk["Chemistry Mult"] = sk["Base Line Multiplier"].fillna(1.0) * (1.0 + ((sk["xGF/60"].fillna(5.0) - 5.0) / 25.0))
    else:
        sk["Chemistry Mult"] = 1.0

    # TOI splits (coarse defaults; replace later if you wire detailed TOI/PP data)
    sk["TOI_5v5"] = sk["Position"].apply(lambda p: 15.0 if "D" not in str(p).upper() else 19.0)
    sk["TOI_PP"]  = sk["PP Line"].apply(lambda pp: 3.6 if pp in ("PP1","PP2") else 1.0)
    sk["TOI_PK"]  =  sk["Position"].apply(lambda p: 1.0 if "D" in str(p).upper() else 0.4)

    toi5  = pd.to_numeric(sk["TOI_5v5"], errors="coerce").fillna(0)/60.0
    toipp = pd.to_numeric(sk["TOI_PP"],  errors="coerce").fillna(0)/60.0
    toipk = pd.to_numeric(sk["TOI_PK"],  errors="coerce").fillna(0)/60.0

    # Opponent factors (neutral until you add schedule/team-defense)
    sk["Opp_SOG_factor"] = SETTINGS["Opp_SOG_factor"]
    sk["Opp_xGA_factor"] = SETTINGS["Opp_xGA_factor"]
    sk["Opp_CA_factor"]  = SETTINGS["Opp_CA_factor"]

    # Base situation-blended projections (5v5 + PP for G/A/SOG; PK for BLK)
    sk["Proj Goals (base)"]   = sk["G/60"]   * (toi5 + toipp*0.9)
    sk["Proj Assists (base)"] = sk["A/60"]   * (toi5 + toipp*1.0)
    sk["Proj SOG (base)"]     = sk["SOG/60"] * (toi5 + toipp*1.1)
    sk["Proj Blocks (base)"]  = sk["BLK/60"] * (toi5 + toipk*1.4)

    # Apply chemistry & opponent factors
    sk["Proj Goals (chem)"]   = sk["Proj Goals (base)"]   * sk["Chemistry Mult"] * sk["Opp_xGA_factor"]
    sk["Proj Assists (chem)"] = sk["Proj Assists (base)"] * sk["Chemistry Mult"] * sk["Opp_xGA_factor"]
    sk["Proj SOG (chem)"]     = sk["Proj SOG (base)"]     * sk["Chemistry Mult"] * sk["Opp_SOG_factor"]
    sk["Proj Blocks (chem)"]  = sk["Proj Blocks (base)"]  * (0.5 + 0.5*sk["Opp_CA_factor"])

    # Light goalie dampener (we don’t know exact starter here → use league avg as a tiny nudge)
    sk["Goalie_Damp"] = light_goalie_dampener(LEAGUE_AVG_SV)
    sk["Adj Proj Goals"]   = sk["Proj Goals (chem)"]   * (1.0 + sk["Goalie_Damp"])
    sk["Adj Proj Assists"] = sk["Proj Assists (chem)"] * (1.0 + sk["Goalie_Damp"])
    sk["Adj Proj SOG"]     = sk["Proj SOG (chem)"]
    sk["Adj Proj Blocks"]  = sk["Proj Blocks (chem)"]

    # DK points & value
    pts = SETTINGS["DK_points"]
    salary = pd.to_numeric(sk["DK Salary"], errors="coerce")
    sk["Projected DK Points"] = (
        sk["Adj Proj Goals"]*pts["Goal"] +
        sk["Adj Proj Assists"]*pts["Assist"] +
        sk["Adj Proj SOG"]*pts["SOG"] +
        sk["Adj Proj Blocks"]*pts["Block"]
    )
    sk["DFS Value Score"] = (sk["Projected DK Points"] / salary.replace(0, math.nan)) * 1000.0

    # Final columns for DFS Projections
    cols = [
        "Name","Team","Position","Line","PP Line","TOI_5v5","TOI_PP","TOI_PK","DK Salary",
        "G/60","A/60","SOG/60","BLK/60",
        "Chemistry Mult","Opp_SOG_factor","Opp_xGA_factor","Opp_CA_factor","Goalie_Damp",
        "Adj Proj Goals","Adj Proj Assists","Adj Proj SOG","Adj Proj Blocks",
        "Projected DK Points","DFS Value Score","Line ID_5v5"
    ]
    for c in cols:
        if c not in sk.columns: sk[c] = ""
    dfs_proj = sk[cols].rename(columns={
        "Name":"Player",
        "TOI_5v5":"TOI_5v5 (min)","TOI_PP":"TOI_PP (min)","TOI_PK":"TOI_PK (min)"
    }).copy()

    # ----- TOP STACKS (5v5 lines only) -----
    if lines_df is not None and not lines_df.empty:
        lp = explode_line_players(lines_df)
        trio = lp[lp["Line Type"].str.match(r"^5v5 Line \d$", na=False)].copy()
        if trio.empty:
            top_stacks = pd.DataFrame()
        else:
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

    # Goalies built separately
    goalies = pd.DataFrame()

    return dfs_proj, top_stacks, goalies

def build_goalie_projections(dk_df: pd.DataFrame,
                             line_chem: pd.DataFrame,
                             goalie_sv_table: pd.DataFrame) -> pd.DataFrame:
    """
    Opponent-driven goalie projections with three-layer Weighted_SV.
    If we can't map opponent, we use neutral FALLBACK values.
    """
    # Build goalie list from DK
    if dk_df is None or dk_df.empty:
        return pd.DataFrame(columns=["Goalie","Team","Opponent","DK Salary","Proj Saves","Proj GA","DK Points","Weighted_SV"])
    dk = dk_df.copy()
    ren = {}
    for c in dk.columns:
        lc = c.lower()
        if lc == "name": ren[c] = "Name"
        if lc == "teamabbrev": ren[c] = "Team"
        if lc == "position": ren[c] = "Position"
        if lc == "salary": ren[c] = "DK Salary"
    dk = dk.rename(columns=ren)
    gk = dk[dk["Position"].astype(str).str.contains("G", case=False, na=False)].copy()
    if gk.empty:
        return pd.DataFrame(columns=["Goalie","Team","Opponent","DK Salary","Proj Saves","Proj GA","DK Points","Weighted_SV"])

    # Weighted SV% from NST three-layer table
    if goalie_sv_table is None or goalie_sv_table.empty:
        gk["Weighted_SV"] = LEAGUE_AVG_SV
    else:
        gk["Goalie_norm"] = gk["Name"].apply(norm_name)
        gk = gk.merge(goalie_sv_table, on="Goalie_norm", how="left")
        gk["Weighted_SV"] = gk["Weighted_SV"].fillna(LEAGUE_AVG_SV)

    # Opponent mapping not wired yet → neutral opponent proxies
    gk["Opponent"] = ""
    gk["Opp_SF60"]  = FALLBACK_SF60
    gk["Opp_xGF60"] = FALLBACK_xGF60

    # Projections
    gk["Proj Saves"] = gk["Opp_SF60"] * (GOALIE_TOI_MIN/60.0) * gk["Weighted_SV"]
    gk["Proj GA"]    = gk["Opp_xGF60"]* (GOALIE_TOI_MIN/60.0) * (1.0 - gk["Weighted_SV"])
    gk["DK Points"]  = gk["Proj Saves"]*0.7 - gk["Proj GA"]*3.5

    return gk.rename(columns={"Name":"Goalie"})[[
        "Goalie","Team","Opponent","DK Salary","Proj Saves","Proj GA","DK Points","Weighted_SV"
    ]]

# ================== MAIN ==================
def main():
    print("Reading DK salaries ...")
    dk = fetch_dk_salaries_csv_if_present()
    dk.to_csv("data/dk_salaries_echo.csv", index=False)

    print("Fetching NST player three-layer stats ...")
    nst_players = fetch_nst_player_stats_multi()
    nst_players.to_csv("data/nst_player_stats.csv", index=False)

    print("Fetching Lines (DFO → LWL fallback) ...")
    lines_df = fetch_lines_df()
    lines_df.to_csv("data/line_context.csv", index=False)

    print("Building line chemistry ...")
    line_chem = build_line_chemistry(nst_players, lines_df)
    line_chem.to_csv("data/line_chemistry.csv", index=False)

    print("Building skater projections (line-aware) ...")
    dfs_proj, top_stacks, _ = build_projections(dk, nst_players, lines_df, line_chem)
    dfs_proj.to_csv("data/dfs_projections.csv", index=False)
    top_stacks.to_csv("data/top_stacks.csv", index=False)

    print("Fetching NST goalie SV% (three-layer) ...")
    goalie_sv = fetch_nst_goalie_sv_three_layer()
    goalie_sv.to_csv("data/goalie_sv_table.csv", index=False)

    print("Building goalie projections (opponent-driven) ...")
    goalies = build_goalie_projections(dk, line_chem, goalie_sv)
    goalies.to_csv("data/goalies.csv", index=False)

    print("Done. CSVs written to /data")

if __name__ == "__main__":
    main()
