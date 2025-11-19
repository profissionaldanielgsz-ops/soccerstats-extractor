# scrape_soccerstats.py
# Script principal que extrai estatísticas do SoccerSTATS.
# Ajuste a URL da liga conforme necessário.

import requests, json, os, datetime
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

# ---------------- CONFIG ----------------

LEAGUE_URL = "https://www.soccerstats.com/latest.asp?league=england"  
# TROQUE por outra liga se quiser ex.: ?league=spain, ?league=italy

HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
OUT_DIR = Path("data")
MIN_TEAMS_EXPECTED = 8  

# ----------------------------------------


def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.text


def extract_tables(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    dfs = []
    for t in tables:
        try:
            df = pd.read_html(str(t))[0]
            dfs.append((t, df))
        except Exception:
            continue
    return dfs, soup


def find_team_table(dfs):
    # heurística: tabela com muitos nomes de times na primeira coluna
    for tag, df in dfs:
        if df.shape[1] < 2:
            continue
        first_col = df.iloc[:,0].astype(str)
        sample = first_col.head(8).tolist()
        name_like = sum(1 for v in sample if any(c.isalpha() for c in v))
        if name_like >= 3:
            return df
    return None


def load_aliases():
    try:
        with open("teams_aliases.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def normalize_name(name, aliases):
    key = name.strip()
    return aliases.get(key, key)


def validate_output(teams):
    return len(teams) >= MIN_TEAMS_EXPECTED


def main():
    print("Iniciando extração:", LEAGUE_URL)
    html = fetch(LEAGUE_URL)
    dfs, soup = extract_tables(html)
    team_table = find_team_table(dfs)
    aliases = load_aliases()
    date = datetime.date.today().isoformat()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    output = {
        "date_extracted": date,
        "source_url": LEAGUE_URL,
        "league": LEAGUE_URL.split("?")[-1],
        "teams": []
    }

    if team_table is None:
        if dfs:
            team_table = sorted(dfs, key=lambda x: x[1].shape[0], reverse=True)[0][1]

    if team_table is None:
        print("ERRO: nenhuma tabela encontrada.")
        with open(OUT_DIR / f"error_{date}.log", "w", encoding="utf-8") as f:
            f.write("No table found\n")
        return

    df = team_table.reset_index(drop=True)
    first_col = df.columns[0]

    for _, row in df.iterrows():
        try:
            team_raw = str(row[first_col]).strip()
            if team_raw == "" or team_raw.lower().startswith("team"):
                continue
            team = normalize_name(team_raw, aliases)

            metrics = {}
            cols = list(df.columns)
            for c in cols[1:6]:
                try:
                    metrics[str(c)] = str(row[c])
                except:
                    metrics[str(c)] = ""
            output["teams"].append({"team_raw": team_raw, "team": team, "metrics": metrics})
        except Exception:
            continue

    if not validate_output(output["teams"]):
        print("Validação falhou: poucos times extraídos.")
        with open(OUT_DIR / f"error_{date}.log", "w", encoding="utf-8") as f:
            f.write(json.dumps(output, ensure_ascii=False, indent=2))
        return

    json_path = OUT_DIR / f"{date}.json"
    csv_path = OUT_DIR / f"{date}.csv"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    rows = []
    for t in output["teams"]:
        row = {"date": date, "team": t["team"], "team_raw": t["team_raw"]}
        row.update(t["metrics"])
        rows.append(row)

    df_out = pd.DataFrame(rows)
    df_out.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"OK: {len(rows)} teams salvos -> {json_path} / {csv_path}")


if __name__ == "__main__":
    main()
