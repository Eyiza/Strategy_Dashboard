import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from bs4 import BeautifulSoup
import time
import random
from io import StringIO

# --- MASTER LIST (ORDER IS CRITICAL) ---
# Used for:
# 1. Matching (Specific names first to avoid partial match errors)
# 2. Reporting (These rows appear FIRST in this exact order)
GENCO_MASTER_LIST = [
    "AFAM III FAST POWER", "AFAM VI (GAS/STEAM)", "AZURA-EDO IPP (GAS)", 
    "DADINKOWA G.S (HYDRO)", "DELTA (GAS)", "EGBIN (STEAM)", 
    "GEREGU NIPP (GAS)",        # Checked before generic Geregu
    "GEREGU (GAS)",             # Generic
    "GPAL (GAS)", "IBOM POWER (GAS)", "IHOVBOR NIPP (GAS)", 
    "JEBBA (HYDRO)", "KAINJI (HYDRO)", "ODUKPANI NIPP (GAS)", "OKPAI (GAS/STEAM)", 
    "OLORUNSOGO NIPP (GAS)",    # Checked before generic
    "OLORUNSOGO (GAS)",         # Generic
    "OMOKU (GAS)", 
    "OMOTOSHO NIPP (GAS)",      # Checked before generic
    "OMOTOSHO (GAS)",           # Generic
    "PARAS ENERGY (GAS)", "RIVERS IPP (GAS)", 
    "SAPELE NIPP (GAS)",        # Checked before generic
    "SAPELE (STEAM)",           # Generic
    "SHIRORO (HYDRO)", "TRANS AFAM POWER", "TRANS-AMADI (GAS)", 
    "ZUNGERU", "KASHIMBILA GS"
]

def standardize_name(raw_name):
    """Matches raw website names to your official Master List."""
    if not isinstance(raw_name, str): return str(raw_name)
    clean_raw = raw_name.lower().strip()
    
    # 1. Try to find in Master List
    for master_name in GENCO_MASTER_LIST:
        clean_master_key = master_name.lower().split('(')[0].strip()
        if clean_master_key in clean_raw:
            return master_name.title() # Return Title Case
            
    # 2. If NO match found (New Station!), return it Title Cased
    return raw_name.title()

def get_date_range(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    return [start + timedelta(days=x) for x in range((end - start).days + 1)]



TARGET_URL = "https://niggrid.org/GenerationProfile2"

def get_hidden_fields(session):
    """Fetch page and extract ASP.NET hidden fields"""
    r = session.get(TARGET_URL)
    soup = BeautifulSoup(r.text, "lxml")

    hidden_inputs = {}
    for tag in soup.find_all("input", type="hidden"):
        hidden_inputs[tag.get("name")] = tag.get("value", "")

    return hidden_inputs

def fetch_day_data(session, date_str):
    r = session.get(TARGET_URL)
    soup = BeautifulSoup(r.text, "lxml")

    hidden_fields = {
        tag.get("name"): tag.get("value", "")
        for tag in soup.find_all("input", type="hidden")
    }

    payload = hidden_fields.copy()
    payload.update({
        "ctl00$MainContent$txtReadingDate": date_str,
        "ctl00$MainContent$btnSearch": "Get Generation",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": ""
    })

    response = session.post(TARGET_URL, data=payload)

    return response.text

def run_scraper(start_date, end_date, download_folder):
    os.makedirs(download_folder, exist_ok=True)

    date_list = get_date_range(start_date, end_date)
    all_data = []

    session = requests.Session()

    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Referer": TARGET_URL
    })

    for current_date in date_list:
        formatted_date = current_date.strftime("%Y/%m/%d")
        short_date = current_date.strftime("%b-%d")

        try:
            html = fetch_day_data(session, formatted_date)
            time.sleep(random.uniform(1.5, 3.0))
            print(type(html))
            if not html:
                print(f"No HTML returned for {formatted_date}")
                continue
            dfs = pd.read_html(StringIO(html), flavor="lxml")

            if dfs:
                df = max(dfs, key=len).copy()
                df.rename(columns={df.columns[1]: "Raw_Name"}, inplace=True)

                df["Station_Name"] = df["Raw_Name"].apply(standardize_name)
                df["Date_Short"] = short_date

                all_data.append(df)

        except Exception as e:
            # print(f"Error on {formatted_date}: {e}")
            print(f"Error on {formatted_date}: {type(e).__name__}")
            continue

    if not all_data:
        return None

    full_df = pd.concat(all_data, ignore_index=True)

    # Detect hour columns
    hour_cols = [c for c in full_df.columns if ":00" in str(c)]
    if not hour_cols:
        hour_cols = full_df.columns[2:26]

    for col in hour_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors="coerce").fillna(0)

    full_df["Daily_Total"] = full_df[hour_cols].sum(axis=1)

    pivot = full_df.pivot_table(
        index="Station_Name",
        columns="Date_Short",
        values="Daily_Total",
        aggfunc="sum"
    )

    # Sorting logic stays EXACTLY SAME as your original
    known_stations = [x.title() for x in GENCO_MASTER_LIST]
    captured_stations = pivot.index.tolist()
    new_stations = [s for s in captured_stations if s not in known_stations]
    new_stations.sort()

    final_order = known_stations + new_stations
    pivot = pivot.reindex(final_order, fill_value=0)

    pivot["MONTHLY_TOTAL"] = pivot.sum(axis=1)
    pivot.loc["DAILY_GRID_TOTAL"] = pivot.sum()

    filename = f"NIGGRID_Report_{start_date}_to_{end_date}.xlsx"
    filepath = os.path.join(download_folder, filename)

    pivot.to_excel(filepath)

    return filename