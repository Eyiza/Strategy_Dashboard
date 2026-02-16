import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from bs4 import BeautifulSoup

TARGET_URL = "https://niggrid.org/GenerationProfile2"

GENCO_MASTER_LIST = [
    "AFAM III FAST POWER", "AFAM VI (GAS/STEAM)", "AZURA-EDO IPP (GAS)", 
    "DADINKOWA G.S (HYDRO)", "DELTA (GAS)", "EGBIN (STEAM)", 
    "GEREGU NIPP (GAS)", "GEREGU (GAS)", "GPAL (GAS)", "IBOM POWER (GAS)",
    "IHOVBOR NIPP (GAS)", "JEBBA (HYDRO)", "KAINJI (HYDRO)",
    "ODUKPANI NIPP (GAS)", "OKPAI (GAS/STEAM)",
    "OLORUNSOGO NIPP (GAS)", "OLORUNSOGO (GAS)",
    "OMOKU (GAS)", "OMOTOSHO NIPP (GAS)", "OMOTOSHO (GAS)",
    "PARAS ENERGY (GAS)", "RIVERS IPP (GAS)",
    "SAPELE NIPP (GAS)", "SAPELE (STEAM)",
    "SHIRORO (HYDRO)", "TRANS AFAM POWER",
    "TRANS-AMADI (GAS)", "ZUNGERU", "KASHIMBILA GS"
]

def standardize_name(raw_name):
    if not isinstance(raw_name, str):
        return str(raw_name)

    clean_raw = raw_name.lower().strip()

    for master_name in GENCO_MASTER_LIST:
        clean_master_key = master_name.lower().split('(')[0].strip()
        if clean_master_key in clean_raw:
            return master_name.title()

    return raw_name.title()


def get_date_range(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    return [start + timedelta(days=x) for x in range((end - start).days + 1)]


def extract_hidden_fields(html):
    """Extract ASP.NET hidden fields required for POST"""
    soup = BeautifulSoup(html, "html.parser")

    fields = {}
    for field in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
        tag = soup.find("input", {"name": field})
        if tag:
            fields[field] = tag.get("value", "")

    return fields


def run_scraper(start_date, end_date, download_folder):
    session = requests.Session()
    all_data = []

    # First GET request to retrieve hidden form fields
    initial_response = session.get(TARGET_URL)
    hidden_fields = extract_hidden_fields(initial_response.text)

    date_list = get_date_range(start_date, end_date)

    for current_date in date_list:
        date_website_fmt = current_date.strftime("%Y/%m/%d")
        short_date = current_date.strftime("%b-%d")

        payload = {
            **hidden_fields,
            "MainContent$txtReadingDate": date_website_fmt,
            "MainContent$btnSubmit": "Get Generation"
        }

        response = session.post(TARGET_URL, data=payload)

        try:
            dfs = pd.read_html(response.text)
        except ValueError:
            continue

        if dfs:
            df = max(dfs, key=len).copy()
            df.rename(columns={df.columns[1]: "Raw_Name"}, inplace=True)
            df["Station_Name"] = df["Raw_Name"].apply(standardize_name)
            df["Date_Short"] = short_date
            all_data.append(df)

    if not all_data:
        return None

    # Combine all days
    full_df = pd.concat(all_data, ignore_index=True)

    # Convert numeric columns
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

    with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:
        pivot.to_excel(writer, sheet_name="Station_Totals")
        full_df.to_excel(writer, sheet_name="Raw_Data", index=False)

    return filename