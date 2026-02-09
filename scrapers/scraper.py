import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timedelta
import os

# --- MASTER LIST ---
GENCO_MASTER_LIST = [
    "AFAM III FAST POWER", "AFAM VI (GAS/STEAM)", "AZURA-EDO IPP (GAS)", 
    "DADINKOWA G.S (HYDRO)", "DELTA (GAS)", "EGBIN (STEAM)", "GEREGU (GAS)", 
    "GEREGU NIPP (GAS)", "GPAL (GAS)", "IBOM POWER (GAS)", "IHOVBOR NIPP (GAS)", 
    "JEBBA (HYDRO)", "KAINJI (HYDRO)", "ODUKPANI NIPP (GAS)", "OKPAI (GAS/STEAM)", 
    "OLORUNSOGO (GAS)", "OLORUNSOGO NIPP (GAS)", "OMOKU (GAS)", "OMOTOSHO (GAS)", 
    "OMOTOSHO NIPP (GAS)", "PARAS ENERGY (GAS)", "RIVERS IPP (GAS)", 
    "SAPELE (STEAM)", "SAPELE NIPP (GAS)", "SHIRORO (HYDRO)", "TRANS AFAM POWER", 
    "TRANS-AMADI (GAS)", "ZUNGERU", "KASHIMBILA GS"
]

def standardize_name(raw_name):
    if not isinstance(raw_name, str): return str(raw_name)
    clean_raw = raw_name.lower().strip()
    for master_name in GENCO_MASTER_LIST:
        clean_master = master_name.lower().split('(')[0].strip()
        if clean_master in clean_raw or clean_raw in clean_master:
            return master_name.title()
    return raw_name.title()

def get_date_range(start_date, end_date):
    # Convert string 'YYYY-MM-DD' to date objects
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return [start + timedelta(days=x) for x in range((end - start).days + 1)]

async def run_scraper(start_date_str, end_date_str, download_folder):
    TARGET_URL = "https://niggrid.org/GenerationProfile2"
    
    date_list = get_date_range(start_date_str, end_date_str)
    all_data = []
    
    print(f"Starting scrape for {len(date_list)} days...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(TARGET_URL, timeout=60000)

        for current_date in date_list:
            date_website_fmt = current_date.strftime("%Y/%m/%d") # Site needs YYYY/MM/DD
            short_date = current_date.strftime("%b-%d")
            
            try:
                # Unlock and Fill Date
                await page.wait_for_selector("#MainContent_txtReadingDate")
                await page.evaluate("document.querySelector('#MainContent_txtReadingDate').removeAttribute('readonly');")
                await page.locator("#MainContent_txtReadingDate").fill(date_website_fmt)
                await page.evaluate("document.querySelector('#MainContent_txtReadingDate').dispatchEvent(new Event('change', { bubbles: true }))")
                
                # Click 'Get Generation' or fallback
                try:
                    await page.get_by_role("button", name="Get Generation").click()
                except:
                    await page.click("input[type='submit']")
                
                await page.wait_for_timeout(3000) # Wait for load
                
                # Extract
                html = await page.content()
                dfs = pd.read_html(html)
                if dfs:
                    df = max(dfs, key=len).copy()
                    df.rename(columns={df.columns[1]: 'Raw_Name'}, inplace=True)
                    df['Station_Name'] = df['Raw_Name'].apply(standardize_name)
                    df['Date_Short'] = short_date
                    
                    # Numeric conversion logic
                    hour_cols = [c for c in df.columns if ":00" in str(c)]
                    if not hour_cols: hour_cols = df.columns[2:26]
                    for col in hour_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        
                    df['Daily_Total'] = df[hour_cols].sum(axis=1)
                    
                    all_data.append(df)
            except Exception as e:
                print(f"Error on {date_website_fmt}: {e}")
                continue

        await browser.close()

    if all_data:
        # Build Final Matrix
        full_df = pd.concat(all_data)
        
        pivot = full_df.pivot_table(index='Station_Name', columns='Date_Short', values='Daily_Total', aggfunc='sum')
        pivot = pivot.reindex([x.title() for x in GENCO_MASTER_LIST], fill_value=0)
        pivot['MONTHLY_TOTAL'] = pivot.sum(axis=1)
        pivot.loc['GRID_TOTAL'] = pivot.sum()

        # Save File
        filename = f"NIGGRID_Report_{start_date_str}_to_{end_date_str}.xlsx"
        filepath = os.path.join(download_folder, filename)
        
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            pivot.to_excel(writer, sheet_name='Matrix')
            full_df.to_excel(writer, sheet_name='Raw_Data', index=False)
            
        return filename
    return None