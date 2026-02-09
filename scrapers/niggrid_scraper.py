import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timedelta
import os

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

async def run_scraper(start_date, end_date, download_folder):
    TARGET_URL = "https://niggrid.org/GenerationProfile2"
    
    date_list = get_date_range(start_date, end_date)
    all_data = []
    
    print(f"--- Starting Scraper for {len(date_list)} days ---")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        await page.goto(TARGET_URL, timeout=60000)

        for current_date in date_list:
            date_website_fmt = current_date.strftime("%Y/%m/%d")
            short_date = current_date.strftime("%b-%d")
            
            try:
                # 1. Unlock Date Input
                await page.wait_for_selector("#MainContent_txtReadingDate")
                await page.evaluate("document.querySelector('#MainContent_txtReadingDate').removeAttribute('readonly');")
                await page.locator("#MainContent_txtReadingDate").fill(date_website_fmt)
                await page.evaluate("document.querySelector('#MainContent_txtReadingDate').dispatchEvent(new Event('change', { bubbles: true }))")
                
                # 2. Click Search (Try 'Get Generation', fallback to generic submit)
                try:
                    await page.get_by_role("button", name="Get Generation").click()
                except:
                    await page.click("input[type='submit']")
                
                # 3. Wait & Scroll
                await page.wait_for_timeout(3000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1000)
                
                # 4. Extract
                html = await page.content()
                dfs = pd.read_html(html)
                if dfs:
                    df = max(dfs, key=len).copy()
                    df.rename(columns={df.columns[1]: 'Raw_Name'}, inplace=True)
                    
                    # Clean & Tag (New stations get Title Cased here)
                    df['Station_Name'] = df['Raw_Name'].apply(standardize_name)
                    df['Date_Short'] = short_date
                    
                    all_data.append(df)
            except Exception as e:
                print(f"Error processing {date_website_fmt}: {e}")
                continue

        await browser.close()

    # --- DATA PROCESSING & FORMATTING ---
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        
        # Numeric Conversion
        hour_cols = [c for c in full_df.columns if ":00" in str(c)]
        if not hour_cols: hour_cols = full_df.columns[2:26]
        for col in hour_cols:
            full_df[col] = pd.to_numeric(full_df[col], errors='coerce').fillna(0)

        # Calculate Daily Total
        full_df['Daily_Total'] = full_df[hour_cols].sum(axis=1)

        # Pivot Matrix
        pivot = full_df.pivot_table(
            index='Station_Name', 
            columns='Date_Short', 
            values='Daily_Total', 
            aggfunc='sum'
        )

        # --- HYBRID SORTING LOGIC ---
        # 1. Get List of "Known" Stations (from Master List)
        known_stations = [x.title() for x in GENCO_MASTER_LIST]
        
        # 2. Get List of "New" Stations (Present in data but NOT in Master List)
        # We look at the pivot table's index to find what we actually scraped
        captured_stations = pivot.index.tolist()
        new_stations = [s for s in captured_stations if s not in known_stations]
        
        # 3. Sort "New" Stations Alphabetically
        new_stations.sort()
        
        # 4. Combine: Known First + New Alphabetical Second
        final_order = known_stations + new_stations
        
        # 5. Reindex (This forces the order AND includes 0-value rows for known stations)
        pivot = pivot.reindex(final_order, fill_value=0)

        # Add Totals
        pivot['MONTHLY_TOTAL'] = pivot.sum(axis=1)
        pivot.loc['DAILY_GRID_TOTAL'] = pivot.sum()

        # --- SAVE & FORMAT (GARAMOND) ---
        filename = f"NIGGRID_Report_{start_date}_to_{end_date}.xlsx"
        filepath = os.path.join(download_folder, filename)
        
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            pivot.to_excel(writer, sheet_name='Station_Totals')
            full_df.to_excel(writer, sheet_name='Raw_Data', index=False)
            
            workbook = writer.book
            
            # Styles
            header_fmt = workbook.add_format({'bold': True, 'font_name': 'Garamond', 'font_size': 12, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center'})
            body_fmt = workbook.add_format({'font_name': 'Garamond', 'font_size': 11})
            number_fmt = workbook.add_format({'font_name': 'Garamond', 'font_size': 11, 'num_format': '#,##0'})
            total_fmt = workbook.add_format({'bold': True, 'font_name': 'Garamond', 'font_size': 11, 'num_format': '#,##0', 'bg_color': '#F2F2F2'})
            
            ws = writer.sheets['Station_Totals']
            
            # Apply Column Widths & Formats
            ws.set_column(0, 0, 30, body_fmt)
            ws.set_column(1, len(pivot.columns)-1, 12, number_fmt)
            ws.set_column(len(pivot.columns), len(pivot.columns), 15, total_fmt) # Last col
            
            # Apply Header Format
            for col_num, value in enumerate(pivot.columns.values):
                ws.write(0, col_num + 1, value, header_fmt)
                
            # Apply Bottom Row Format
            ws.set_row(len(pivot), None, total_fmt)

        return filename
    return None