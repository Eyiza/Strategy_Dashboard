import pandas as pd
import io
import re
import os
import datetime

# --- CONFIGURATION ---
CITY_TO_STATE_DB = {
    'ikeja': 'Lagos', 'lagos': 'Lagos', 'port harcourt': 'Rivers', 'uyo': 'Akwa Ibom',
    'benin': 'Edo', 'benin city': 'Edo', 'calabar': 'Cross River',
    'asaba': 'Delta', 'warri': 'Delta', 'osubi': 'Delta', 'enugu': 'Enugu',
    'kano': 'Kano', 'kaduna': 'Kaduna', 'zaria': 'Kaduna', 'owerri': 'Imo',
    'ilorin': 'Kwara', 'maiduguri': 'Borno', 'sokoto': 'Sokoto', 'yola': 'Adamawa',
    'akure': 'Ondo', 'iperu': 'Ogun', 'ibadan': 'Oyo', 'jos': 'Plateau',
    'makurdi': 'Benue', 'minna': 'Niger', 'abuja': 'Abuja', 'lekki': 'Lagos'
}

def standardize_airport_name(name):
    n = str(name).strip().lower()
    if 'murtala' in n or 'muritala' in n: return 'Murtala Muhammed International Airport'
    elif 'nnamdi' in n or 'azikiwe' in n: return 'Nnamdi Azikiwe International Airport'
    elif 'aminu kano' in n: return 'Mallam Aminu Kano International Airport'
    elif 'akanu ibiam' in n or 'enugu' in n: return 'Akanu Ibiam International Airport'
    elif 'sam mbakwe' in n or 'owerri' in n: return 'Sam Mbakwe International Cargo Airport'
    elif 'margaret ekpo' in n or 'calabar' in n: return 'Margaret Ekpo International Airport'
    elif 'port harcourt' in n: return 'Port Harcourt International Airport'
    elif 'yakubu gowon' in n or 'jos' in n: return 'Yakubu Gowon Airport'
    elif 'sadiq abubakar' in n or 'sultan saddik' in n: return 'Sadiq Abubakar III International Airport'
    elif 'tunde idiagbon' in n or 'ilorin' in n: return 'Ilorin Airport'
    elif 'kaduna' in n: return 'Kaduna International Airport'
    elif 'zaria' in n: return 'Zaria Airport'
    elif 'benin' in n: return 'Benin Airport'
    else: return str(name).strip()

def process_flight_files(uploaded_files, target_month, target_year, download_folder):
    """
    uploaded_files: List of FileStorage objects from Flask
    target_month: int
    target_year: int
    """
    all_daily_data = []
    
    # --- 1. FILE INGESTION ---
    for file in uploaded_files:
        filename = file.filename
        try:
            # Read directly from memory (Flask FileStorage)
            try:
                temp_df = pd.read_csv(file, sep='\t')
                if temp_df.shape[1] < 2: 
                    file.seek(0) # Reset pointer
                    temp_df = pd.read_csv(file, sep=',')
            except:
                file.seek(0)
                temp_df = pd.read_csv(file, sep=',')

            # Janitor
            temp_df.columns = temp_df.columns.str.strip().str.lower()
            if temp_df.empty: continue

            # Smart Date Logic (Preserved from your script)
            if 'date_takeoff' in temp_df.columns and temp_df['date_takeoff'].notna().any():
                raw_val = temp_df['date_takeoff'].dropna().iloc[0]
                day_num = 1
                try:
                    # Handle Excel Serial Dates
                    if isinstance(raw_val, (int, float)) or (isinstance(raw_val, str) and raw_val.isdigit()):
                        serial = float(raw_val)
                        temp_date = pd.Timestamp('1899-12-30') + pd.to_timedelta(serial, unit='D')
                        day_num = temp_date.day
                    else:
                        # Handle String Dates
                        raw_str = str(raw_val).strip()
                        if re.match(r'^\d{4}', raw_str):
                            temp_date = pd.to_datetime(raw_str, errors='raise')
                        else:
                            temp_date = pd.to_datetime(raw_str, dayfirst=True, errors='raise')
                        day_num = temp_date.day
                except:
                    pass # Keep day_num = 1 if parse fails

                forced_date = f"{day_num}/{target_month}/{target_year}"
                temp_df['date_takeoff'] = forced_date
            else:
                # No date column found
                temp_df['date_takeoff'] = f"01/{target_month}/{target_year}"

            all_daily_data.append(temp_df)
            
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue

    if not all_daily_data:
        return None

    # --- 2. MERGE & PROCESS ---
    df = pd.concat(all_daily_data, ignore_index=True)

    # Logic: Travel Type
    def get_travel_type(row):
        o = str(row.get('origin_country', '')).strip().upper()
        d = str(row.get('destination_country', '')).strip().upper()
        if o in ['NAN', '', 'NONE', 'NULL', 'NAM']: o = 'UNKNOWN'
        if d in ['NAN', '', 'NONE', 'NULL', 'NAM']: d = 'UNKNOWN'
        return 'Domestic' if (o == d) or (o == 'UNKNOWN') or (d == 'UNKNOWN') else 'International'
    
    df['Travel Type'] = df.apply(get_travel_type, axis=1)

    # Logic: Categories
    def map_category(st):
        s = str(st).strip().lower()
        if s in ['general aviation', 'other', 'others', 'non-categorised']: return 'General Aviation'
        if s == 'passenger': return 'Commercial'
        if 'business' in s: return 'Private'
        return str(st).strip()
    
    if 'service_type' in df.columns:
        df['Category of Flight'] = df['service_type'].apply(map_category)
    else:
        df['Category of Flight'] = 'Unknown'

    # Logic: Unpivot (Split Dep/Arr)
    dep_cols = {'origin_city': 'Airport State', 'origin_name': 'Airport Name', 'origin_country': 'Airport Country', 'date_takeoff': 'Date'}
    arr_cols = {'destination_city': 'Airport State', 'destination_name': 'Airport Name', 'destination_country': 'Airport Country', 'date_takeoff': 'Date'}
    common = ['Travel Type', 'Category of Flight']

    df_dep = df[list(dep_cols.keys()) + common].copy().rename(columns=dep_cols)
    df_dep['Flight Status'] = 'Departure'
    
    df_arr = df[list(arr_cols.keys()) + common].copy().rename(columns=arr_cols)
    df_arr['Flight Status'] = 'Arrival'

    df_final = pd.concat([df_dep, df_arr], ignore_index=True)

    # Logic: Filter Nigeria & Map States
    df_final = df_final[df_final['Airport Country'].str.upper().str.contains('NIGERIA', na=False)]
    
    df_final['Mapped_State'] = df_final['Airport State'].astype(str).str.strip().str.lower().map(CITY_TO_STATE_DB)
    df_final['Airport State'] = df_final['Mapped_State'].fillna(df_final['Airport State'])
    
    df_final['Airport Name'] = df_final['Airport Name'].apply(standardize_airport_name)

    # Logic: Dates
    df_final['Date'] = pd.to_datetime(df_final['Date'], dayfirst=True, errors='coerce')
    df_final['Year'] = df_final['Date'].dt.year
    df_final['Month Name'] = df_final['Date'].dt.month_name()

    # Logic: Group By
    output_cols = ['Airport Name', 'Airport State', 'Category of Flight', 'Travel Type', 'Month Name', 'Year', 'Flight Status']
    report = df_final.groupby(output_cols).size().reset_index(name='Number of Flights')

    # --- 3. SAVE ---
    out_name = f"Flight_Data_Summary_{target_month}_{target_year}.xlsx"
    out_path = os.path.join(download_folder, out_name)

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        report.to_excel(writer, index=False, sheet_name='Report')
        worksheet = writer.sheets['Report']
        # Auto-width
        for i, col in enumerate(report.columns):
            max_len = max(report[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_len)
            
    return out_name