import pandas as pd
import os
import io
from datetime import datetime

def get_travel_type(row):
    o_country = str(row.get('origin_country', '')).strip().upper()
    d_country = str(row.get('destination_country', '')).strip().upper()

    # Treat nan/blank strings as "UNKNOWN"
    if o_country in ['NAN', '', 'NONE', 'NULL']: o_country = 'UNKNOWN'
    if d_country in ['NAN', '', 'NONE', 'NULL']: d_country = 'UNKNOWN'

    # Logic: If match OR if either is missing -> Domestic
    if (o_country == d_country) or (o_country == 'UNKNOWN') or (d_country == 'UNKNOWN'):
        return 'Domestic'
    else:
        return 'International'

def process_weekly_flights(uploaded_files, download_folder):
    """
    uploaded_files: List of FileStorage objects
    """
    summary_data = []

    for file in uploaded_files:
        filename = file.filename
        try:
            # 1. Read File
            try:
                temp_df = pd.read_csv(file, sep='\t')
                if temp_df.shape[1] < 2:
                    file.seek(0)
                    temp_df = pd.read_csv(file, sep=',')
            except:
                file.seek(0)
                temp_df = pd.read_csv(file, sep=',')
            
            # Clean Headers
            temp_df.columns = temp_df.columns.str.strip().str.lower()
            
            if temp_df.empty: continue

            # 2. Extract Date (Your Logic)
            current_date_str = "Unknown Date"
            if 'date_takeoff' in temp_df.columns and temp_df['date_takeoff'].notna().any():
                valid_date = temp_df['date_takeoff'].dropna().iloc[0]
                # Keep date as is for the record
                current_date_str = str(valid_date)

            # 3. Apply Travel Type Logic
            temp_df['Travel Type'] = temp_df.apply(get_travel_type, axis=1)

            # 4. Calculate Metrics
            total_intl = len(temp_df[temp_df['Travel Type'] == 'International'])
            total_dom = len(temp_df[temp_df['Travel Type'] == 'Domestic'])
            
            # International Departures from Nigeria
            intl_dep_ng = len(temp_df[
                (temp_df['Travel Type'] == 'International') &
                (temp_df['origin_country'].str.upper().str.contains('NIGERIA', na=False))
            ])

            # 5. Append
            summary_data.append({
                'Date': current_date_str,
                'Total International Flights': total_intl,
                'Total Domestic Flights': total_dom,
                'International Departures (from Nigeria)': intl_dep_ng
            })

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue

    if not summary_data:
        return None

    # 6. Create DataFrame & Sort
    final_summary_df = pd.DataFrame(summary_data)
    
    # Sort by date safely
    final_summary_df['SortDate'] = pd.to_datetime(final_summary_df['Date'], dayfirst=True, errors='coerce')
    final_summary_df = final_summary_df.sort_values('SortDate').drop(columns=['SortDate'])

    # 7. Save
    output_filename = f"Weekly_Flight_Summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    output_path = os.path.join(download_folder, output_filename)
    
    final_summary_df.to_excel(output_path, index=False)
    
    return output_filename