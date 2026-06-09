import pandas as pd
import os
import numpy as np

project_root = '../../' 
raw_dir = os.path.join(project_root, 'data', 'po_entry') 
reference_dir = os.path.join(project_root, 'data', 'reference')


sheet_id = '1EZ7kPPvnRqvR5UN0Vi0NNLpLTNXEArzRklsVTIGb1vc'
exportformat = 'gviz/tq?tqx=out:csv&gid='


normalisasi_rfm_id = '0'
normalisasi_po_id = '1138035324'
notcounted_po_id = '2061221686'
normalisasi_logistic_id = '822694285'
nonworkdays_id = '632183983'
wilayah_id = '723767888'
pulau_id = '410190247'
timedate_id = '1205634597'
cost_saving_id = '1828930868'
jasa_service_id = '1312001151'
freight_id = '1063908444'
rara_id = '394331579'
ryi_id = '2095297594'
way_id = '532810996'
sln_id = '744466142'
normalisasi_rfm_solar_id = '930974516'
lebaran_dates_id = '1186672226'


normalisasi_rfm_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{normalisasi_rfm_id}'
normalisasi_po_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{normalisasi_po_id}'
notcounted_po_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{notcounted_po_id}'
normalisasi_logistic_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{normalisasi_logistic_id}'
nonworkdays_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{nonworkdays_id}'
wilayah_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{wilayah_id}'
pulau_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{pulau_id}'
timedate_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{timedate_id}'
cost_saving_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{cost_saving_id}' 
jasa_service_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{jasa_service_id}'
freight_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{freight_id}'
rara_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{rara_id}'
ryi_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{ryi_id}'
way_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{way_id}'
sln_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{sln_id}'
normalisasi_rfm_solar_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{normalisasi_rfm_solar_id}'
lebaran_dates_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/{exportformat}{lebaran_dates_id}'

def load_all_data(refresh_cache=False):
    data = {}
    
    # 1. Load Main Data with automatic timestamp-based caching
    xlsx_path = os.path.join(raw_dir, "PO Entry List.xlsx")
    parquet_path = os.path.join(raw_dir, "PO_Entry_List.parquet")
    
    try:
        if os.path.exists(xlsx_path):
            xlsx_mtime = os.path.getmtime(xlsx_path)
            # Check if parquet exists and is newer than xlsx
            if os.path.exists(parquet_path) and os.path.getmtime(parquet_path) > xlsx_mtime:
                print("Loading PO Entry List from Parquet cache...")
                data['df'] = pd.read_parquet(parquet_path, engine='pyarrow')
            else:
                print("Excel file changed or cache missing. Loading PO Entry List.xlsx (takes ~45s)...")
                data['df'] = pd.read_excel(xlsx_path)
                data['df'].to_parquet(parquet_path, index=False, engine='pyarrow')
                print("Saved PO Entry List to Parquet cache.")
        else:
            # Fallback if xlsx_path does not exist but parquet does (e.g. production/offline)
            if os.path.exists(parquet_path):
                print("PO Entry List.xlsx not found, loading from existing Parquet cache...")
                data['df'] = pd.read_parquet(parquet_path, engine='pyarrow')
            else:
                raise FileNotFoundError(f"Neither {xlsx_path} nor {parquet_path} exists.")
    except Exception as e:
        print(f"Error loading PO Entry List: {e}")
        raise

    # 2. Define Reference Sheets mapping
    ref_sheets = {
        'holidays_df': ('holidays.parquet', nonworkdays_path),
        'wilayah_df': ('wilayah.parquet', wilayah_path),
        'pulau_df': ('pulau.parquet', pulau_path),
        'jasa_service_df': ('jasa_service.parquet', jasa_service_path),
        'cost_saving_df': ('cost_saving.parquet', cost_saving_path),
        'freight_df': ('freight.parquet', freight_path),
        'rara_df': ('rara.parquet', rara_path),
        'ryi_df': ('ryi.parquet', ryi_path),
        'way_df': ('way.parquet', way_path),
        'sln_df': ('sln.parquet', sln_path),
        'rfm_normalized_df': ('rfm_normalized.parquet', normalisasi_rfm_path),
        'ontime_normalized_df': ('ontime_normalized.parquet', normalisasi_po_path),
        'timedate_normalized_df': ('timedate_normalized.parquet', timedate_path),
        'notcounted_df': ('notcounted.parquet', notcounted_po_path),
        'logistic_normalized_df': ('logistic_normalized.parquet', normalisasi_logistic_path),
        'normalisasi_rfm_solar_df': ('normalisasi_rfm_solar.parquet', normalisasi_rfm_solar_path),
        'lebaran_dates_df': ('lebaran_dates.parquet', lebaran_dates_path),
    }

    # Ensure reference directory exists
    if not os.path.exists(reference_dir):
        os.makedirs(reference_dir)

    # 3. Load or download references
    for key, (pq_name, url_path) in ref_sheets.items():
        pq_path = os.path.join(reference_dir, pq_name)
        
        # Load from Parquet if cache exists and refresh_cache is False
        if os.path.exists(pq_path) and not refresh_cache:
            try:
                data[key] = pd.read_parquet(pq_path, engine='pyarrow')
            except Exception as e:
                print(f"Error reading cache for {key}, will re-download: {e}")
                # Force re-download
                refresh_cache = True

        # Download from URL if refresh_cache is True or local cache is missing
        if key not in data:
            print(f"Downloading {key} from Google Sheets...")
            try:
                data[key] = pd.read_csv(url_path)
                # Save to cache
                data[key].to_parquet(pq_path, index=False, engine='pyarrow')
            except Exception as e:
                print(f"Failed to download {key} from {url_path}: {e}")
                # Special fallback for lebaran_dates_df
                if key == 'lebaran_dates_df':
                    print("Using hardcoded fallback defaults for Lebaran dates...")
                    default_lebaran_df = pd.DataFrame([
                        {"YEAR": [2025, 2026], "START_DATE": ["2025-03-28", "2026-03-18"], "END_DATE": ["2025-04-13", "2026-03-31"]}
                    ])
                    # Fix dataframe layout
                    default_lebaran_df = pd.DataFrame([
                        {"YEAR": 2025, "START_DATE": "2025-03-28", "END_DATE": "2025-04-13"},
                        {"YEAR": 2026, "START_DATE": "2026-03-18", "END_DATE": "2026-03-31"}
                    ])
                    data[key] = default_lebaran_df
                    # Cache the fallback so we don't try downloading it again unless forced
                    default_lebaran_df.to_parquet(pq_path, index=False, engine='pyarrow')
                else:
                    # Check if cache exists as fallback even if refresh_cache was True
                    if os.path.exists(pq_path):
                        print(f"Loading cached version of {key} as fallback...")
                        data[key] = pd.read_parquet(pq_path, engine='pyarrow')
                    else:
                        raise e

    return data

if __name__ == '__main__':
    load_all_data()