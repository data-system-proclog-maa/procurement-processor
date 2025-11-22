import pandas as pd
import numpy as np
import datetime as dt
import re
import math

#Lebaran blockout dates
lebaran_2025 = pd.to_datetime(pd.date_range(start='2025-03-28', end='2025-04-13')).date
#lebaran_2026 = pd.to_datetime(pd.date_range(start='2026-02-16', end='2026-03-05')).date

#combined_dates = list(lebaran_2025) + list(lebaran_2026)
lebaran_dates = set(lebaran_2025) #change into set(combined_dates) when 2026 is added

#extract LOC from Department column
def LOC_strings(s):
    if pd.isna(s):
        return 'Unknown'
   
   #remove X and whitespace if exist in the beginning     
    s = s[1:].strip().upper() if str(s).upper().startswith('X') else s.strip().upper()
    
    #loc mapping for HO endings
    if s.endswith('-HO'):
        return "HO"

    #loc mapping for LC endings
    LOCATION_MAP = {
        'PALU': 'PALU', 'FLUK': 'FLUK', 'LAR': 'LAR', 'LWK': 'LWK', 
        'OBI': 'OBI', 'KDI': 'KDI', 'BARU': 'BARU', 'MUNA': 'MUNA', 
        'LWI': 'LWI', 'POM': 'POM', 'KNW': 'KNW', 'WATU': 'WATU', 'LAEYA': 'LAEYA'
    }
    
    loc_type = 'HO' if '__' in s else 'LC'
    
    #Use regex to find location from the start of the string up to the first delimiter
    match = re.search(r'([A-Z0-9]+)', s)
    location_key = match.group(1) if match else ''

    location = LOCATION_MAP.get(location_key, '')
    
    #Fallback/Correction: Search for location keywords throughout the string
    if not location:
        for key, val in LOCATION_MAP.items():
            if key in s:
                location = val
                break
    
    if loc_type == 'HO' and location:
        return f"HO {location}".strip()
    
    return f"{loc_type} {location}".strip()

#extract dept name from Department column
def project_string(s):
    if pd.isna(s):
        return None
     
    #remove X and whitespace if exist in the beginning      
    s = s[1:].strip().upper() if str(s).upper().startswith('X') else s.strip().upper()
    
    parts = s.split('-')[0].split('_')[0]
    
    #Project mapping for specific PT
    if parts == "IMS":
        if "147" in s: return "IMS 147"
        if "52" in s: return "IMS 52"
        return "IMS"
    
    if parts == "MPS":
        return "MPS SC" if "SC" in s else "MPS"

    if parts == "MMP":
        suffix_map = {
            'SC': 'SC', 'HO': 'HO', 'LAR': 'LAR', 'WATU': 'WATU', 
            'OBI': 'OBI', 'POM': 'POM', 'LAEYA': 'LAEYA', 'KDI': 'LAR',
            'BARU': 'BARU', 'LWI': 'LWI', 'SOL': 'SOL' 
        }
        
        # Check if the string ends with any key in the map
        for key, suffix in suffix_map.items():
            if s.endswith(key):
                return f"MMP {suffix}"
        
        return "MMP"
        
    return parts

#extract division name from Department column
def divisi_string(s):
    if pd.isna(s):
        return None
    parts = str(s).split('-')
    if len(parts) > 1:
        sub_parts = parts[1].split('_')
        first_part = sub_parts[0]
        if "HRGA" in first_part:
            return "HRGA"
        return first_part
    return None

#extract supplier type PT/CV/Lainnya
def PTCV_strings(s):
    if pd.isna(s):
        return np.nan #return nan if empty
    if str(s).startswith('PT'): return 'PT'
    if str(s).startswith('CV'): return 'CV'
    return 'Toko/Lainnya'

#categorizing Term of Payment
def TOP_strings(s):
    if pd.isna(s): return np.nan
    s = str(s).lower().strip()
    
    progressive_keywords = ['dp', 'downpayment', 'down payment', 'pembayaran 1 ', '50% sebelum', 'kredit', 'tahap', 'leasing', 'installment']
    tbd_keywords = ['pembayaran sebelum', 'before delivery', 'sebelum pengiriman', 'cash', 'uang muka', '100% sebelum', 'tunai', 'seelum', 'sebeulm', 'transfer', 'pengiriman setelah pembayaran', 'setelah pembayaran','100% di muka']
    tempo_keywords = ['hari setelah', 'hari dari', 'tempo', 'invoice diterima', 'penagihan dilakukan', 'setelah pengiriman', 'pembayaran setelah', 'kontrak', 'after delivery', 'hari kerja', 'telah diterima', 'pembayaran per bulan', 'ari', '0', 'pekerjaan pengujian dilakukan setelah pembayaran dilakukan']
    
    if any(keyword in s for keyword in progressive_keywords): return 'Progressive'
    if any(keyword in s for keyword in tbd_keywords): return 'TBD'
    if any(keyword in s for keyword in tempo_keywords): return 'Tempo'
    return 'Not Applicable'

#categorization within Item Category for CATEGORYMERGED 
def item_category_merged(item_category, unit=None):
    if not isinstance(item_category, str):
        return item_category
    
    item_category_upper = item_category.upper().strip()
    unit_upper = str(unit).upper().strip() if isinstance(unit, str) else ""

    if 'XCMG' in item_category_upper:
        return 'Spare Part XCMG'
    if 'SANY' in item_category_upper:
        return 'Spare Part SANY'
    if 'ZS' in item_category_upper:
        return 'Spare Part ZS'

    if 'TIRE DT' in item_category_upper:
        return 'Tire DT - Set' if 'SET' in unit_upper else 'Tire DT - non Set'

    return item_category

#Value marker, for more info check documentation
def category_value_marker(row):
    """Marks categories that should not be counted for performance."""
    specific_categories = ["Kontrak", "Seragam", "Jasa Logistik", "Jasa/Service", "ATK", "Cetak", "Makanan dan Minuman", "Seragam Security", "x Kebutuhan Kantin", "x Kebutuhan Mess", "x Medical dan Obat"]
    item_category = str(row['Item Category']).strip()   
    requisition_type = str(row['Requisition Type']).strip()
    item_name = str(row['Item Name']).strip().lower()
    
    if item_category in specific_categories or requisition_type == "Consignment" or (item_category == "APD" and "sepatu" in item_name):
        return 1
    return 0

def category_value_xcmg(row):
    requisition_type = row['Requisition Type']
    item_category = row['Item Category']
    background_needs = row['Background Needs']
    keywords = ["Pengambilan", "Berita acara pengeluaran", "BA", "Consignment"]
    
    if (isinstance(requisition_type, str) and requisition_type != "Consignment" and
        isinstance(item_category, str) and "XCMG" in item_category and
        isinstance(background_needs, str) and any(keyword in background_needs for keyword in keywords)):
        return 1
    return 0

#override routine with parameter inside
def apply_routine_logic(df_series_routine, category_series, item_name_series, pic_name_series):
    """
    Applies the complex, multi-rule categorization logic to determine if an item is '_Routine'.
    
    :param df_series_routine: The initial 'Routine' column Series (used as a base).
    :param category_series: The 'CATEGORYMERGED' (lowercase) Series.
    :param item_name_series: The 'Item Name' (lowercase) Series.
    :param pic_name_series: The 'Procurement Name' (lowercase) Series.
    :return: The updated '_Routine' status Series.
    """
    df_series_routine = df_series_routine.copy()
    
    nonroutine_pics = [
        'rizal agus fianto', 'syifa alifia', 'syifa ramadhani luthfi',
        'linda permata sari', 'puji astuti', 'laurensius adi', 'stheven immanuel'
    ]
    
    #define rules as a list of (condition_mask, value)
    rules = [
        ((category_series == 'aksesoris kendaraan') & (item_name_series.str.contains('lampu rotary', na=False)), 'Routine'),
        ((category_series == 'alat hiburan') & (item_name_series.str.contains('shuttlecock|cock', na=False)), 'Routine'),
        ((category_series == 'apd') & (item_name_series.str.contains('helm|kacamata|kaca mata|rompi|masker medis|safety shoes|tali|backsupport', na=False)), 'Routine'),
        (category_series == 'cetak', 'Non-Routine'),
        (category_series == 'container & part', 'Non-Routine'),
        ((category_series == 'elektrikal') & (pic_name_series.isin(nonroutine_pics)), 'Non-Routine'),
        ((category_series == 'karoseri ft') & (item_name_series.str.contains('filter', na=False)), 'Routine'),
        ((category_series == 'karoseri ft') & (~item_name_series.str.contains('filter', na=False)), 'Non-Routine'),
        (category_series == 'karoseri lt', 'Non-Routine'),
        (category_series == 'peralatan dapur', 'Non-Routine'),
        ((category_series == 'peralatan shipping') & (item_name_series.str.contains('terpal', na=False)), 'Routine'),
        ((category_series == 'peralatan shipping') & (~item_name_series.str.contains('terpal', na=False)), 'Non-Routine'),
        ((category_series == 'peralatan survey') & (item_name_series.str.contains('flagging tape', na=False)), 'Routine'),
        ((category_series == 'peralatan survey') & (~item_name_series.str.contains('flagging tape', na=False)), 'Non-Routine'),
        (category_series == 'telepon', 'Non-Routine'),
        (category_series == 'tire dt', 'Routine'),
        ((category_series == 'tire innova') & (item_name_series.str.contains('delium', na=False)), 'Routine'),
        ((category_series == 'tire manhaul') & (item_name_series.str.contains('gt|gajah tunggal', na=False)), 'Routine'),
        (category_series == 'tire tl', 'Non-Routine'),
        (category_series == 'tire vb', 'Routine'),
        (category_series == 'radio ht, rig', 'Non-Routine'),
        (category_series == 'packaging', 'Routine')
    ]
    
    #apply all rules sequentially 
    for condition, value in rules:
        df_series_routine.loc[condition] = value
        
    return df_series_routine
    
#lead time for HO,Local Site; Sulawesi Area and Halmahera Area
def urgent_normal_function(row):
    if row['Requisition Type'] == 'Consignment': return 'Normal'
    
    loc = str(row['LOC']) if not pd.isnull(row['LOC']) else ''
    lead_time = row['LEAD TIME']
    if pd.isna(lead_time): return 'Normal'

    #hardcoded logic
    if (loc == 'HO' and lead_time <= 15) or \
       ('LC' in loc and lead_time <= 15) or \
       (loc in ['HO LAR', 'HO LWK', 'HO PALU', 'HO KDI', 'HO MUNA', 'HO WATU', 'HO LAEYA', 'HO TKE'] and lead_time <= 36) or \
       (loc in ['HO OBI', 'HO FLUK', 'HO BARU', 'HO LWI'] and lead_time <= 43):
        return 'Urgent'
    return 'Normal'

def determine_time_date_days(loc_series, item_category_series):
    sulawesi_locations = ['LAR', 'LWK', 'PALU', 'KDI', 'MUNA', 'TKE', 'WATU', 'LAEYA']
    halmahera_locations = ['OBI', 'FLUK', 'BARU', 'LWI']
    ho_halmahera_pattern = '|'.join([f'HO {loc}' for loc in halmahera_locations])
    ho_sulawesi_pattern = '|'.join([f'HO {loc}' for loc in sulawesi_locations])
    lc_halmahera_pattern = '|'.join([f'LC {loc}' for loc in halmahera_locations])
    lc_sulawesi_pattern = '|'.join([f'LC {loc}' for loc in sulawesi_locations])
    LC_Categorizatoin = [
        'Consumable Workshop', 'Packaging', 'Alat dan Bahan Bangunan', 
        'Bolt dan Nut', 'Elektrikal', 'Consumable Cleaning',
        'Perabotan', 'Peralatan Geologi', 'Peralatan Dapur'
    ]
    is_special_lc_category = item_category_series.isin(LC_Categorizatoin)
    
    conditions = [
        loc_series.str.contains(ho_halmahera_pattern, na=False),
        loc_series.str.contains(ho_sulawesi_pattern, na=False),
        loc_series.str.contains(lc_halmahera_pattern, na=False) & is_special_lc_category,
        loc_series.str.contains(lc_sulawesi_pattern, na=False) & is_special_lc_category,
        loc_series.str.contains(r"LC|HO", na=False)
    ]
    choices = [43, 36, 43, 36, 15]
    
    return np.select(conditions, choices, default=0).astype(int)

#def for exclude lebaran
def days_excluding_lebaran(start_date, end_date):
    if pd.isnull(start_date) or pd.isnull(end_date):
        return np.nan

    start_d = pd.to_datetime(start_date, dayfirst=True).date()
    end_d = pd.to_datetime(end_date, dayfirst=True).date()

    if start_d == end_d:
        return 0

    is_early = end_d < start_d
    range_start = min(start_d, end_d)
    range_end = max(start_d, end_d)
    date_range = pd.date_range(start=range_start, end=range_end)[1:] 
    non_lebaran_days = sum(1 for d in date_range if d.date() not in lebaran_dates)

    if is_early:
        return -non_lebaran_days
    else:
        return non_lebaran_days

#freight type
def determine_freight(row, freight_mapping, rara_map, ryi_map):
    supplier = row['Supplier']
    po_number = row['PO Number']

    if not isinstance(supplier, str):
        return "Other Freight"

    if supplier in freight_mapping and pd.notnull(freight_mapping[supplier]):
        return freight_mapping[supplier]
    
    if "RARA" in supplier.upper():
        freight_type = rara_map.get(po_number)
        return freight_type if pd.notnull(freight_type) else "Unknown RARA Freight"

    if "RYI" in supplier.upper():
        freight_type = ryi_map.get(po_number)
        return freight_type if pd.notnull(freight_type) else "Unknown RYI Freight"
        
    return "Other Freight"

#take finalization date
def extract_finalisasi_date(text):
    """
    Extracts a date (dd/mm/yyyy or dd Mmm yyyy) that immediately follows 
    'Finalisasi' (case-insensitive) in the text string using Regex.
    """
    if pd.isna(text):
        return None
    text = str(text)
    # Matches "Finalisasi" followed by optional characters, whitespace, and a date pattern
    regex = r"finalisasi.*?\\s*(\\d{1,2}[/\\s][A-Za-z0-9]{2,3}[/\\s]\\d{4})"
    match = re.search(regex, text, re.IGNORECASE)
    if match:
        date_string = match.group(1).strip().replace('.', '')
        try:
            return pd.to_datetime(date_string, dayfirst=True, errors='raise').date()
        except Exception:
            return None
    else:
        return None

#for determining ontime status
def calculate_purchasing_status(procurement_name_series, duration_series):
    """
    Determines Purchasing On-Time Status based on team rules and duration.
    
    :param procurement_name_series: Series containing 'Procurement Name'.
    :param duration_series: Series containing 'Purchasing_Duration' (days).
    :return: A tuple of (status_series, on_time_series, late_series, on_time_percent_series)
    """
    
    #Define team members (Core Business Rule)
    ho_team = ['Linda / Puji / Syifa R / Stheven', 'Syifa Ramadhani', 'Syifa Alifia', 'Rizal Agus Fianto', 'Auriel', 'Puji Astuti', 'Linda Permata Sari', 'Laurensius Adi', 'Syifa Ramadhani Luthfi']
    site_team = ['Rona / Joko', 'Joko', 'Victo', 'Rakan', 'Rona Justhafist', 'Rona / Victo / Rakan / Joko', 'Fairus / Irwan', 'Fairus Mubakri', 'Irwan', 'Ady', 'Fairus / Ady', 'Olvan']

    #Create boolean masks for each team
    is_ho_team = procurement_name_series.isin(ho_team)
    is_site_team = procurement_name_series.isin(site_team)

    #Set status based on a hierarchy of rules
    conditions = [
        #HO Team Rules (<= 5 days)
        (is_ho_team) & (duration_series <= 5),
        (is_ho_team) & (duration_series > 5),
        
        #Site Team Rules (<= 3 days)
        (is_site_team) & (duration_series <= 3),
        (is_site_team) & (duration_series > 3)
    ]
    choices = ['On Time', 'Late', 'On Time', 'Late']
    
    status_series = np.select(conditions, choices, default=None)
    
    #calculate flag and ontime marker
    on_time_series = np.where(status_series == 'On Time', 1, np.nan)
    late_series = np.where(status_series == 'Late', 1, np.nan)
    on_time_percent_series = np.where(status_series == 'On Time', 1, np.where(status_series == 'Late', 0, np.nan))
    
    return status_series, on_time_series, late_series, on_time_percent_series

if __name__ == '__main__':
    print("data_helpers.py is a utility module. Functions defined.")