import pandas as pd
import numpy as np
import os
import math
#Import all necessary helper functions from the helpers module
from data_helper import (
    LOC_strings, project_string, divisi_string, PTCV_strings, TOP_strings, 
    item_category_merged, category_value_marker, category_value_xcmg, 
    urgent_normal_function, days_excluding_lebaran, determine_freight, 
    extract_finalisasi_date, determine_time_date_days, apply_routine_logic,
    calculate_purchasing_status
)

def run_all_processing(df, picnorm_df, holidays_df, wilayah_df, pulau_df, jasa_service_df, 
                       freight_df, rara_df, ryi_df, way_df,
                       cost_saving_df, timedate_normalized_df, 
                       ontime_normalized_df, notcounted_df, logistic_normalized_df):
    """
    Executes the entire data processing and feature engineering pipeline 
    on the main Procurement DataFrame by applying sequential business logic.
    
    :return: The fully processed pandas DataFrame.
    """

    print("preparing data and normalizing dates...")
    
    # 1.Date Preparation
    date_columns = [
        'Requisition Approved Date', 'Requisition Required Date', 'PO Submit Date',
        'PO Approval Date', 'Receive PO Date', 'Created TL Date', 'Shipped Date',
        'Received TL Date', 'PO Required Date'
    ]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Extract holiday dates for busday_count (Pythonic variable naming used here)
    holidays = pd.to_datetime(holidays_df['NONWORKDAYS'], format='%d/%m/%Y').dt.date.tolist()
    
    # Prepare picnorm_df (Normalization data for Requisition Approved/Required Dates)
    picnorm_indexed = picnorm_df.drop_duplicates(subset='Requisition Number').set_index('Requisition Number')


    # --- Step 1: Initial Feature Engineering ---
    print("running step 1: initial feature engineering...")

    # A. Extract date from Req Progress Status using the helper function
    df['Extracted Approved Date'] = df['Req Progress Status'].apply(extract_finalisasi_date)

    # B. Map dates from picnorm
    df['Updated Requisition Approved Date'] = df['Requisition Number'].map(picnorm_indexed['Updated Requisition Approved Date'])
    df['Updated Requisition Required Date'] = df['Requisition Number'].map(picnorm_indexed['Updated Requisition Required Date'])
    df['Background Update'] = df['Requisition Number'].map(picnorm_indexed['Background Update'])

    # C. Apply Priority Logic from normalization (Highest: picnorm -> Second: Extracted -> Lowest: Original)
    used_approved_date = (
        df['Updated Requisition Approved Date']
        .combine_first(df['Extracted Approved Date'])
        .combine_first(df['Requisition Approved Date'])
    )

    used_required_date = df['Updated Requisition Required Date'].fillna(df['Requisition Required Date'])

    # D. Convert final selected dates to datetime objects for calculations
    used_approved_date = pd.to_datetime(used_approved_date, errors='coerce')
    used_required_date = pd.to_datetime(used_required_date, errors='coerce')

    # Apply helpers for LOC, LEAD TIME, URGENT/NORMAL flags
    df['LOC'] = df['Department'].apply(LOC_strings)
    df['LEAD TIME'] = np.floor((used_required_date - used_approved_date).dt.total_seconds() / (24 * 3600))
    df['URGENT_NORMAL'] = df.apply(urgent_normal_function, axis=1)

    df['URGENT*'] = np.where(
        (df['Urgent'] == 'Normal') & (df['URGENT_NORMAL'] == 'Urgent'), 
        'Urgent*', 
        None
    )
    df['NORMAL'] = np.where(df['URGENT_NORMAL'] == 'Normal', 'Normal', None)
    df['URGENT2'] = np.where(df['URGENT_NORMAL'] == 'Urgent', 'Urgent', None)
    df['URGENT_FINALFORLOGBOOK'] = df['URGENT*'].combine_first(df['URGENT_NORMAL'])

    # Wilayah and Pulau (Mapping Logic)
    temp_df = df[['Supplier Location']].copy()
    temp_df['Supplier Location'] = temp_df['Supplier Location'].str.strip().str.lower()
    
    wilayah_df['Supplier Location'] = wilayah_df['Supplier Location'].str.strip().str.lower()
    pulau_df['Wilayah'] = pulau_df['Wilayah'].str.strip().str.lower()
    
    wilayah_process = pd.merge(temp_df, wilayah_df, on='Supplier Location', how='left')
    wilayah_process['To'] = wilayah_process['To'].str.lower()
    
    supplier_process = pd.merge(wilayah_process, pulau_df, left_on='To', right_on='Wilayah', how='left')
    
    df['WILAYAH'] = supplier_process['To']
    df['PULAU'] = supplier_process['Pulau']

    # Apply other string helpers
    df['DEPARTMENT_'] = df['Department'].apply(project_string)
    df['DIVISI'] = df['Department'].apply(divisi_string)
    df['SUPPLIER_'] = df['Supplier'].apply(PTCV_strings)
    df['TOP'] = df['Term of Payment'].apply(TOP_strings)
    df['CATEGORYMERGED'] = df.apply(
        lambda x: item_category_merged(x['Item Category'], x.get('Unit')), 
        axis=1
    )
    df['CATEGORYVALUE'] = df.apply(category_value_marker, axis=1)
    df['CATEGORYVALUEXCMG'] = df.apply(category_value_xcmg, axis=1)
    df['VALUE'] = 1
    
    # Logic to set 'VALUE' to 0 for POs with certain categories
    df.loc[df.groupby("PO Number")["CATEGORYVALUE"].transform("max") == 1, "VALUE"] = 0
    df.loc[df.groupby("PO Number")["CATEGORYVALUEXCMG"].transform("max") == 1, "VALUE"] = 0
    
    first_occurrence_mask = ~df.duplicated(subset=['PO Number'])
    df.loc[first_occurrence_mask & (df.groupby('PO Number')['VALUE'].transform('sum') == 0), 'VALUE'] = 1
    
    df['UNIQUE COUNT PO'] = np.where(~df['PO Number'].duplicated(), 1, 0)
    df['Final_ItemID'] = df['Item ID']


    # --- Step 2: Time-Based Calculations ---
    print("running step 2: time-based calculations...")

    # Set TIME DATE baseline using helper
    days_to_add = determine_time_date_days(df['LOC'], df['Item Category']) 

    timedate_norm_map = timedate_normalized_df.set_index('PO Number')['timedate']
    overrides = df['PO Number'].map(timedate_norm_map)

    final_days_to_add = overrides.fillna(pd.Series(days_to_add, index=df.index)).astype(int)
    df['TIME DATE'] = used_approved_date + pd.to_timedelta(final_days_to_add, unit='D')
    
    # Condition mask for time-based calculations
    is_calculable = (
        (df['VALUE'] == 1)
        & (df['Requisition Type'] != 'Consignment')
        & (~df['Item Category'].isin(['Jasa Logistik', 'Solar']))
    )
    
    # Calculate time differences using apply and helper functions
    df['PR - PO'] = pd.Series(  # <--- pd.Series starts here
        np.where(
            is_calculable, 
            df.apply(lambda row: days_excluding_lebaran(row['Updated Requisition Approved Date'], row['PO Submit Date']), axis=1), 
            np.nan
        )).clip(lower=0)
    
    df['PO SUB - PO APP'] = np.where(
        is_calculable, 
        df.apply(lambda row: days_excluding_lebaran(row['PO Submit Date'], row['PO Approval Date']), axis=1), 
        np.nan
    )

    is_calculable_po_rpo = df['PR - PO'].notna() & df['Receive PO Date'].notna() & (df['Item Category'] != 'Jasa/Service')
    df['PO - R PO'] = np.where(
        is_calculable_po_rpo, 
        df.apply(lambda row: days_excluding_lebaran(row['PO Approval Date'], row['Receive PO Date']), axis=1), 
        np.nan
    )

    is_calculable_r_rsite = df['PR - PO'].notna() & df['Receive PO Date'].notna() & (df['Item Category'] != 'Jasa/Service') & df['Location TL Received'].notna() & (df['LOC'] != 'HO')
    df['R-R SITE'] = np.where(
        is_calculable_r_rsite, 
        df.apply(lambda row: days_excluding_lebaran(row['Receive PO Date'], row['Received TL Date']), axis=1), 
        np.nan
    )

    #Calculate PR - PO SUB WD (Work days)
    df['PR - PO SUB WD'] = np.nan
    is_calculable_wd = is_calculable & used_approved_date.notna() & df['PO Submit Date'].notna()
    start_dates_filtered = pd.to_datetime(used_approved_date[is_calculable_wd], errors='coerce')
    end_dates_filtered = pd.to_datetime(df.loc[is_calculable_wd, 'PO Submit Date'], errors='coerce')
    final_valid_mask = start_dates_filtered.notna() & end_dates_filtered.notna()
    final_valid_index = start_dates_filtered[final_valid_mask].index
    valid_start_dates = start_dates_filtered[final_valid_mask]
    valid_end_dates = end_dates_filtered[final_valid_mask]
    
    if not final_valid_index.empty:
        df.loc[final_valid_index, 'PR - PO SUB WD'] = np.busday_count(
            valid_start_dates.values.astype('datetime64[D]'),
            valid_end_dates.values.astype('datetime64[D]'),
            weekmask='1111100',
            holidays=holidays
        )
    
    # Calculate Financials
    df['REQUISITION_TOTAL'] = (df['Requisition SubTotal'] * df['Exchange Rate']) * 1.11
    df['PO_TOTAL'] = (df['Qty Order'] * df['PO Price'] * df['Exchange Rate']) + (df['Jumlah PPN'] * df['Exchange Rate'])
    df['BUDGET'] = (df['REQUISITION_TOTAL'] - df['PO_TOTAL']).round(2)
    df['BUDGET%'] = (df['PO_TOTAL'] / df['REQUISITION_TOTAL']).clip(upper=1)
    
    # Calculate Logistics Time Splits
    df['RPO-TLC'] = np.where(df['R-R SITE'].notna(), (df['Created TL Date'] - df['Receive PO Date']).dt.days, np.nan)
    df['TLC-SHIP'] = np.where(df['R-R SITE'].notna(), (df['Shipped Date'] - df['Created TL Date']).dt.days, np.nan)
    df['SHIP-RSITE'] = np.where(df['R-R SITE'].notna(), (df['Received TL Date'] - df['Shipped Date']).dt.days, np.nan)

    # --- Purchasing On-Time Calculation ---
    print("calculating purchasing on-time metric...")
    
    df['Purchasing_Duration'] = np.where(
        is_calculable, 
        df.apply(lambda row: days_excluding_lebaran(row['Updated Requisition Approved Date'], row['PO Approval Date']), axis=1),
        np.nan
    )

    # Use helper function to calculate all status flags at once
    df['STATUS_Purchasing'], df['ON_TIME_Purchasing'], df['LATE_Purchasing'], df['ON_TIME%_Purchasing'] = \
        calculate_purchasing_status(df['Procurement Name'], df['Purchasing_Duration'])


    # --- Step 3: Logistics & Receiving Status ---
    print("running step 3: logistics & receiving status...")
    freight_mapping = dict(zip(freight_df['Supplier'], freight_df['Freight Type']))
    
    # Pre-compute RARA/RYI PO-to-Freight Type Mappings
    rara_map = rara_df.drop_duplicates(subset=['PO Number']).set_index('PO Number')['Freight Type'].to_dict()
    ryi_map = ryi_df.drop_duplicates(subset=['PO Number']).set_index('PO Number')['Freight Type'].to_dict()
    way_map = way_df.drop_duplicates(subset=['PO Number']).set_index('PO Number')['Freight Type'].to_dict()

    df['LOGISTIC_FREIGHT'] = np.where(
        df['Item Category'] == 'Jasa Logistik', 
        df.apply(determine_freight, axis=1, args=(freight_mapping, rara_map, ryi_map, way_map)), 
        ''
    )
    
    df['FARTHEST REQUIRED DATE'] = df[['PO Required Date', 'Requisition Required Date', 'TIME DATE']].max(axis=1)
    df['USED RECEIVE DATE'] = df['Receive PO Date'].fillna(df['Received TL Date'])
    
    df['REC'] = np.where(df['VALUE'] == 1, 
                         df.apply(lambda row: days_excluding_lebaran(row['FARTHEST REQUIRED DATE'], row['USED RECEIVE DATE']), axis=1), 
                         np.nan)
    
    df['STATUS REC'] = np.where(
        df['REC'].isna(), 
        "", 
        np.where(
            df['Requisition Type'] == "Consignment", 
            "On Time", 
            np.where(df['REC'] >= 1, "Late", "On Time")
        )
    )
    df['ON_TIME'] = np.where(df['STATUS REC'] == 'On Time', 1, np.nan)
    df['LATE'] = np.where(df['STATUS REC'] == 'Late', 1, np.nan)
    df['ON_TIME%'] = np.where(df['STATUS REC'] == 'On Time', 1, np.where(df['STATUS REC'] == 'Late', 0, np.nan))
    
    special_ontime_categories = ['Jasa/Service', 'Solar']
    mask_special_ontime = df['Item Category'].isin(special_ontime_categories)
    df.loc[mask_special_ontime, 'STATUS REC'] = 'On Time'
    df.loc[mask_special_ontime, 'ON_TIME'] = 1
    df.loc[mask_special_ontime, 'LATE'] = np.nan
    df.loc[mask_special_ontime, 'ON_TIME%'] = 1
    
    df['ON_TIME%_original_purchasing'] = df['ON_TIME%']
    df['LOGISTICAL_PROCESS'] = (df['Final Destination Location'] != df['PO Receive Location']).astype(int)
    df['RECEIVE_INDICATOR_PO'] = (df['Qty Order'] == df['Qty Received']).astype(int)
    df['RECEIVE_PO_STATUS'] = np.where(df['Qty Received'] == 0, "PO Not Received", np.where(df['Qty Order'] == df['Qty Received'], "Fully Received", "Partial Received"))
    df['TL_NUMBER_?'] = df['TL Number'].notnull().astype(int)
    
    df['RECEIVE_INDICATOR_LOGISTIC'] = np.where(
        df['LOGISTICAL_PROCESS'] == 1,
        ((df['TL Qty Received'] == df['Qty Shipped']) & (df['Qty Order'] == df['Qty Received'])).astype(int),
        0
    )
    
    conditions_tl = [
        df['LOGISTICAL_PROCESS'] == 0,
        (df['LOGISTICAL_PROCESS'] == 1) & (df['RECEIVE_PO_STATUS'] == "PO Not Received"),
        (df['TL Qty Received'] == 0) & (df['TL Number'].isnull()),
        (df['TL Qty Received'] == 0) & (df['TL Number'].notnull()),
        (df['TL Qty Received'] > 0) & (df['Location TL Received'] != df['Final Destination Location']),
        (df['Location TL Received'] == df['Final Destination Location']) & (df['TL Qty Received'] == df['Qty Shipped']) & (df['Qty Shipped'] == df['Qty Order']) & (df['Qty Order'] == df['Qty Received']),
        (df['Location TL Received'] == df['Final Destination Location'])
    ]
    choices_tl = ["Without Logistical Process", "PO Not Received", "Transfer List Preparation", "On Transit", "At Intermediate Location", "Fully Received", "Partial Received"]
    df['TL_RECEIVE_INFO'] = np.select(conditions_tl, choices_tl, default="Check Status")
    
    fully_received_cond = (df['Requisition Type'] == "Consignment") | (df['Item Category'] == "Jasa Logistik") | ((df['LOGISTICAL_PROCESS'] == 0) & (df['RECEIVE_INDICATOR_PO'] == 1)) | ((df['TL_RECEIVE_INFO'] == "Fully Received") & (df['RECEIVE_INDICATOR_PO'] == 1))
    df['FULLY_RECEIVE_INFO'] = np.where(fully_received_cond, 1, 0)
    
    df['RECEIVED'] = np.where(df['FULLY_RECEIVE_INFO'] == 1, 1, np.nan)
    df['NOT_RECEIVED'] = np.where(df['FULLY_RECEIVE_INFO'] == 0, 1, np.nan)
    
    df['TRANSFER_ITEM'] = np.where((df['LOGISTICAL_PROCESS'] == 0) & (df['TL_NUMBER_?'] == 1), "Transfer Item", "")
    df['SHIPPING_TYPE_LAND'] = np.where(df['Shipping Type'].astype(str).str.contains('darat', case=False, na=False), 'Land', '')
    df['SHIPPING_TYPE_SEA'] = np.where(df['Shipping Type'].astype(str).str.contains('laut', case=False, na=False), 'Sea', '')
    df['SHIPPING_TYPE_AIR'] = np.where(df['Shipping Type'].astype(str).str.contains('udara', case=False, na=False), 'Air', '')


    # --- Step 4: Fully Received PO Status ---
    print("running step 4: fully received po status...")
    po_group_counts = df.groupby('PO Number')['PO Number'].transform('count')
    po_group_received = df.groupby('PO Number')['RECEIVED'].transform('sum')
    is_fully_received = (po_group_counts == po_group_received)
    df['PO_RECEIVE'] = np.where(is_fully_received, 'Fully Received', '')


    # --- Step 5: Jasa Service Merge ---
    print("running step 5: jasa service merge...")
    
    df_itemID_clean = df['Item ID'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_poNum_clean = df['PO Number'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    js_itemID_clean = jasa_service_df['Item ID'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    js_poNum_clean = jasa_service_df['PO Number'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    df['uid'] = df_itemID_clean + df_poNum_clean
    jasa_service_df['uid'] = js_itemID_clean + js_poNum_clean
    
    df = df.merge(jasa_service_df[['uid', 'JS_SERVICE']], on='uid', how='left')
    df.drop(columns=['uid'], inplace=True)


    # --- Step 6a: Apply Purchasing Business Rule Normalizations ---
    print("running step 6a: apply purchasing business rule normalizations...")
    is_jasa_logistik_tracked = (df['Item Category'].str.contains('Jasa Logistik|Solar', na=False)) & (df['VALUE'] == 1)
    df.loc[is_jasa_logistik_tracked, 'STATUS_Purchasing'] = 'On Time'
    df.loc[is_jasa_logistik_tracked, 'ON_TIME_Purchasing'] = 1
    df.loc[is_jasa_logistik_tracked, 'LATE_Purchasing'] = np.nan
    df.loc[is_jasa_logistik_tracked, 'ON_TIME%_Purchasing'] = 1


    # --- Step 6b: Apply Manual Delivery On-Time Normalizations ---
    print("running step 6b: applying manual delivery on-time normalizations...")
    df['PO Number'] = df['PO Number'].astype(str).str.strip()
    ontime_normalized_df['PO Number'] = ontime_normalized_df['PO Number'].astype(str).str.strip()
    notcounted_df['PO Number'] = notcounted_df['PO Number'].astype(str).str.strip()
    
    mask_ontime = df['PO Number'].isin(ontime_normalized_df['PO Number']) & (df['VALUE'] == 1)
    df.loc[mask_ontime, 'STATUS REC'] = 'On Time'
    df.loc[mask_ontime, 'ON_TIME'] = 1
    df.loc[mask_ontime, 'LATE'] = np.nan
    df.loc[mask_ontime, 'ON_TIME%'] = 1
    
    mask_excluded = df['PO Number'].isin(notcounted_df['PO Number'])
    df.loc[mask_excluded, 'STATUS REC'] = None
    df.loc[mask_excluded, ['ON_TIME', 'LATE', 'ON_TIME%']] = np.nan


    # --- Step 6c: Apply Logistic On-Time Normalizations ---
    print("running step 6c: applying logistic on-time normalizations...")
    logistic_normalized_df['PO Number'] = logistic_normalized_df['PO Number'].astype(str).str.strip()
    logistic_norm_dict = logistic_normalized_df.drop_duplicates(subset='PO Number').set_index('PO Number')['Value']
    df['ON_TIME%_logistic'] = df['PO Number'].map(logistic_norm_dict)


    # --- Step 7: Apply Cost Saving Updates ---
    print("running step 7: apply cost saving updates...")
    df['Unicode_Key'] = df['Item Name'].astype(str) + '-' + df['PO Number'].astype(str)
    cost_saving_df['Unicode_Key'] = cost_saving_df['Item Name'].astype(str) + '-' + cost_saving_df['PO Number'].astype(str)
    
    cost_saving_dict = cost_saving_df.drop_duplicates(subset='Unicode_Key').set_index('Unicode_Key')['Cost Saving']
    updated_cost_saving = df['Unicode_Key'].map(cost_saving_dict)
    
    df['Cost Saving'] = updated_cost_saving.fillna(df['Cost Saving'])


    # --- Step 8: Create Final_ItemID ---
    print("creating final_itemid (using item id as final id)...")
    df['Final_ItemID'] = df['Item ID']

    
    # --- Step 9: Apply Routine Categorization Updates ---
    print("applying routine categorization updates...")

    category_l = df['CATEGORYMERGED'].str.lower().fillna('')
    item_name_l = df['Item Name'].str.lower().fillna('')
    pic_name_l = df['Procurement Name'].str.strip().fillna('').str.lower()

    df['_Routine'] = df['Routine']

    df['_Routine'] = apply_routine_logic(df['_Routine'], category_l, item_name_l, pic_name_l)

    # 20. On time unnormalized
    print("pulling unnormalized on time%...")
    df['ON_TIME%_overall_original'] = np.where(
        df['Item Category'].isin(['Jasa/Service', 'Solar']),
        1,                                                  
        np.where(                                             
            df['REC'].isna(),                                  
            np.nan,                                             
            np.where(df['REC'] >= 1, 0, 1)                      
        )
    )

    # 21. Update 2024 Logistic Cost Saving
    print("updating 2024 logistic cost saving...")
    df['Cost Saving'] = np.where(
        (df['Item ID'] == 18640) & (df['PO Number'] == '20/CB/012024'),
        674957950,
        df['Cost Saving']
    )

    print("reordering columns and performing final cleanup...")
    # --- Final Column Ordering and Cleanup ---
    final_column_order = [
        'Requisition Type', 'Item ID', 'Item Name', 'Item Category', 'Department', 
        'Unit', 'Currency', 'Exchange Rate', 'PO Price', 'Qty Order', 
        'PO Disc/Cost', 'PO Sub Total', 'Jumlah PPN', 'Qty Received', 'PO Receive Location', 
        'PO Submit Date', 'PO Required Date', 'PO Approval Date', 'Receive PO Estimation', 'Receive PO Date', 
        'Qty Handover', 'Handover Date', 'PO Number', 'Supplier', 'Supplier Location', 
        'Term of Payment', 'PO Status', 'PO Progress Status', 'Status Update Date', 'Requisition Number', 
        'Requisition Date', 'Requisition Submited Date', 'Requisition Approved Date', 'Requisition Required Date', 'Qty Requisition', 
        'Requisition Unit Price', 'Requisition SubTotal', 'Asset / Non Asset', 'Cost Saving', 'Routine', 
        'Urgent', 'Background Needs', 'Urgent Note', 'Urgent Cost', 'Procurement Name', 
        'Req Status', 'Req Progress Status', 'TL Number', 'Shipping Type', 'Created TL Date', 
        'Qty Shipped', 'Shipped Date', 'ETA Date', 'TL Qty Received', 'Received TL Date', 
        'Location TL Received', 'Final Destination Location', 'Remarks', 'VALUE', 'UNIQUE COUNT PO', 
        'CATEGORYMERGED', 'LOC', 'LEAD TIME', 'URGENT_NORMAL', 'NORMAL', 
        'URGENT2', 'URGENT*', 'URGENT_FINALFORLOGBOOK', 'WILAYAH', 'PULAU', 
        'DEPARTMENT_', 'DIVISI', 'SUPPLIER_', 'TOP', 'Updated Requisition Approved Date', 
        'Updated Requisition Required Date', 'Background Update', 'TIME DATE', 'PR - PO', 'PO SUB - PO APP', 
        'PO - R PO', 'R-R SITE', 'PR - PO SUB WD', 'RPO-TLC', 'TLC-SHIP', 
        'SHIP-RSITE', 'REQUISITION_TOTAL', 'PO_TOTAL', 'BUDGET', 'BUDGET%', 
        'FARTHEST REQUIRED DATE', 'USED RECEIVE DATE', 'REC', 'STATUS REC', 'ON_TIME', 
        'LATE', 'ON_TIME%', 'LOGISTICAL_PROCESS', 'RECEIVE_INDICATOR_PO', 'RECEIVE_PO_STATUS', 
        'TL_NUMBER_?', 'RECEIVE_INDICATOR_LOGISTIC', 'TL_RECEIVE_INFO', 'FULLY_RECEIVE_INFO', 'TRANSFER_ITEM', 'SHIPPING_TYPE_LAND', 
        'SHIPPING_TYPE_SEA', 'SHIPPING_TYPE_AIR', 'LOGISTIC_FREIGHT', 'RECEIVED', 'NOT_RECEIVED', 
        'PO_RECEIVE', 'JS_SERVICE', 'ON_TIME%_overall_original' ,'ON_TIME%_original_purchasing', 'ON_TIME%_logistic', 'Final_ItemID',
        'Purchasing_Duration', 'STATUS_Purchasing', 'ON_TIME_Purchasing', 'LATE_Purchasing', 'ON_TIME%_Purchasing','_Routine'
    ]
    
    for col in final_column_order:
        if col not in df.columns:
            df[col] = np.nan
            
    df = df[final_column_order]
    
    df.drop(columns=['Unicode_Key', 'CATEGORYVALUEXCMG', 'CATEGORYVALUE'], inplace=True, errors='ignore')

    print("processing complete!")
    return df

if __name__ == '__main__':
    print("processing_steps.py is the core logic module. Run from the notebook.")