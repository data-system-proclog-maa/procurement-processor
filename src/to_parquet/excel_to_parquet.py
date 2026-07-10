import os
import pandas as pd
import numpy as np
from python_calamine import CalamineWorkbook

def convert_excel_to_parquet(input_file="data.xlsx", output_file="data_1.parquet",min_rows=0,  max_rows=71763):
    """
    Reads data.xlsx up to max_rows (including header) using python-calamine,
    applies strict data typing to each column, and saves to a Parquet file.
    """
    print(f"Reading {min_rows} to {max_rows} rows from {input_file} using Calamine...")
    wb = CalamineWorkbook.from_path(input_file)
    sheet = wb.get_sheet_by_name(wb.sheet_names[0])
    
    # Calamine to_python returns all rows
    # We slice up to max_rows (which is 1 header + (max_rows - 1) data rows)
    all_rows = sheet.to_python()
    slice_rows = all_rows[min_rows-1:max_rows]
    
    header = all_rows[0]
    data = all_rows[min_rows-1:max_rows]
    
    print(f"Loaded {len(data)} data records. Creating DataFrame...")
    df = pd.DataFrame(data, columns=header)
    
    # Define Column Type Mappings
    date_columns = [
        "PO Submit Date", "PO Required Date", "PO Approval Date", 
        "Receive PO Estimation", "Receive PO Date", "Handover Date", 
        "Status Update Date", "Requisition Date", "Requisition Submited Date", 
        "Requisition Approved Date", "Requisition Required Date", "Created TL Date", 
        "Shipped Date", "ETA Date", "Received TL Date", 
        "Updated Requisition Approved Date", "Updated Requisition Required Date", 
        "TIME DATE", "FARTHEST REQUIRED DATE", "USED RECEIVE DATE"
    ]
    
    # Int64 nullable integer columns to prevent float conversions (.0 values)
    int_columns = [
        "Item ID", "Final_ItemID"
    ]
    
    numeric_columns = [
        "Exchange Rate", "PO Price", "Qty Order", "PO Disc/Cost", "PO Sub Total", 
        "Jumlah PPN", "Qty Received", "Qty Handover", "Qty Requisition", 
        "Requisition Unit Price", "Requisition SubTotal", "Cost Saving", "Urgent Cost", 
        "Qty Shipped", "TL Qty Received", "VALUE", "UNIQUE COUNT PO", 
        "LEAD TIME", "PR - PO", "PO SUB - PO APP", "PO - R PO", "R-R SITE", 
        "PR - PO SUB WD", "PO SUB - PO APP WD", "RPO-TLC", "TLC-SHIP", 
        "SHIP-RSITE", "REQUISITION_TOTAL", "PO_TOTAL", "BUDGET", "BUDGET%", 
        "REC", "ON_TIME", "LATE", "ON_TIME%", "LOGISTICAL_PROCESS", 
        "RECEIVE_INDICATOR_PO", "RECEIVE_INDICATOR_LOGISTIC", "FULLY_RECEIVE_INFO", 
        "RECEIVED", "NOT_RECEIVED", "ON_TIME%_overall_original", 
        "ON_TIME%_original_purchasing", "ON_TIME%_logistic", "TL_NUMBER_?",
        "Purchasing_Duration", "ON_TIME_Purchasing", "LATE_Purchasing", 
        "ON_TIME%_Purchasing", "Total_Logistic_Lead_Time", "logistic_on_time"
    ]
    
    print("Applying strict schema and data types...")
    
    # Convert dates
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
    # Convert numeric columns
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # Convert nullable integer columns
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
    # Convert all remaining columns to string type
    non_str_cols = set(date_columns + numeric_columns + int_columns)
    for col in df.columns:
        if col not in non_str_cols:
            # Convert to string and handle None/NaN representation properly
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'None': np.nan, 'nan': np.nan, '': np.nan})
            df[col] = df[col].astype("string")
            
    print(f"Saving to Parquet format at: {output_file}...")
    df.to_parquet(output_file, index=False, engine='pyarrow')
    print("Successfully exported!")

if __name__ == "__main__":
    convert_excel_to_parquet()

