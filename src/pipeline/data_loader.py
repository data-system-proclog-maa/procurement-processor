import pandas as pd
import os
import numpy as np

project_root = '../../' 
raw_dir = os.path.join(project_root, 'data', 'po_entry') 
reference_dir = os.path.join(project_root, 'data', 'reference')

ontime_norm_path = os.path.join(reference_dir, 'ontime_normalisasi.xlsx')

def load_all_data():
    
    data = {}
    
    try:
        # 1. Load Main Data
        data['df'] = pd.read_excel(os.path.join(raw_dir, "PO Entry List.xlsx"))

        # 2. Load Reference Files 
        data['holidays_df'] = pd.read_excel(os.path.join(reference_dir, 'non_workdays.xlsx'))
        data['wilayah_df'] = pd.read_excel(os.path.join(reference_dir, 'wilayah.xlsx'))
        data['pulau_df'] = pd.read_excel(os.path.join(reference_dir, 'pulau.xlsx'))
        data['jasa_service_df'] = pd.read_excel(os.path.join(reference_dir, 'jasa_service.xlsx'))
        data['cost_saving_df'] = pd.read_excel(os.path.join(reference_dir, 'cost_saving.xlsx'))
    
        logistic_freight_path = os.path.join(reference_dir, 'logistic_freight.xlsx')
        data['freight_df'] = pd.read_excel(logistic_freight_path, sheet_name="Freight")
        data['rara_df'] = pd.read_excel(logistic_freight_path, sheet_name="RARA")
        data['ryi_df'] = pd.read_excel(logistic_freight_path, sheet_name="RYI")
    
        data['picnorm_df'] = pd.read_excel(os.path.join(reference_dir, 'normalisasi.xlsx'))    
        data['ontime_normalized_df'] = pd.read_excel(ontime_norm_path, sheet_name = 'normalized')
        data['timedate_normalized_df'] = pd.read_excel(ontime_norm_path, sheet_name ='timedate')
        data['notcounted_df'] = pd.read_excel(ontime_norm_path, sheet_name = 'notcalculated')
        data['logistic_normalized_df'] = pd.read_excel(ontime_norm_path, sheet_name = 'logistic_normalized')

        # Return data
        return data

    except FileNotFoundError as e:
        # This block is correctly indented for the error handling
        print(f"error: a file was not found. please check file paths in data_loader.py.")
        print(e)
        # Re-raise the error to stop the program
        raise

if __name__ == '__main__':
    load_all_data()