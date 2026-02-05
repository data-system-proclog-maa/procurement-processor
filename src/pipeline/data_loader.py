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

def load_all_data():
    
    data = {}
    
    try:
        # 1. Load Main Data
        data['df'] = pd.read_excel(os.path.join(raw_dir, "PO Entry List.xlsx"))

        # 2. Load Reference Files 
        data['holidays_df'] = pd.read_csv(nonworkdays_path)
        data['wilayah_df'] = pd.read_csv(wilayah_path)
        data['pulau_df'] = pd.read_csv(pulau_path)
        data['jasa_service_df'] = pd.read_csv(jasa_service_path)
        data['cost_saving_df'] = pd.read_csv(cost_saving_path)
    
        data['freight_df'] = pd.read_csv(freight_path)
        data['rara_df'] = pd.read_csv(rara_path)
        data['ryi_df'] = pd.read_csv(ryi_path)
        data['way_df'] = pd.read_csv(way_path)
        data['sln_df'] = pd.read_csv(sln_path)
    
        data['picnorm_df'] = pd.read_csv(normalisasi_rfm_path) 
        data['ontime_normalized_df'] = pd.read_csv(normalisasi_po_path) 
        data['timedate_normalized_df'] = pd.read_csv(timedate_path)
        data['notcounted_df'] = pd.read_csv(notcounted_po_path)
        data['logistic_normalized_df'] = pd.read_csv(normalisasi_logistic_path)

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