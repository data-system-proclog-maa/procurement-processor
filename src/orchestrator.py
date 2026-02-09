
import pandas as pd
import numpy as np
import os
import sys

# Add pipeline directory to sys.path to ensure imports work
# Assuming this script is located in src/orchestrator.py
current_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(current_dir, 'pipeline')
sys.path.append(pipeline_dir)

import data_loader
import data_helper
import processing_steps
import data_export

def main():
    print("Starting Orchestrator...")
    
    # ---------------------------------------------------------
    # FIX: Patch data_loader and data_export paths to ensure they work from this script
    # The original modules use relative paths assuming a specific execution context.
    # We override them here to be relative to the project root (one level up from src).
    # ---------------------------------------------------------
    project_root = os.path.dirname(current_dir) # d:/github/procurement-processor
    print(f"Project Root: {project_root}")
    
    # 1. Patch data_loader
    data_loader.project_root = project_root
    data_loader.raw_dir = os.path.join(project_root, 'data', 'po_entry')
    data_loader.reference_dir = os.path.join(project_root, 'data', 'reference')
    print(f"Data Source: {data_loader.raw_dir}")
    
    # 2. Patch data_export
    data_export.project_root = project_root
    data_export.export_dir = os.path.join(project_root, 'export')
    print(f"Export Target: {data_export.export_dir}")

    # 3. Load Data
    print("Loading data...")
    try:
        loaded_data = data_loader.load_all_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Unpack loaded data
    df = loaded_data['df']
    holidays_df = loaded_data['holidays_df']
    wilayah_df = loaded_data['wilayah_df']
    pulau_df = loaded_data['pulau_df']
    jasa_service_df = loaded_data['jasa_service_df']
    freight_df = loaded_data['freight_df']
    rara_df = loaded_data['rara_df']
    ryi_df = loaded_data['ryi_df']
    way_df = loaded_data['way_df']
    sln_df = loaded_data['sln_df']
    cost_saving_df = loaded_data['cost_saving_df']
    picnorm_df = loaded_data['rfm_normalized_df']
    ontime_normalized_df = loaded_data['ontime_normalized_df']
    timedate_normalized_df = loaded_data['timedate_normalized_df']
    notcounted_df = loaded_data['notcounted_df']
    logistic_normalized_df = loaded_data['logistic_normalized_df']
    normalisasi_rfm_solar_df = loaded_data['normalisasi_rfm_solar_df']

    # 4. Run Processing
    # IMPORTANT: Passing arguments in the CORRECT order as defined in processing_steps.py
    # def run_all_processing(df, rfm_normalized_df, normalisasi_rfm_solar_df, holidays_df, ...)
    print("Running processing pipeline...")
    try:
        df = processing_steps.run_all_processing(
            df, 
            picnorm_df, 
            normalisasi_rfm_solar_df,   # <--- Corrected Position (3rd arg)
            holidays_df,                # <--- Corrected Position (4th arg)
            wilayah_df, 
            pulau_df, 
            jasa_service_df, 
            freight_df, 
            rara_df, 
            ryi_df, 
            way_df,
            sln_df,
            cost_saving_df, 
            timedate_normalized_df, 
            ontime_normalized_df, 
            notcounted_df, 
            logistic_normalized_df
        )
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return

    # 5. Export Data
    print("Exporting data...")
    try:
        data_export.export_data(df)
    except Exception as e:
        print(f"Error exporting data: {e}")
        return
        
    print("Pipeline execution completed successfully.")

if __name__ == '__main__':
    main()
