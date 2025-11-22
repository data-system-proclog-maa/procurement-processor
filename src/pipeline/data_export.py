import os
import datetime

project_root = '../../' 
export_dir = os.path.join(project_root, 'export')

def export_data(df, filename=None):
    """
    Exports the DataFrame to an Excel file in the export directory.
    
    :param df: pandas DataFrame to export
    :param filename: Optional filename. If None, generates one with timestamp.
    """
    # Ensure export directory exists
    if not os.path.exists(export_dir):
        try:
            os.makedirs(export_dir)
            print(f"Created export directory at: {os.path.abspath(export_dir)}")
        except OSError as e:
            print(f"Error creating export directory: {e}")
            return None

    if filename is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"procurement_data_{timestamp}.xlsx"
    
    if not filename.endswith('.xlsx'):
        filename += '.xlsx'
        
    filepath = os.path.join(export_dir, filename)
    
    try:
        print(f"Exporting data to {os.path.abspath(filepath)}...")
        df.to_excel(filepath, index=False)
        print("Export successful!")
        return filepath
    except Exception as e:
        print(f"Failed to export data: {e}")
        return None

if __name__ == '__main__':
    print("data_export.py is a utility module. Functions defined.")