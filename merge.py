import os
import pandas as pd

# Set the folder containing the CSV files
folder_path = './current_stock/'

# List all CSV files in the folder
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

data_frames = []


for file in csv_files:
    file_path = os.path.join(folder_path, file)
    
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            data_frames.append(df)
    except pd.errors.EmptyDataError:
        continue

if data_frames:
    merged_df = pd.concat(data_frames, ignore_index=True)
    merged_df.to_csv('merged_stock_report.csv', index=False)
    print("Merged CSV saved as 'merged_stock_report.csv'")
else:
    print("No data found to merge.")
