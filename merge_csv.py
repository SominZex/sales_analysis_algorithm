import pandas as pd

# Load the first CSV with header
df1 = pd.read_csv('may1_28_sale_report.csv')

# Load the second CSV and skip its header
df2 = pd.read_csv('may29_31.csv', skiprows=1, header=None)

# Manually assign column names to df2 to match df1
df2.columns = df1.columns

# Concatenate both DataFrames
merged_df = pd.concat([df1, df2], ignore_index=True)

# (Optional) Save to a new CSV file
merged_df.to_csv('may2025.csv', index=False)

print("CSV files merged successfully.")
