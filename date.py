import pandas as pd

# Load the CSV file
df = pd.read_csv('sale_data.csv')

# Convert the date column — assuming it's called 'Date'
df['Date'] = pd.to_datetime(df['orderDate'], format='%m/%d/%Y', errors='coerce')

# Format to yyyy-mm-dd
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

# Save to a new CSV file
df.to_csv('converted_dates.csv', index=False)
