import pandas as pd

# File path for the CSV file
file_path = '18.csv'

# Read the first few rows to inspect column names
try:
    df_sample = pd.read_csv(file_path, nrows=5)
    print("Column names in the CSV file:")
    print(df_sample.columns.tolist())
except Exception as e:
    print("Error reading CSV file:", str(e))

# Add the rest of the processing script here after confirming the column names
