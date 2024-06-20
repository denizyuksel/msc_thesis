import pandas as pd
import glob
import os

# Step 1: Get the current working directory
current_dir = os.getcwd()

# Step 2: Use glob to find all files in the directory with the format 'YYYY-MM-DD.csv'
csv_files = glob.glob(os.path.join(current_dir, '*.csv'))

# Step 3: Sort the list of files in chronological order by parsing the dates from filenames
csv_files_sorted = sorted(csv_files, key=lambda x: pd.to_datetime(x.split('/')[-1].replace('.csv', ''), errors='coerce'))

# Step 4: Read each CSV file in the sorted list and append it to a list of DataFrames
dataframes = []
for file in csv_files_sorted:
    df = pd.read_csv(file)
    dataframes.append(df)

# Step 5: Concatenate all DataFrames in the list into a single DataFrame
big_dataframe = pd.concat(dataframes, ignore_index=True)

# Step 6: Save the big DataFrame to a new CSV file
output_file = os.path.join(current_dir, 'mevshare_2024.csv')
big_dataframe.to_csv(output_file, index=False)

print(f"All files have been merged in chronological order and saved to '{output_file}'")
