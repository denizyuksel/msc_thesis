import pandas as pd

# Step 1: Read both CSV files into pandas DataFrames
df_protect = pd.read_csv('mevshare_2024.csv')  # Updated to read 'mevshare_2024.csv' instead of 'protect_historical.csv'
df_mevshare = pd.read_csv('mevshare_historical.csv')

# Step 2: Merge the two DataFrames on 'block_number' ensuring all block numbers are present
# Assuming 'block_number' is a common column that you want to ensure is fully covered in both datasets
df_protect = pd.merge(df_protect, df_mevshare, on='block_number', how='outer')

# Step 3: Group the merged dataframe by block_number and count the number of unique tx_hash values
df_block_tx_count = df_protect.groupby('block_number')['tx_hash'].nunique().reset_index()

# Step 4: Rename the columns to match the desired output
df_block_tx_count = df_block_tx_count.rename(columns={'tx_hash': 'flashbots_protect_tx_count'})

# Step 5: Read blocknative_zeromev_flashbots_rq4.csv into a pandas dataframe
df_blocknative = pd.read_csv('blocknative_zeromev_flashbots_rq4.csv')

# Step 6: Merge the two dataframes using a left join on the block_number column
df_merged = pd.merge(df_blocknative, df_block_tx_count, on='block_number', how='left')

# Step 7: Print the number of rows in all dataframes
print("Number of rows in df_protect (after merge):", len(df_protect))
print("Number of rows in df_mevshare:", len(df_mevshare))
print("Number of rows in df_blocknative:", len(df_blocknative))
print("Number of rows in df_merged:", len(df_merged))

# Step 8: Write the final merged dataframe to a CSV file called 'final_data.csv'
df_merged.to_csv('final_data.csv', index=False)
