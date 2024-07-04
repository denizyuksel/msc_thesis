import pandas as pd

# Step 1: Read the first CSV file
df_protect = pd.read_csv('protect_historical.csv')

# Step 2: Read the second CSV file
df_protect_2024 = pd.read_csv('flashbots_2024.csv')

# Step 3: Merge the two dataframes
merged_df = pd.concat([df_protect, df_protect_2024], ignore_index=True)

# Step 4: Save the merged dataframe to a new CSV file
# merged_df.to_csv('merged_protect.csv', index=False)

# Step 2: Count tx_hash per created_at_block_number
tx_count_per_block = merged_df.groupby('created_at_block_number')['tx_hash'].count().reset_index()

# Step 3: Add the new column for transaction count
tx_count_per_block.rename(columns={'tx_hash': 'fb_postmerge_tx_count'}, inplace=True)

# Step 4: Rename the column from 'created_at_block_number' to 'block_number'
tx_count_per_block.rename(columns={'created_at_block_number': 'block_number'}, inplace=True)

# Step 5: Read the second CSV file
df_final = pd.read_csv('final_data_backup.csv')

# Step 6: Concatenate the data based on block_number
df_final = pd.merge(df_final, tx_count_per_block, on='block_number', how='left')

# Optional: Save the updated DataFrame if needed
df_final.to_csv('final_data.csv', index=False)
