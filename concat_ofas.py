import pandas as pd


df_protect = pd.read_csv('protect_historical.csv')

# Group the dataframe by block_number and count the number of unique tx_hash values
df_block_tx_count = df_protect.groupby('created_at_block_number')['tx_hash'].nunique().reset_index()

# Rename the columns to match the desired output
df_block_tx_count = df_block_tx_count.rename(columns={'created_at_block_number': 'block_number', 'tx_hash': 'flashbots_protect_tx_count'})

# Read blocknative_zeromev_flashbots_rq4.csv into a pandas dataframe
df_blocknative = pd.read_csv('blocknative_zeromev_flashbots_rq4.csv')

# Merge the two dataframes using a left join on the block_number column
df_merged = pd.merge(df_blocknative, df_block_tx_count, on='block_number', how='left')

# Print the number of rows in all 3 dataframes
print("Number of rows in df_protect:", len(df_protect))
print("Number of rows in df_blocknative:", len(df_blocknative))
print("Number of rows in df_merged:", len(df_merged))

# Write the final merged dataframe to a CSV file called 'final_data.csv'
df_merged.to_csv('final_data.csv', index=False)