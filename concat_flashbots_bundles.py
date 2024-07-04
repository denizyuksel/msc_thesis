import pandas as pd


df_flashbots_counts = pd.read_csv('data_fetch_flashbots/flashbots_bundle_counts.csv')

df_final_backup = pd.read_csv('final_data_v2.csv')

# Merge the two dataframes using a left join on the block_number column
df_merged = pd.merge(df_final_backup, df_flashbots_counts, on='block_number', how='left')

# Print the number of rows in all 3 dataframes
print("Number of rows in df_protect:", len(df_flashbots_counts))
print("Number of rows in df_blocknative:", len(df_final_backup))
print("Number of rows in df_merged:", len(df_merged))

# Write the final merged dataframe to a CSV file called 'final_data.csv'
df_merged.to_csv('final_data.csv', index=False)