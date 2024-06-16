import pandas as pd
import numpy as np

# Read the csv file into a pandas dataframe
data_df = pd.read_csv('final_data_backup_v2.csv')

# Calculate the mev_tx_count with respect to zeromev swap count and append it to the dataframe
data_df['mev_tx_count_with_swaps'] = data_df[['arb_count', 'sandwich_count', 'liquid_count', 'swap_count']].sum(axis=1)
data_df['mev_tx_count'] = data_df[['arb_count', 'sandwich_count', 'liquid_count']].sum(axis=1)

# Calculate the mev_tx_pct with respect to blocknative_tx_count and append it to the dataframe
data_df['mev_tx_pct'] = np.where(data_df['tx_count'] == 0, 0, (data_df['mev_tx_count'] / data_df['tx_count']) * 100)
data_df['mev_tx_pct'] = data_df['mev_tx_pct'].apply(lambda x: round(x, 2))

# Calculate the mev_tx_pct with respect to zeromev_count and append it to the dataframe
data_df['mev_tx_pct_swaps'] = np.where(data_df['swap_count'] == 0, 0, (data_df['mev_tx_count'] / data_df['mev_tx_count_with_swaps']) * 100)
data_df['mev_tx_pct_swaps'] = data_df['mev_tx_pct_swaps'].apply(lambda x: round(x, 2))

data_df['mean_gasprice_gwei'] = data_df['mean_gasprice_gwei'].apply(lambda x: round(x, 2))
data_df['median_gasprice_gwei'] = data_df['median_gasprice_gwei'].apply(lambda x: round(x, 2))

# Write the final merged dataframe to a CSV file called 'final_data.csv'
data_df.to_csv('final_data.csv', index=False)