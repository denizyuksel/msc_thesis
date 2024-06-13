import pandas as pd
import numpy as np

# Read the csv file into a pandas dataframe
data_df = pd.read_csv('final_data_backup.csv')

# Calculate the private_tx_pct and append it to the dataframe
data_df['private_tx_pct'] = np.where(data_df['tx_count'] == 0, 0, (data_df['private_tx_count'] / data_df['tx_count']) * 100)
data_df['private_tx_pct'] = data_df['private_tx_pct'].apply(lambda x: round(x, 2))

# Calculate the private_gasused_pct and append it to the dataframe
total_gasused = data_df['public_gasused_gwei'] + data_df['private_gasused_gwei']
data_df['private_gasused_pct'] = np.where(total_gasused == 0, 0, (data_df['private_gasused_gwei'] / total_gasused) * 100)
data_df['private_gasused_pct'] = data_df['private_gasused_pct'].apply(lambda x: round(x, 2))

# Write the final merged dataframe to a CSV file called 'final_data.csv'
data_df.to_csv('final_data.csv', index=False)