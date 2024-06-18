import pandas as pd

# Read the csv file into a dataframe
df = pd.read_csv('mevshare_historical.csv')

# Create a new field 'block_date' which is only the YYYY-MM-DD value of the block_time
df['block_date'] = pd.to_datetime(df['block_time']).dt.date

# Create a new dataframe with the required fields
new_df = df[['block_number', 'block_date', 'refund_value_eth']]

# Aggregate the new dataframe with respect to 'block_date'
new_df = new_df.groupby('block_date').agg({'refund_value_eth': 'sum', 'block_number': 'count'}).reset_index()

# Rename the columns
new_df = new_df.rename(columns={'refund_value_eth': 'daily_refund_value_eth', 'block_number': 'mev_share_bundle_count'})

# Write the dataframe to a csv file
new_df.to_csv('mevshare_by_date.csv', index=False)