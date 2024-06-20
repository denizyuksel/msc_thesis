import pandas as pd
import numpy as np

# Read the csv file into a pandas dataframe
blocknative_df = pd.read_csv('aggregated_transactions.csv')

# Aggregate by date and sum the counts
date_aggregation = blocknative_df.groupby('block_date').agg(
    total_tx_count=('tx_count', 'sum'),
    total_private_tx_count=('private_tx_count', 'sum')
).reset_index()

date_aggregation.to_csv('date_aggregated_transactions.csv', index=False)
