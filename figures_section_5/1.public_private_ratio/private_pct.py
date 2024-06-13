import pandas as pd

data = pd.read_csv('../final_data.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'arb_count': 'sum',
    'liquid_count': 'sum',
    # 'frontrun_count': 'sum',
    'sandwich_count': 'sum',
    # 'backrun_count': 'sum',
}).reset_index()