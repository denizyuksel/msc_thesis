import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('blocknative_zeromev_flashbots_rq4.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'private_gasused_gwei': 'median',
    'public_gasused_gwei': 'sum',
    'private_mean_gasused': 'mean',
    'private_median_gasused': 'median',
    'public_mean_gasused': 'mean',
    'public_median_gasused': 'median',
    'mean_gasprice_gwei': 'mean',
    'median_gasprice_gwei': 'median',
    'private_tx_count': 'sum',
    'public_tx_count': 'sum',
    'timepending_block_total': 'mean',
    'mean_timepending': 'mean',
    'median_timepending': 'mean',
}).reset_index()

# Apply 14-day rolling average
# aggregated_data['private_gasused_gwei'] = aggregated_data['private_gasused_gwei'].rolling(window=14).mean()
aggregated_data['median_timepending'] = aggregated_data['median_timepending'].rolling(window=14).mean()

# Drop data where gasused field is not defined
aggregated_data = aggregated_data.dropna(subset=['private_gasused_gwei'])

# Plotting
plt.figure(figsize=(12, 6))
plt.scatter(aggregated_data['private_gasused_gwei'], aggregated_data['median_timepending'], alpha=0.5, label='Data points')
plt.xlabel('Private Block Gas Used (Gwei)')
plt.ylabel('Median Time Pending (sec)')
plt.yscale('log')
plt.title('Scatter Plot of Private Gas Used by a Block vs Median Time Pending')
plt.grid(True)
plt.legend()
plt.show()
