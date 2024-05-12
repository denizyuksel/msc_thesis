import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import pearsonr, spearmanr

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
# data = data[(data['block_date'] > pd.Timestamp('2021-03-01')) & (data['block_date'] <= pd.Timestamp('2022-07-01'))]

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'tx_count': 'sum',
    'private_tx_count': 'sum',
    'arb_count': 'sum',
    # 'frontrun_count': 'sum',
    'sandwich_count': 'sum',
    # 'backrun_count': 'sum',
    'liquid_count': 'sum',
    'bundle_tx_count': 'sum',
}).reset_index()

# Calculate the total MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['arb_count', 'sandwich_count', 'liquid_count']].sum(axis=1)

# Calculate percentages
# aggregated_data['private_tx_percentage'] = 100 * aggregated_data['private_tx_count'] / aggregated_data['tx_count']
# aggregated_data['mev_tx_percentage'] = 100 * aggregated_data['mev_tx_count'] / aggregated_data['tx_count']

# Apply a rolling window for smoothing
aggregated_data['rolling_private_tx'] = aggregated_data['private_tx_count'].rolling(window=14).mean()
aggregated_data['rolling_mev_tx'] = aggregated_data['mev_tx_count'].rolling(window=14).mean()
aggregated_data['rolling_bundle_tx'] = aggregated_data['bundle_tx_count'].rolling(window=14).mean()

# Calculate Pearson and Spearman correlation coefficients on the raw data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['private_tx_count'], aggregated_data.dropna()['bundle_tx_count'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['private_tx_count'], aggregated_data.dropna()['bundle_tx_count'])

# Plotting
fig, ax = plt.subplots(figsize=(8, 4))  # Adjusted to fit on A4 paper in landscape orientation

plt.title('Total Number of MEV Transactions Per Day')
ax.plot(aggregated_data['block_date'], aggregated_data['rolling_mev_tx'], color='red', label='MEV Count')
ax.plot(aggregated_data['block_date'], aggregated_data['rolling_bundle_tx'], color='green', label='Bundle Count')
ax.plot(aggregated_data['block_date'], aggregated_data['rolling_private_tx'], color='skyblue', label='Private Count')
ax.set_xlabel('Date')
ax.set_ylabel('Daily MEV Transaction Count', color='red')
ax.tick_params(axis='y', labelcolor='red')
ax.set_xlim(left=aggregated_data['block_date'].min())

plt.title('Pre-Merge Number of Private, MEV Transactions & Bundle Counts Per Day')

# Plot for private transactions percentage
ax.set_xlabel('Date')
ax.set_ylabel('Count per Day', color='blue')
ax.tick_params(axis='y', labelcolor='blue')
ax.set_xlim(left=aggregated_data['block_date'].min())


# Significant dates as vertical lines
significant_dates = {
    '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
    # Uncomment below for full data (after merge)
    # '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    # '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    # '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    # '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Set to every four months
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

# Annotate Pearson and Spearman correlation coefficients in the top right
ax.annotate(f'Pearson Corr (private vs bundle): {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', xy=(0, 1), xycoords='axes fraction', verticalalignment='top', horizontalalignment='left', color='black')
ax.annotate(f'Spearman Corr (private vs bundle): {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', xy=(0, 0.95), xycoords='axes fraction', verticalalignment='top', horizontalalignment='left', color='black')

# Rotate x-tick labels
plt.xticks(rotation=45, ha='center')

# Tight layout for label spacing
plt.tight_layout()

# Add grid
plt.grid(True)

# Move the legend to the top right
plt.legend(loc="upper right", bbox_to_anchor=(1,1), bbox_transform=ax.transAxes)

plt.grid(True)
plt.savefig('figures/private_mev_bundle.png')
# plt.show()