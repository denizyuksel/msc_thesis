import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import pearsonr, spearmanr
import numpy as np

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
# data = data[(data['block_date'] > pd.Timestamp('2021-03-01')) & (data['block_date'] <= pd.Timestamp('2022-07-01'))]

# Assign private tx percentage to every row (for every block)
data['private_tx_pct'] = np.where(data['tx_count'] > 0, (data['private_tx_count'] / data['tx_count']) * 100, 0)

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'tx_count': 'sum',
    'private_tx_count': 'sum',
    'private_tx_pct': 'mean',
    'arb_count': 'sum',
    # 'frontrun_count': 'sum',
    'sandwich_count': 'sum',
    # 'backrun_count': 'sum',
    'liquid_count': 'sum',
    'bundle_tx_count': 'sum',
    'total_user_swap_volume': 'sum',
    'total_extractor_profit': 'sum',
}).reset_index()

# Apply a rolling window for smoothing
# Calculate the total MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['arb_count', 'sandwich_count', 'liquid_count']].sum(axis=1)

# Apply a rolling window for smoothing
aggregated_data['rolling_bundle_tx'] = aggregated_data['bundle_tx_count'].rolling(window=14).mean()
aggregated_data['rolling_extractor_profit'] = aggregated_data['total_extractor_profit'].rolling(window=14).mean()
aggregated_data['rolling_user_swap_volume'] = aggregated_data['total_user_swap_volume'].rolling(window=14).mean()
aggregated_data['rolling_private_tx_pct'] = aggregated_data['private_tx_pct'].rolling(window=14).mean()

# Calculate Pearson and Spearman correlation coefficients on the raw data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['rolling_private_tx_pct'], aggregated_data.dropna()['rolling_bundle_tx'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['rolling_private_tx_pct'], aggregated_data.dropna()['rolling_bundle_tx'])

# Plotting
fig, ax = plt.subplots(figsize=(8, 4))

# Annotations for Pearson and Spearman correlations
# Moved below the figure for better visibility
ax.annotate(f'Pearson Corr (private % vs bundle count): {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', xy=(0.0, -0.2), xycoords='axes fraction', verticalalignment='top', horizontalalignment='left', color='black')
ax.annotate(f'Spearman Corr (private % vs bundle count): {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', xy=(0.0, -0.3), xycoords='axes fraction', verticalalignment='top', horizontalalignment='left', color='black')

# Significant dates as vertical lines (ensure these are defined correctly)
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

# Plotting
ax.plot(aggregated_data['block_date'], aggregated_data['rolling_bundle_tx'], color='skyblue', label='Bundle Count')
ax.set_xlabel('Date')
ax.set_ylabel('Count per Day', color='blue')
ax.tick_params(axis='y', labelcolor='blue')
ax.set_xlim(left=aggregated_data['block_date'].min())

# Second Y-axis for the extractor profit
ax2 = ax.twinx()
ax2.plot(aggregated_data['block_date'], aggregated_data['rolling_private_tx_pct'], color='green', label='Private Transaction Percentage')
ax2.set_ylabel('Percentage', color='green')
ax2.tick_params(axis='y', labelcolor='green')
# ax2.set_ylim(0, 100)

plt.title('Bundle Counts and Private Transaction Percentage (Pre-Merge)')
plt.xticks(rotation=45, ha='right')  # Adjust for better label alignment
plt.tight_layout()

# Place the grid behind other plot elements
ax.set_axisbelow(True)
ax.grid(True)

# Combine legends from both axes and improve legend placement
lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
legend = ax.legend(lines + lines2, labels + labels2, loc='upper right', fontsize='x-small', framealpha=1)
legend.set_zorder(100)  # Set a high z-order to ensure it's on top

plt.savefig('figures/private_pct_bundle.png')  # Make sure the directory exists
# plt.show()
