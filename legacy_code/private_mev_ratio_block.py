import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from scipy.stats import pearsonr, spearmanr

# Read data from CSV file
data = pd.read_csv('blocknative_zeromev_flashbots_rq4.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
# data = data[(data['block_date'] > pd.Timestamp('2021-05-01')) & (data['block_date'] <= pd.Timestamp('2022-07-01'))]
data = data[data['block_date'] >= pd.Timestamp('2021-12-15')]

# Ensure the 'private_gasused_gwei' and 'public_gasused_gwei' are in numeric format
data['private_gasused_gwei'] = pd.to_numeric(data['private_gasused_gwei'], errors='coerce')
data['public_gasused_gwei'] = pd.to_numeric(data['public_gasused_gwei'], errors='coerce')

# Calculate 'private_gasused_pct'
data['private_gasused_pct'] = (data['private_gasused_gwei'] / (data['private_gasused_gwei'] + data['public_gasused_gwei'])) * 100

# Assign private tx percentage to every row (for every block)
data['private_tx_pct'] = np.where(data['tx_count'] > 0, (data['private_tx_count'] / data['tx_count']) * 100, 0)

# Calculate percentages for each MEV transaction type
transaction_types = ['arb_count', 'sandwich_count', 'liquid_count']
for tx_type in transaction_types:
    data[f'{tx_type}_pct'] = np.where(data['tx_count'] > 0, (data[tx_type] / data['tx_count']) * 100, 0)

# Aggregate data by block date, calculating the mean of percentages for each type
aggregated_data = data.groupby('block_date').agg({
    'private_tx_pct': 'mean',
    'private_gasused_pct': 'mean',
    **{f'{tx_type}_pct': 'mean' for tx_type in transaction_types},
}).reset_index()

# Calculate the average MEV transactions as the sum of transaction type percentages
aggregated_data['mev_tx_pct'] = aggregated_data[[f'{tx_type}_pct' for tx_type in transaction_types]].sum(axis=1)

# Apply a 14-day rolling window to smoothen the curves
aggregated_data['rolling_private_tx_pct'] = aggregated_data['private_tx_pct'].rolling(window=14).mean()
aggregated_data['rolling_private_gasused_pct'] = aggregated_data['private_gasused_pct'].rolling(window=14).mean()
aggregated_data['rolling_mev_tx_pct'] = aggregated_data['mev_tx_pct'].rolling(window=14).mean()

# before_ofa_data = aggregated_data[(aggregated_data['block_date'] > pd.Timestamp('2021-12-15')) & (aggregated_data['block_date'] <= pd.Timestamp('2023-04-27'))]

# Calculate Pearson and Spearman correlation coefficients on the raw data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['rolling_private_gasused_pct'], aggregated_data.dropna()['rolling_mev_tx_pct'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['rolling_private_gasused_pct'], aggregated_data.dropna()['rolling_mev_tx_pct'])

# Correlation coefficients before OFAs
# pearson_corr, pearson_p_value = pearsonr(before_ofa_data.dropna()['rolling_private_gasused_pct'], before_ofa_data.dropna()['rolling_mev_tx_pct'])
# spearman_corr, spearman_p_value = spearmanr(before_ofa_data.dropna()['rolling_private_gasused_pct'], before_ofa_data.dropna()['rolling_mev_tx_pct'])

# Plotting
fig, ax1 = plt.subplots(figsize=(8, 4))
plt.title('Daily Average Percentage of Private & MEV Transactions')
ax1.fill_between(aggregated_data['block_date'], 0, aggregated_data['rolling_private_gasused_pct'], color='skyblue', alpha=0.4, label='Average Gasused By Private Transactions (%)')
ax1.set_xlabel('Date')
ax1.set_ylabel('Average Percentage of Gas Used By Private Transactions', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

ax2 = ax1.twinx()
ax2.plot(aggregated_data['block_date'], aggregated_data['rolling_mev_tx_pct'], color='red', label='Average MEV Transactions (%) Window=14')
ax2.set_ylabel('Average Percentage of MEV Transactions', color='red')
ax2.tick_params(axis='y', labelcolor='red')

# Annotations for Pearson and Spearman correlations
# Moved below the figure for better visibility
ax1.annotate(f'Pearson Corr (private gasused vs mev tx count): {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', xy=(0.0, -0.2), xycoords='axes fraction', verticalalignment='top', horizontalalignment='left', color='black')
ax1.annotate(f'Spearman Corr (private gasused vs mev tx count): {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', xy=(0.0, -0.3), xycoords='axes fraction', verticalalignment='top', horizontalalignment='left', color='black')

# Significant dates as vertical lines (ensure these are defined correctly)
significant_dates = {
    # '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
    # Uncomment below for full data (after merge)
    '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

# Date formatting and legend setup
# Apply the x-tick rotation here
labels = ax1.get_xticklabels()
ax1.set_xticklabels(labels, rotation=0, ha='center')
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Set to every month
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

plt.grid(True)
fig.legend(loc="upper left", bbox_to_anchor=(0,1), bbox_transform=ax1.transAxes)
fig.tight_layout()
plt.savefig('figures/private_mev_ratio_gas_space.png')
plt.show()
