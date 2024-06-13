import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Uncomment below for full data (after merge)
# data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
data = data[(data['block_date'] > pd.Timestamp('2021-03-01')) & (data['block_date'] <= pd.Timestamp('2022-07-01'))]

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'arb_count': 'sum',
    'liquid_count': 'sum',
    'sandwich_count': 'sum',
    'private_tx_count': 'sum',
    'bundle_tx_count': 'sum'  # Aggregate the new column as well
}).reset_index()

aggregated_data['mev_tx_count'] = aggregated_data[['liquid_count', 'arb_count', 'sandwich_count']].sum(axis=1)

# Apply a 14-day rolling average to the counts
for col in ['arb_count', 'liquid_count', 'sandwich_count', 'bundle_tx_count', 'mev_tx_count', 'private_tx_count']:
    aggregated_data[col] = aggregated_data[col].rolling(window=14, min_periods=1).mean()

# Calculate Pearson and Spearman correlation coefficients on the raw data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['private_tx_count'], aggregated_data.dropna()['bundle_tx_count'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['private_tx_count'], aggregated_data.dropna()['bundle_tx_count'])

# Plotting
plt.figure(figsize=(8, 4))

colors = ['magenta', 'skyblue', 'orange']
labels = ['Liquid', 'Arb', 'Sandwich']

# Start from 0 bottom, incrementally add the previous smoothed count
bottom = pd.Series([0] * len(aggregated_data))

for i, col in enumerate(['liquid_count', 'arb_count', 'sandwich_count']):
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[col], color=colors[i], label=labels[i], step='mid', alpha=0.4)
    bottom += aggregated_data[col]
    
    # Uncomment to choose between plot and area chart.
    # plt.plot(aggregated_data['block_date'], aggregated_data[col], color=colors[i], label=labels[i], linestyle='-', alpha=0.7)

plt.title('Count of MEV Transactions and Flashbots Blocks Over Time (Pre-Merge)')
plt.xlabel('Date')
plt.ylabel('Count of MEV Transactions per Day')

# Set up the primary x-axis
ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.xticks(rotation=0)

# Create a second y-axis for the new column
ax2 = ax.twinx()
ax2.plot(aggregated_data['block_date'], aggregated_data['bundle_tx_count'], color='blue', label='Flashbots Bundle Transactions', linestyle='-')  # Make line solid
ax2.set_ylabel('Flashbots Bundle Count', color='blue')
ax2.tick_params(axis='y', colors='blue')

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

# Adjust plot layout to make space at the bottom for annotations
plt.subplots_adjust(bottom=0.45)

# Annotate Pearson and Spearman correlation coefficients below the plot
plt.figtext(0.1, 0.05, f'Pearson Corr (private vs bundle count): {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', horizontalalignment='left', color='black', fontsize='x-small')
plt.figtext(0.1, 0.02, f'Spearman Corr (private vs bundle count): {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', horizontalalignment='left', color='black', fontsize='x-small')

# Combine legends from both axes and improve legend placement
lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
legend = ax.legend(lines + lines2, labels + labels2, loc='upper right', fontsize='x-small', framealpha=1)
legend.set_zorder(100)  # Set a high z-order to ensure it's on top

plt.grid(True)
plt.tight_layout()
plt.savefig('figures/count_mev_types_vs_flashbots_private.png')
# plt.show()  # Uncomment to display the plot directly
