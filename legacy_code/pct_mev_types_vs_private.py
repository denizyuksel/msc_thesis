import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Define a safe division function
def safe_division(numerator, denominator):
    return numerator / denominator if denominator != 0 else 0

# Calculate percentages for each transaction type
transaction_types = ['liquid_count', 'arb_count', 'sandwich_count']
for tx_type in transaction_types:
    data[f'{tx_type}_pct'] = data.apply(lambda x: safe_division(x[tx_type], x['private_tx_count']) * 100, axis=1)

# Aggregate percentage data by date using the mean for each type
aggregated_data = data.groupby('block_date').agg({f'{tx_type}_pct': 'mean' for tx_type in transaction_types}).reset_index()

# Apply a 14-day rolling average to the percentage data
for tx_type in transaction_types:
    pct_col = f'{tx_type}_pct'
    aggregated_data[pct_col] = aggregated_data[pct_col].rolling(window=14, min_periods=1).mean()

# Plotting
plt.figure(figsize=(8, 4))
colors = ['purple', 'skyblue', 'orange']
labels = [ 'Liquid', 'Arb', 'Sandwich']

# Incremental plot
bottom = 0
for i, tx_type in enumerate(transaction_types):
    pct_col = f'{tx_type}_pct'
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[pct_col], color=colors[i], alpha=0.4, label=labels[i])
    bottom += aggregated_data[pct_col]

plt.title('Percentage of Each MEV Transactions Type Over Time (Block Average)')
plt.xlabel('Date')
plt.ylabel('Block Average Percentage of MEV Transactions')
# plt.ylim(0, 100)  # Cap y-axis at 100%
plt.xlim(left=aggregated_data['block_date'].min())

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

# Significant dates as vertical lines (ensure these are defined correctly)
significant_dates = {
    '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
    # Uncomment below for full data (after merge)
    '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

plt.grid(True)
plt.xticks(rotation=45)
legend = plt.legend(loc="upper left", fontsize='x-small', bbox_to_anchor=(0,1), bbox_transform=ax.transAxes)
legend.set_zorder(5)  # Set a high zorder to ensure the legend is on top of the grid

plt.tight_layout()
plt.savefig('figures/pct_mev_types_block.png')
# plt.show()