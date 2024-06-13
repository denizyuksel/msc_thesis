import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'arb_count': 'sum',
    'liquid_count': 'sum',
    'sandwich_count': 'sum'
}).reset_index()

# Calculate the total MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['liquid_count', 'arb_count', 'sandwich_count']].sum(axis=1)

# Calculate percentages for each transaction type relative to MEV transactions
transaction_types = ['liquid_count', 'arb_count', 'sandwich_count']
for tx_type in transaction_types:
    aggregated_data[f'{tx_type}_pct'] = (aggregated_data[tx_type] / aggregated_data['mev_tx_count']) * 100

# Apply a 10-day rolling average to the percentage data
for tx_type in transaction_types:
    pct_col = f'{tx_type}_pct'
    aggregated_data[pct_col] = aggregated_data[pct_col].rolling(window=14, min_periods=1).mean()

# Plotting
plt.figure(figsize=(8, 4))

colors = ['magenta', 'skyblue', 'orange']
labels = ['Liquid', 'Arb', 'Sandwich']

# Start from 0 bottom, incrementally add the previous percentage
bottom = 0
for i, col in enumerate(transaction_types):
    pct_col = f'{col}_pct'
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[pct_col], color=colors[i], alpha=0.4, label=labels[i])
    bottom += aggregated_data[pct_col]

plt.title('Percentage of Each MEV Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Percentage')
plt.ylim(0, 100)  # Ensuring y-axis is capped at 100%
plt.xlim(left=aggregated_data['block_date'].min())

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

# Significant dates as vertical lines
significant_dates = {
    "2021-10-06": ("darkred", "--", "Flashbots Protect Launch Date (Oct 2021)"),
    # Uncomment below for full data (after merge)
    '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax.axvline(
        pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label
    )
plt.grid(True)
plt.xticks(rotation=45)

plt.legend(loc="upper left", fontsize='x-small', bbox_to_anchor=(0,1), bbox_transform=ax.transAxes)
# plt.legend(fontsize='x-small')
plt.tight_layout()
plt.savefig('figures/pct_mev_types.png')
# plt.show()