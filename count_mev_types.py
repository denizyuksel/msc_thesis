import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

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

# Apply a 14-day rolling average to the counts
for col in ['arb_count', 'liquid_count', 'sandwich_count']:
    aggregated_data[col] = aggregated_data[col].rolling(window=14, min_periods=1).mean()

# Plotting
plt.figure(figsize=(8, 4))

colors = ['skyblue', 'magenta', 'orange']
labels = ['Arb', 'Liquid', 'Sandwich']

# Start from 0 bottom, incrementally add the previous smoothed count
bottom = pd.Series([0] * len(aggregated_data))

for i, col in enumerate(['arb_count', 'liquid_count', 'sandwich_count']):
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[col], color=colors[i], label=labels[i], step='mid', alpha=0.4)
    bottom += aggregated_data[col]

plt.title('Count of Each MEV Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Count of MEV Transactions per Day')
plt.xlim(left=aggregated_data['block_date'].min())
plt.ylim(0)  # Setting the lower y-axis limit to 0

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

# Significant dates as vertical lines
significant_dates = {
    '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
    '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

plt.grid(True)
plt.xticks(rotation=45)
plt.legend(loc="upper left", fontsize='x-small', bbox_to_anchor=(0,1), bbox_transform=ax.transAxes)
plt.tight_layout()
plt.savefig('figures/count_mev_types.png')
# plt.show()
