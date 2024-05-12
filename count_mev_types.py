import pandas as pd
from sqlalchemy import create_engine
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

fb_launch_date = '2021-10-06'
the_merge = '2022-09-15'
ftx_collapse = '2022-11-11'
usdc_depeg = '2023-03-11'
mev_blocker_launch_date = '2023-04-27'
# Dashed lines for the specified dates
ax.axvline(pd.Timestamp(fb_launch_date), color='darkred', linestyle='--', linewidth=2, label='Flashbots Protect Launch Date (Oct 2021)')
ax.axvline(pd.Timestamp(the_merge), color='red', linestyle='-.', linewidth=2, label='The Merge (Sept 2022)')
ax.axvline(pd.Timestamp(ftx_collapse), color='deepskyblue', linestyle=':', linewidth=2, label='FTX Collapse Date (Nov 2022)')
ax.axvline(pd.Timestamp(usdc_depeg), color='fuchsia', linestyle='--', linewidth=2, label='USDC Depeg Date (Mar 2023)')
ax.axvline(pd.Timestamp(mev_blocker_launch_date), color='indigo', linestyle='-.', linewidth=2, label='MEV Blocker Launch Date (Apr 2023)')

plt.grid(True)
plt.xticks(rotation=45)
plt.legend(loc="upper left", fontsize='x-small', bbox_to_anchor=(0,1), bbox_transform=ax.transAxes)
plt.tight_layout()
plt.savefig('figures/count_mev_types.png')
# plt.show()
