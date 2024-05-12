import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Setting up the connection to the database
localhost_name = 'localhost'
db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

# SQL query to retrieve transactions data aggregated by date
table_name = 'blocknative_zeromev'
query = f"SELECT block_number, block_date, arb_count, frontrun_count, sandwich_count, backrun_count, liquid_count, private_tx_count FROM {table_name} ORDER BY block_number ASC"
data = pd.read_sql(query, engine)

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Define a safe division function
def safe_division(numerator, denominator):
    return numerator / denominator if denominator != 0 else 0

# Calculate percentages for each transaction type
transaction_types = ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']
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
colors = ['skyblue', 'orange', 'green', 'red', 'purple']
labels = ['Arb', 'Frontrun', 'Sandwich', 'Backrun', 'Liquid']

# Incremental plot
bottom = 0
for i, tx_type in enumerate(transaction_types):
    pct_col = f'{tx_type}_pct'
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[pct_col], color=colors[i], alpha=0.4, label=labels[i])
    bottom += aggregated_data[pct_col]

plt.title('Percentage of Each MEV Transaction Type Over Time (Block Average)')
plt.xlabel('Date')
plt.ylabel('Block Average Percentage of MEV Transactions')
plt.ylim(0, 100)  # Cap y-axis at 100%
plt.xlim(left=aggregated_data['block_date'].min())

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
plt.savefig('figures/pct_mev_types_block.png')
# plt.show()