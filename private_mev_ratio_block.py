import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Database connection setup
localhost_name = 'localhost'
db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

# SQL query to fetch data
table_name = 'blocknative_zeromev'
query = f"SELECT block_number, block_date, tx_count, private_tx_count, arb_count, frontrun_count, sandwich_count, backrun_count, liquid_count FROM {table_name} ORDER BY block_date ASC"
data = pd.read_sql(query, engine)

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Define a safe division function
def safe_division(numerator, denominator):
    return 100 * numerator / denominator if denominator != 0 else 0

# Calculate private transaction percentage for each row
data['private_tx_pct'] = data.apply(lambda x: safe_division(x['private_tx_count'], x['tx_count']), axis=1)

# Calculate percentages for each MEV transaction type
transaction_types = ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']
for tx_type in transaction_types:
    data[f'{tx_type}_pct'] = data.apply(lambda x: safe_division(x[tx_type], x['tx_count']), axis=1)

# Aggregate data by block date, calculating the mean of percentages for each type
aggregated_data = data.groupby('block_date').agg({
    'private_tx_pct': 'mean',
    **{f'{tx_type}_pct': 'mean' for tx_type in transaction_types},
}).reset_index()

# Calculate the average MEV transactions as the sum of transaction type percentages
aggregated_data['mev_tx_pct'] = aggregated_data[[f'{tx_type}_pct' for tx_type in transaction_types]].sum(axis=1)

# Apply a 14-day rolling window to smoothen the curves
aggregated_data['mean_private_tx_pct'] = aggregated_data['private_tx_pct'].rolling(window=1).mean()
aggregated_data['mean_mev_tx_pct'] = aggregated_data['mev_tx_pct'].rolling(window=14).mean()

# Plotting
fig, ax1 = plt.subplots(figsize=(8, 4))
plt.title('Daily Average Percentage of Private & MEV Transactions')
ax1.fill_between(aggregated_data['block_date'], 0, aggregated_data['mean_private_tx_pct'], color='skyblue', alpha=0.4, label='Average Private Transactions (%)')
ax1.set_xlabel('Date')
ax1.set_ylabel('Average Percentage of Private Transactions', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

ax2 = ax1.twinx()
ax2.plot(aggregated_data['block_date'], aggregated_data['mean_mev_tx_pct'], color='red', label='Average MEV Transactions (%) Window=14')
ax2.set_ylabel('Average Percentage of MEV Transactions', color='red')
ax2.tick_params(axis='y', labelcolor='red')

# Important dates
fb_launch_date = '2021-10-06'
the_merge = '2022-09-15'
ftx_collapse = '2022-11-11'
usdc_depeg = '2023-03-11'
mev_blocker_launch_date = '2023-04-27'
# Dashed lines for the specified dates
ax1.axvline(pd.Timestamp(fb_launch_date), color='darkred', linestyle='--', linewidth=2, label='Flashbots Protect Launch Date (Oct 2021)')
ax1.axvline(pd.Timestamp(the_merge), color='red', linestyle='-.', linewidth=2, label='The Merge (Sept 2022)')
ax1.axvline(pd.Timestamp(ftx_collapse), color='deepskyblue', linestyle=':', linewidth=2, label='FTX Collapse Date (Nov 2022)')
ax1.axvline(pd.Timestamp(usdc_depeg), color='fuchsia', linestyle='--', linewidth=2, label='USDC Depeg Date (Mar 2023)')
ax1.axvline(pd.Timestamp(mev_blocker_launch_date), color='indigo', linestyle='-.', linewidth=2, label='MEV Blocker Launch Date (Apr 2023)')

# Date formatting and legend setup
# Apply the x-tick rotation here
labels = ax1.get_xticklabels()
ax1.set_xticklabels(labels, rotation=45, ha='center')
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Set to every month
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

plt.grid(True)
fig.legend(loc="upper left", bbox_to_anchor=(0,1), bbox_transform=ax1.transAxes)
fig.tight_layout()
plt.savefig('figures/private_mev_ratio_block.png')
# plt.show()
