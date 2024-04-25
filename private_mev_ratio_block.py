import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

localhost_name = 'localhost'
db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

table_name = 'blocknative_zeromev'
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

# SQL query to retrieve transactions data aggregated by date
query = f"SELECT * FROM {table_name} ORDER BY block_date ASC"
data = pd.read_sql(query, engine)

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Aggregate data by date, using mean instead of sum
aggregated_data = data.groupby('block_date').agg({
    'tx_count': 'mean',
    'private_tx_count': 'mean',
    'arb_count': 'mean',
    'frontrun_count': 'mean',
    'sandwich_count': 'mean',
    'backrun_count': 'mean',
    'liquid_count': 'mean'
}).reset_index()

# Calculate the average MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']].mean(axis=1)

# Calculate percentages of averages
aggregated_data['private_tx_percentage'] = 100 * aggregated_data['private_tx_count'] / aggregated_data['tx_count']
aggregated_data['mev_tx_percentage'] = 100 * aggregated_data['mev_tx_count'] / aggregated_data['tx_count']

# Apply a rolling window for smoothing
aggregated_data['mean_private_tx_percentage'] = aggregated_data['private_tx_percentage'].rolling(window=14).mean()
aggregated_data['mean_mev_tx_percentage'] = aggregated_data['mev_tx_percentage'].rolling(window=14).mean()

# Plotting
fig, ax1 = plt.subplots(figsize=(8, 6))

plt.title('Block Average Per Day of Private & MEV Transactions')

# Plot for average private transactions percentage
ax1.fill_between(aggregated_data['block_date'], 0, aggregated_data['mean_private_tx_percentage'], color='skyblue', alpha=0.4, label='Average Private Transactions (%)')
ax1.set_xlabel('Date')
ax1.set_ylabel('Block Average Percentage of Private Transactions', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')
ax1.set_xlim(left=aggregated_data['block_date'].min())

ax2 = ax1.twinx()
# Plot for MEV transaction percentage
ax2.plot(aggregated_data['block_date'], aggregated_data['mean_mev_tx_percentage'], color='red', label='Average MEV Transactions (%)')
ax2.set_ylabel('Block Average Percentage of MEV Transaction Percentage', color='red')
ax2.tick_params(axis='y', labelcolor='red')

ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

# Apply the x-tick rotation here
labels = ax1.get_xticklabels()
ax1.set_xticklabels(labels, rotation=45, ha='right')

fig.tight_layout()
plt.grid(True)
fig.legend(loc="upper right", bbox_to_anchor=(1,1), bbox_transform=ax1.transAxes)
plt.savefig('figures/private_mev_ratio_block.png')
# plt.show()
