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

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'tx_count': 'sum',
    'private_tx_count': 'sum',
    'arb_count': 'sum',
    'frontrun_count': 'sum',
    'sandwich_count': 'sum',
    'backrun_count': 'sum',
    'liquid_count': 'sum'
}).reset_index()

# Calculate the total MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']].sum(axis=1)

# Calculate percentages
aggregated_data['private_tx_percentage'] = 100 * aggregated_data['private_tx_count'] / aggregated_data['tx_count']
aggregated_data['mev_tx_percentage'] = 100 * aggregated_data['mev_tx_count'] / aggregated_data['tx_count']

# Apply a rolling window for smoothing
aggregated_data['mean_private_tx_percentage'] = aggregated_data['private_tx_percentage'].rolling(window=14).mean()
aggregated_data['mean_mev_tx_percentage'] = aggregated_data['mev_tx_percentage'].rolling(window=14).mean()

# Plotting
fig, ax1 = plt.subplots(figsize=(8, 6))  # Adjusted to fit on A4 paper in landscape orientation

plt.title('Total Number of Private & MEV Transactions Ratio Per Day')

# Plot for private transactions percentage
ax1.fill_between(aggregated_data['block_date'], 0, aggregated_data['mean_private_tx_percentage'], color='skyblue', alpha=0.4, label='Private Transactions (%)')
ax1.set_xlabel('Date')
ax1.set_ylabel('Percentage of Total Number of Private Transactions per Day', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')
ax1.set_xlim(left=aggregated_data['block_date'].min())

ax2 = ax1.twinx()  # Instantiate a second axes that shares the same x-axis
# Plot for MEV transaction percentage
ax2.plot(aggregated_data['block_date'], aggregated_data['mean_mev_tx_percentage'], color='red', label='MEV Transactions (%)')
ax2.set_ylabel('MEV Transaction Percentage per Day', color='red')
ax2.tick_params(axis='y', labelcolor='red')

ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Set to every four months
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

# Apply the x-tick rotation here
labels = ax1.get_xticklabels()
ax1.set_xticklabels(labels, rotation=45, ha='center')

fig.tight_layout()  # Call tight_layout after setting rotation to accommodate label spacing
plt.grid(True)
fig.legend(loc="upper right", bbox_to_anchor=(1,1), bbox_transform=ax1.transAxes)
plt.savefig('figures/private_mev_ratio.png')
# plt.show()