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
    'tx_count': 'mean',
    'private_tx_count': 'mean',
    'public_tx_count': 'mean',
    'arb_count': 'mean',
    'frontrun_count': 'mean',
    'sandwich_count': 'mean',
    'backrun_count': 'mean',
    'liquid_count': 'mean',
    'swap_count': 'mean',
}).reset_index()

# Apply a 14-day rolling window to each column
window_size = 14
smoothed_data = aggregated_data.copy()
for col in ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count', 'swap_count']:
    smoothed_data[col] = aggregated_data[col].rolling(window=window_size).mean()

# Calculating the bottom for each stack in the smoothed data
bottoms = smoothed_data[['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count', 'swap_count']].cumsum(axis=1).shift(axis=1).fillna(0)

# Plotting
plt.figure(figsize=(8, 6))

colors = ['skyblue', 'orange', 'green', 'red', 'purple', 'brown']
labels = ['Arb', 'Frontrun', 'Sandwich', 'Backrun', 'Liquid', 'Swap']

# Create a list to control the order of plotting explicitly
plot_order = ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count', 'swap_count']

for i, col in enumerate(plot_order):
    plt.fill_between(smoothed_data['block_date'], bottoms[col], smoothed_data[col] + bottoms[col], color=colors[i], alpha=0.4, label=labels[i])

plt.title('Ratio of Total MEV Transactions And Swaps per Day')
plt.xlabel('Date')
plt.ylabel('Averge Count of Transactions per Block')
plt.xlim(left=smoothed_data['block_date'].min())

ax = plt.gca()
ax.set_yscale('log')  # Set the y-axis to a logarithmic scale
ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=range(1, 13, 3)))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig('figures/mev_types_with_swap_block.png')
