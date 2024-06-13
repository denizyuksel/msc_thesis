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
    'arb_count': 'mean',
    'frontrun_count': 'mean',
    'sandwich_count': 'mean',
    'backrun_count': 'mean',
    'liquid_count': 'mean',
    'swap_count': 'mean'
}).reset_index()

# Apply a 14-day rolling average to the counts
for col in ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count', 'swap_count']:
    aggregated_data[col] = aggregated_data[col].rolling(window=14, min_periods=1).mean()

# Plotting
plt.figure(figsize=(12, 6))

colors = ['skyblue', 'orange', 'green', 'red', 'purple', 'pink']
labels = ['Arb', 'Frontrun', 'Sandwich', 'Backrun', 'Liquid', 'Swap']

# Start from 0 bottom, incrementally add the previous smoothed count
bottom = pd.Series([0] * len(aggregated_data))

for i, col in enumerate(['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count', 'swap_count']):
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[col], color=colors[i], label=labels[i], step='mid', alpha=0.6)
    bottom += aggregated_data[col]

plt.title('Average Count per Block of Each MEV Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Average Block Count of MEV Transactions per Day')
plt.xlim(left=aggregated_data['block_date'].min())
plt.ylim(bottom=1)  # Set a minimum y value to avoid taking log of zero

ax = plt.gca()
ax.set_yscale('log')  # Setting the y-axis to a logarithmic scale

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend(loc='upper left')
plt.tight_layout()
plt.savefig('figures/count_mev_types_with_swap_block.png')
# plt.show()
