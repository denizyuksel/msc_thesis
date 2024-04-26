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
    'arb_count': 'sum',
    'frontrun_count': 'sum',
    'sandwich_count': 'sum',
    'backrun_count': 'sum',
    'liquid_count': 'sum'
}).reset_index()

# Apply a 14-day rolling average to the counts
for col in ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']:
    aggregated_data[col] = aggregated_data[col].rolling(window=14, min_periods=1).mean()

# Plotting
plt.figure(figsize=(10, 5))

colors = ['skyblue', 'orange', 'green', 'red', 'purple']
labels = ['Arb', 'Frontrun', 'Sandwich', 'Backrun', 'Liquid']

# Start from 0 bottom, incrementally add the previous smoothed count
bottom = 0
for i, col in enumerate(['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']):
    plt.bar(aggregated_data['block_date'], aggregated_data[col], bottom=bottom, color=colors[i], label=labels[i])
    bottom += aggregated_data[col]

plt.title('Smoothed Count of Each MEV Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Smoothed Count of Transactions per Day')
plt.xlim(left=aggregated_data['block_date'].min())

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend(loc='upper right')
plt.tight_layout()
plt.savefig('figures/count_bar_without_swap.png')
# plt.show()
