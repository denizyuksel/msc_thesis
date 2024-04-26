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

# Calculate the total MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']].sum(axis=1)

# Calculate percentages for each transaction type relative to MEV transactions
transaction_types = ['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']
for tx_type in transaction_types:
    aggregated_data[f'{tx_type}_pct'] = (aggregated_data[tx_type] / aggregated_data['mev_tx_count']) * 100

# Apply a 10-day rolling average to the percentage data
for tx_type in transaction_types:
    pct_col = f'{tx_type}_pct'
    aggregated_data[pct_col] = aggregated_data[pct_col].rolling(window=14, min_periods=1).mean()

# Plotting
plt.figure(figsize=(8, 4))

colors = ['skyblue', 'orange', 'green', 'red', 'purple']
labels = ['Arb', 'Frontrun', 'Sandwich', 'Backrun', 'Liquid']

# Start from 0 bottom, incrementally add the previous percentage
bottom = 0
for i, col in enumerate(transaction_types):
    pct_col = f'{col}_pct'
    plt.fill_between(aggregated_data['block_date'], bottom, bottom + aggregated_data[pct_col], color=colors[i], alpha=0.4, label=labels[i])
    bottom += aggregated_data[pct_col]

plt.title('Percentage of Each MEV Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Percentage of Total MEV Transactions per Day')
plt.ylim(0, 100)  # Ensuring y-axis is capped at 100%
plt.xlim(left=aggregated_data['block_date'].min())

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig('figures/pct_mev_types.png')
# plt.show()