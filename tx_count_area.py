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
    'public_tx_count': 'sum',
    'arb_count': 'sum',
    'frontrun_count': 'sum',
    'sandwich_count': 'sum',
    'backrun_count': 'sum',
    'liquid_count': 'sum'
}).reset_index()

# Calculate the percentages relative to total transactions
aggregated_data['private_tx_pct'] = (aggregated_data['private_tx_count'] / aggregated_data['tx_count']) * 100
aggregated_data['public_tx_pct'] = (aggregated_data['public_tx_count'] / aggregated_data['tx_count']) * 100

# Apply a 10-day rolling average to the percentage data
aggregated_data['private_tx_pct_smooth'] = aggregated_data['private_tx_pct'].rolling(window=1, min_periods=1).mean()
aggregated_data['public_tx_pct_smooth'] = aggregated_data['public_tx_pct'].rolling(window=1, min_periods=1).mean()

# Plotting
plt.figure(figsize=(8, 4))

# Plot both private and public transactions percentage curves
plt.fill_between(aggregated_data['block_date'], 0, aggregated_data['private_tx_pct_smooth'], color='skyblue', alpha=0.7, label='Private Transactions')
plt.fill_between(aggregated_data['block_date'], aggregated_data['private_tx_pct_smooth'], aggregated_data['private_tx_pct_smooth'] + aggregated_data['public_tx_pct_smooth'], color='orange', alpha=0.4, label='Public Transactions')

plt.title('Percentage of Private and Public Transactions Over Time')
plt.xlabel('Date')
plt.ylabel('Percentage of Transactions')
plt.xlim(left=aggregated_data['block_date'].min())

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))  # Show every fourth month
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig('figures/tx_percentage_stacked_area.png')
plt.show()
