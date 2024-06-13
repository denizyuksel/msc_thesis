import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import spearmanr

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
fig, ax1 = plt.subplots(figsize=(10, 6))

plt.title('Block Average Per Day of Private & MEV Transactions')

# Plot for average private transactions percentage
ax1.fill_between(aggregated_data['block_date'], 0, aggregated_data['mean_private_tx_percentage'], color='skyblue', alpha=0.4, label='Average Private Transactions (%)')
ax1.set_xlabel('Date')
ax1.set_ylabel('Block Average Percentage of Private Transactions', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

ax2 = ax1.twinx()
# Plot for MEV transaction percentage
ax2.plot(aggregated_data['block_date'], aggregated_data['mean_mev_tx_percentage'], color='red', label='Average MEV Transactions (%)')
ax2.set_ylabel('Block Average Percentage of MEV Transaction Percentage', color='red')
ax2.tick_params(axis='y', labelcolor='red')

ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

start_date = '2022-02-15'
end_date = '2023-06-01'
# Dashed lines for the specified dates
ax1.axvline(pd.Timestamp(start_date), color='green', linestyle='--', label='Start Date (Mid-Feb 2022)')
ax1.axvline(pd.Timestamp(end_date), color='orange', linestyle='--', label='End Date (June 2023)')

# Calculate and annotate Pearson correlation coefficient
mask = (aggregated_data['block_date'] >= start_date) & (aggregated_data['block_date'] <= end_date)
subset_data = aggregated_data.loc[mask]
# Calculate and annotate Spearman correlation coefficient
correlation, p_value = spearmanr(subset_data['mean_private_tx_percentage'], subset_data['mean_mev_tx_percentage'])

ax1.annotate(f'Spearman Corr: {correlation:.2f}', xy=(0.5, 0.9), xycoords='axes fraction', color='black')
ax1.annotate(f'p-value: {p_value:.3f}', xy=(0.5, 0.85), xycoords='axes fraction', color='black')

# Apply the x-tick rotation here
labels = ax1.get_xticklabels()
ax1.set_xticklabels(labels, rotation=45, ha='center')

fig.tight_layout()
plt.grid(True)
fig.legend(loc="upper left", bbox_to_anchor=(0,1), bbox_transform=ax1.transAxes)
plt.savefig('figures/correlation_spearman.png')
# plt.show()
