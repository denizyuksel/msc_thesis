import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import pearsonr, spearmanr

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
    'total_user_swap_volume': 'sum',
    'total_extractor_profit': 'sum'
}).reset_index()

# Apply a 7-day rolling window to smooth the data
aggregated_data['smoothed_user_swap_volume'] = aggregated_data['total_user_swap_volume'].rolling(window=14, center=True).mean()
aggregated_data['smoothed_extractor_profit'] = aggregated_data['total_extractor_profit'].rolling(window=14, center=True).mean()

# Calculate Pearson and Spearman correlation coefficients on the smoothed data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['smoothed_user_swap_volume'], aggregated_data.dropna()['smoothed_extractor_profit'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['smoothed_user_swap_volume'], aggregated_data.dropna()['smoothed_extractor_profit'])

# Plotting
fig, ax = plt.subplots(figsize=(10, 6))
ax.set_yscale('log')  # Setting the y-axis to logarithmic scale

plt.title('Daily Total User Swap Volume and Extractor Profit (2-Week Rolling Window)')

# Plot for smoothed total user swap volume and total extractor profit
ax.plot(aggregated_data['block_date'], aggregated_data['smoothed_user_swap_volume'], color='cyan', label='Smoothed Total User Swap Volume (USD)')
ax.plot(aggregated_data['block_date'], aggregated_data['smoothed_extractor_profit'], color='magenta', label='Smoothed Total Extractor Profit (USD)')
ax.set_xlabel('Date')
ax.set_ylabel('Amount (USD - Log Scale)')

mev_blocker_launch_date = '2023-04-27'
ftx_collapse = '2022-11-11'
usdc_depeg = '2023-03-11'
# Dashed lines for the specified dates
ax.axvline(pd.Timestamp(mev_blocker_launch_date), color='green', linestyle='--', label='MEV Blocker Launch Date (Apr 2023)')
ax.axvline(pd.Timestamp(ftx_collapse), color='orange', linestyle='--', label='FTX Collapse Date (Nov 2022)')
ax.axvline(pd.Timestamp(usdc_depeg), color='red', linestyle='--', label='USDC Depeg Date (Mar 2023)')

# Annotate Pearson and Spearman correlation coefficients in the top right
ax.annotate(f'Pearson Corr: {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', xy=(1, 1), xycoords='axes fraction', verticalalignment='top', horizontalalignment='right', color='black')
ax.annotate(f'Spearman Corr: {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', xy=(1, 0.95), xycoords='axes fraction', verticalalignment='top', horizontalalignment='right', color='black')

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

# Apply the x-tick rotation here
labels = ax.get_xticklabels()
plt.setp(labels, rotation=45, horizontalalignment='right')

fig.tight_layout()
plt.grid(True)
ax.legend(loc="upper left", bbox_to_anchor=(0,1), bbox_transform=ax.transAxes)
plt.savefig('figures/swap_vs_profit.png')
# plt.show()
