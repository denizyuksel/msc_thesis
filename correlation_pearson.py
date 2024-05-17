import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import pearsonr

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Aggregate data by date, using mean instead of sum
aggregated_data = data.groupby('block_date').agg({
    'tx_count': 'mean',
    'private_tx_count': 'mean',
    'arb_count': 'mean',
    'sandwich_count': 'mean',
    'liquid_count': 'mean'
}).reset_index()

# Calculate the average MEV transactions
aggregated_data['mev_tx_count'] = aggregated_data[['arb_count', 'liquid_count', 'sandwich_count']].mean(axis=1)

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

start_date = '2021-07-01'
end_date = '2024-03-01'
# Dashed lines for the specified dates
ax1.axvline(pd.Timestamp(start_date), color='green', linestyle='--', label='Start Date (July 2021)')
ax1.axvline(pd.Timestamp(end_date), color='orange', linestyle='--', label='End Date (Mar 2024)')

# Calculate and annotate Pearson correlation coefficient
mask = (aggregated_data['block_date'] >= start_date) & (aggregated_data['block_date'] <= end_date)
subset_data = aggregated_data.loc[mask]
# correlation = subset_data['mean_private_tx_percentage'].corr(subset_data['mean_mev_tx_percentage'])
correlation, p_value = pearsonr(subset_data['mean_private_tx_percentage'], subset_data['mean_mev_tx_percentage'])

ax1.annotate(f'Pearson Corr: {correlation:.2f}', xy=(0.5, 0.9), xycoords='axes fraction', color='black')
ax1.annotate(f'p-value: {p_value:.3f}', xy=(0.5, 0.85), xycoords='axes fraction', color='black')

# Apply the x-tick rotation here
labels = ax1.get_xticklabels()
ax1.set_xticklabels(labels, rotation=45, ha='center')

fig.tight_layout()
plt.grid(True)
fig.legend(loc="upper left", bbox_to_anchor=(0,1), bbox_transform=ax1.transAxes)
plt.savefig('figures/correlation_person_full.png')
# plt.show()
