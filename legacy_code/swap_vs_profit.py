import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import pearsonr, spearmanr

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('flashbots_blocknative.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# Exclude null values before processing
data = data.dropna(subset=['total_user_swap_volume'])
data = data.dropna(subset=['total_extractor_profit'])

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'total_user_swap_volume': 'sum',
    'total_extractor_profit': 'sum'
}).reset_index()

# Apply a 7-day rolling window to smooth the data
aggregated_data['total_user_swap_volume'] = aggregated_data['total_user_swap_volume'].rolling(window=14, center=True).mean()
aggregated_data['total_extractor_profit'] = aggregated_data['total_extractor_profit'].rolling(window=14, center=True).mean()

# Calculate Pearson and Spearman correlation coefficients on the smoothed data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['total_user_swap_volume'], aggregated_data.dropna()['total_extractor_profit'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['total_user_swap_volume'], aggregated_data.dropna()['total_extractor_profit'])

# Plotting
fig, ax = plt.subplots(figsize=(8, 4))
ax.set_yscale('log')  # Setting the y-axis to logarithmic scale

plt.title('Daily Total User Swap Volume and Extractor Profit')

ax.set_yscale('symlog', linthresh=1)  # Setting a linear threshold around zero


# Plot for smoothed total user swap volume and total extractor profit
ax.plot(aggregated_data['block_date'], aggregated_data['total_user_swap_volume'], color='cyan', label='Total User Swap Volume (USD)')
ax.plot(aggregated_data['block_date'], aggregated_data['total_extractor_profit'], color='magenta', label='Total Extractor Profit (USD)')
ax.set_xlabel('Date')
ax.set_ylabel('Amount (USD - Log Scale)')

# Significant dates as vertical lines (ensure these are defined correctly)
significant_dates = {
    '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
    # Uncomment below for full data (after merge)
    '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

# Annotate Pearson and Spearman correlation coefficients in the top right
ax.annotate(f'Pearson Corr: {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', xy=(1, 1), xycoords='axes fraction', verticalalignment='top', horizontalalignment='right', color='black')
ax.annotate(f'Spearman Corr: {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', xy=(1, 0.95), xycoords='axes fraction', verticalalignment='top', horizontalalignment='right', color='black')

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

# Apply the x-tick rotation here
labels = ax.get_xticklabels()
ax.set_xticklabels(labels, rotation=45, ha='center')

fig.tight_layout()
plt.grid(True)
ax.legend(loc="upper left", bbox_to_anchor=(0,1), bbox_transform=ax.transAxes)
plt.savefig('figures/swap_vs_profit.png')
# plt.show()
