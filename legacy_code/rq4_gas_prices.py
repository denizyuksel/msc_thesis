import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
import numpy as np
from scipy.stats import pearsonr, spearmanr

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv('blocknative_zeromev_flashbots_rq4.csv')

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

data['private_tx_pct'] = np.where(data['tx_count'] > 0, (data['private_tx_count'] / data['tx_count']) * 100, 0)

# Ensure the 'private_gasused_gwei' and 'public_gasused_gwei' are in numeric format
data['private_gasused_gwei'] = pd.to_numeric(data['private_gasused_gwei'], errors='coerce')
data['public_gasused_gwei'] = pd.to_numeric(data['public_gasused_gwei'], errors='coerce')

# Calculate 'private_gasused_pct'
data['private_gasused_pct'] = (data['private_gasused_gwei'] / (data['private_gasused_gwei'] + data['public_gasused_gwei'])) * 100
# It's a good practice to check for any NaN values and decide how to handle them
data['private_gasused_pct'] = data['private_gasused_pct'].fillna(0)  # Replace NaN with 0 if that makes sense for your analysis

# Drop 0 pct rows
# data = data[data['private_gasused_pct'] != 0]

# Aggregate data by date
aggregated_data = data.groupby('block_date').agg({
    'private_gasused_gwei': 'sum',
    'public_gasused_gwei': 'sum',

    'private_gasused_pct': 'mean',
    'private_mean_gasused': 'mean',
    'private_median_gasused': 'median',
    'public_mean_gasused': 'mean',
    'public_median_gasused': 'median',

    'mean_gasprice_gwei': 'mean',
    'median_gasprice_gwei': 'median',

    'private_tx_count': 'sum',
    'private_tx_pct': 'mean',
    'public_tx_count': 'sum',
    'timepending_block_total': 'mean',
    'mean_timepending': 'mean',
    'median_timepending': 'mean',

}).reset_index()

# Drop some rows
aggregated_data = aggregated_data[aggregated_data['private_gasused_pct'] != 0]

# Apply 14-day rolling average
aggregated_data['private_gasused_pct'] = aggregated_data['private_gasused_pct'].rolling(window=14).mean()
aggregated_data['median_gasprice_gwei'] = aggregated_data['median_gasprice_gwei'].rolling(window=14).mean()
aggregated_data['mean_gasprice_gwei'] = aggregated_data['mean_gasprice_gwei'].rolling(window=14).mean()

# Calculate Pearson and Spearman correlation coefficients on the raw data
pearson_corr, pearson_p_value = pearsonr(aggregated_data.dropna()['private_gasused_pct'], aggregated_data.dropna()['mean_gasprice_gwei'])
spearman_corr, spearman_p_value = spearmanr(aggregated_data.dropna()['private_gasused_pct'], aggregated_data.dropna()['mean_gasprice_gwei'])

# Drop data where gasused field is not defined
# aggregated_data = aggregated_data.dropna(subset=['private_tx_pct'])

# Plotting
fig, ax1 = plt.subplots(figsize=(12, 6))

# Plot private median gas used
ax1.set_xlabel('Date')
ax1.set_ylabel('Percentage', color='blue')
ax1.plot(aggregated_data['block_date'], aggregated_data['private_gasused_pct'], color='blue', label='Gas Used by Private Transactions')
ax1.tick_params(axis='y', labelcolor='blue')

# Create a second y-axis to plot the mean time pending
ax2 = ax1.twinx()
ax2.set_ylabel('Gwei', color='green')
ax2.plot(aggregated_data['block_date'], aggregated_data['median_gasprice_gwei'], color='green', linestyle='--', label='Mean Gas Price')
ax2.tick_params(axis='y', labelcolor='green')
ax2.set_yscale('log')

# Format the x-axis to better display dates
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
plt.xticks(rotation=45, ha='center')
fig.autofmt_xdate()

plt.title('Relationship Between Private Transaction Dominance and Mean Gas Price Over Time')
plt.grid(True)

# Combine legends and place it in the upper left
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
all_lines = lines_1 + lines_2
all_labels = labels_1 + labels_2

# Annotate Pearson and Spearman correlation coefficients below the plot
plt.figtext(0.1, 0.05, f'Pearson Corr (private gasused pct vs gas price): {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', horizontalalignment='left', color='black', fontsize='x-small')
plt.figtext(0.1, 0.02, f'Spearman Corr (private gasused pct vs gas price): {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', horizontalalignment='left', color='black', fontsize='x-small')


legend = ax1.legend(all_lines, all_labels, loc='upper left', bbox_to_anchor=(0.02, 1), ncol=1)
plt.setp(legend.get_texts(), fontsize='10')  # Set legend font size

plt.savefig('figures/rq4_gas_price_mean.png', bbox_inches='tight')
plt.show()

