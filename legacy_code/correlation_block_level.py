import pandas as pd
from sqlalchemy import create_engine
from scipy.stats import pearsonr, spearmanr
import matplotlib.pyplot as plt

# Database connection setup
db_params = {
    'host': 'localhost',
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

table_name = 'blocknative_zeromev'
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
start_block = 14212213
end_block = 17387597

# SQL query to retrieve transactions data for distinct block numbers within a specified range
query = f"""
SELECT block_number, tx_count, private_tx_count, arb_count, frontrun_count, sandwich_count, backrun_count, liquid_count
FROM {table_name}
WHERE block_number BETWEEN {start_block} AND {end_block}
ORDER BY block_number ASC;
"""
data = pd.read_sql(query, engine)

# Calculate total MEV transactions across specific types
data['mev_tx_count'] = data[['arb_count', 'frontrun_count', 'sandwich_count', 'backrun_count', 'liquid_count']].sum(axis=1)

# Drop rows where calculations could not be made (e.g., tx_count is zero)
data = data[(data['private_tx_count'] > 0) & (data['mev_tx_count'] > 0)]

# Calculate Pearson correlation coefficient
correlation, p_value = spearmanr(data['private_tx_count'], data['mev_tx_count'])

# Output the correlation results
print(f'Spearman correlation coefficient: {correlation:.2f}')
print(f'p-value: {p_value:.3f}')

# Plotting
fig, ax1 = plt.subplots(figsize=(8, 4))
color = 'tab:blue'
ax1.set_xlabel('Block Number')
ax1.set_ylabel('Private Transaction Count', color=color)
ax1.plot(data['block_number'], data['private_tx_count'], color=color)
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
color = 'tab:red'
ax2.set_ylabel('MEV Transaction Count', color=color)  # we already handled the x-label with ax1
ax2.plot(data['block_number'], data['mev_tx_count'], color=color)
ax2.tick_params(axis='y', labelcolor=color)

# Annotating the plot with correlation information
plt.annotate(f'Pearson r: {correlation:.2f}\np-value: {p_value:.3f}', xy=(0.05, 0.95), xycoords='axes fraction', 
    verticalalignment='top', horizontalalignment='left', fontsize=12, color='black')

fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.title('Private and MEV Transaction Counts  vs Block Number (from Mid-Feb 2022 to June 2023)')
plt.grid(True)

# Save the figure to the filesystem
plt.savefig('figures/correlation_block_level_spearman.png')

plt.show()
