import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
from scipy.stats import pearsonr, spearmanr

register_matplotlib_converters()

# Read data from CSV file
data = pd.read_csv("flashbots_blocknative.csv")

# Convert 'block_date' to datetime format for plotting
data["block_date"] = pd.to_datetime(data["block_date"])

# Uncomment below for full data (after merge)
# data = data[data["block_date"] <= pd.Timestamp("2022-09-15")]
data = data[(data['block_date'] > pd.Timestamp('2021-03-01')) & (data['block_date'] <= pd.Timestamp('2022-07-01'))]

# Aggregate data by date
aggregated_data = (
    data.groupby("block_date")
    .agg(
        {
            "arb_count": "sum",
            "liquid_count": "sum",
            "sandwich_count": "sum",
            "bundle_tx_count": "sum",
        }
    )
    .reset_index()
)

# Calculate the total MEV transactions
aggregated_data["mev_tx_count"] = aggregated_data[
    ["liquid_count", "arb_count", "sandwich_count"]
].sum(axis=1)

# Calculate percentages for each transaction type relative to MEV transactions
transaction_types = ["liquid_count", "arb_count", "sandwich_count"]
for tx_type in transaction_types:
    aggregated_data[f"{tx_type}_pct"] = (
        aggregated_data[tx_type] / aggregated_data["mev_tx_count"]
    ) * 100

# Apply a rolling average to the percentage data
for tx_type in transaction_types:
    pct_col = f"{tx_type}_pct"
    aggregated_data[pct_col] = (
        aggregated_data[pct_col].rolling(window=14, min_periods=1).mean()
    )

# Rolling average for bundle_tx_count
aggregated_data["bundle_tx_count_smooth"] = (
    aggregated_data["bundle_tx_count"].rolling(window=14, min_periods=1).mean()
)

# Plotting
plt.figure(figsize=(8, 4))

colors = ["magenta", "skyblue", "orange"]
labels = ["Liquid", "Arb", "Sandwich"]

# Start from 0 bottom, incrementally add the previous percentage
bottom = 0
for i, col in enumerate(transaction_types):
    pct_col = f"{col}_pct"
    plt.fill_between(
        aggregated_data["block_date"],
        bottom,
        bottom + aggregated_data[pct_col],
        color=colors[i],
        alpha=0.4,
        label=labels[i],
    )
    bottom += aggregated_data[pct_col]

plt.title("Percentage of Each MEV Transaction Type Over Time")
plt.xlabel("Date")
plt.ylabel("Percentage of Total MEV Transactions per Day")
plt.ylim(0, 100)  # Ensuring y-axis is capped at 100%
plt.xlim(left=aggregated_data["block_date"].min())

# Calculate Pearson and Spearman correlation coefficients on the smoothed data
pearson_corr, pearson_p_value = pearsonr(
    aggregated_data.dropna()["mev_tx_count"],
    aggregated_data.dropna()["bundle_tx_count"],
)
spearman_corr, spearman_p_value = spearmanr(
    aggregated_data.dropna()["mev_tx_count"],
    aggregated_data.dropna()["bundle_tx_count"],
)

ax = plt.gca()
ax.xaxis.set_major_locator(
    mdates.MonthLocator(interval=4)
)  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

# Create a second y-axis for the new column
ax2 = ax.twinx()
ax2.plot(
    aggregated_data["block_date"],
    aggregated_data["bundle_tx_count_smooth"],
    color="blue",
    label="Flashbots Bundle Transactions",
    linestyle="-",
)  # Make line solid
ax2.set_ylabel("Flashbots Bundle Count", color="blue")
ax2.tick_params(axis="y", colors="blue")

# Significant dates as vertical lines
significant_dates = {
    "2021-10-06": ("darkred", "--", "Flashbots Protect Launch Date (Oct 2021)"),
    # Uncomment below for full data (after merge)
    # '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
    # '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
    # '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
    # '2023-04-27': ('indigo', '-.', 'MEV Blocker Launch Date (Apr 2023)')
}
for date, (color, linestyle, label) in significant_dates.items():
    ax.axvline(
        pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label
    )

# Adjust plot layout to make space at the bottom for annotations
plt.subplots_adjust(bottom=0.2)

# Annotate Pearson and Spearman correlation coefficients below the plot
plt.figtext(0.1, 0.05, f'Pearson Corr (private vs bundle): {pearson_corr:.2f}, p-value: {pearson_p_value:.3f}', horizontalalignment='left', color='black', fontsize='x-small')
plt.figtext(0.1, 0.02, f'Spearman Corr (private vs bundle): {spearman_corr:.2f}, p-value: {spearman_p_value:.3f}', horizontalalignment='left', color='black', fontsize='x-small')

# Combine legends from both axes and improve legend placement
lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax_legend = ax.legend(
    lines + lines2, labels + labels2, 
    loc='upper right', fontsize='x-small', 
    framealpha=1, facecolor='white', edgecolor='black',
    bbox_to_anchor=(1, 1)  # Adjusts the x and y coordinates relative to the figure
)

# Setting a high zorder to ensure the legend is above the grid
ax_legend.set_zorder(100)

# plt.subplots_adjust(right=0.85)  # Adjust this value to increase/decrease the plot width

plt.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=0)
plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig("figures/pct_mev_types_vs_flashbots.png")
# plt.show()