import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from scipy.stats import pearsonr, spearmanr

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[data['block_date'] >= pd.Timestamp('2022-09-15')].copy()

    filtered_data['arb_pct'] = np.where(filtered_data['mev_tx_count'] == 0, 0, (filtered_data['arb_count'] / filtered_data['mev_tx_count']) * 100)
    filtered_data['sandwich_pct'] = np.where(filtered_data['mev_tx_count'] == 0, 0, (filtered_data['sandwich_count'] / filtered_data['mev_tx_count']) * 100)
    filtered_data['liquid_pct'] = np.where(filtered_data['mev_tx_count'] == 0, 0, (filtered_data['liquid_count'] / filtered_data['mev_tx_count']) * 100)
    return filtered_data

def aggregate_data(data):
    aggregated_data = data.groupby('block_date').agg({
    'arb_count': 'sum',
    'liquid_count': 'sum',
    'sandwich_count': 'sum',
    'mev_tx_count': 'sum',
    'mev_share_count' : 'sum',
    }).reset_index()
    # Apply a 14-day rolling average to the raw counts.
    cols_to_smooth = ['arb_count', 'liquid_count', 'sandwich_count', 'mev_tx_count', 'mev_share_count']
    for col in cols_to_smooth:
        aggregated_data[col] = aggregated_data[col].rolling(window=14, min_periods=1).mean()
    for col in ['arb_count', 'liquid_count', 'sandwich_count']:
        aggregated_data[f'{col}_pct'] = (aggregated_data[col] / aggregated_data['mev_tx_count']) * 100
    return aggregated_data

def plot_data(data, filepath):
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    bottom = np.zeros(len(data))
    
    colors = ['skyblue', 'magenta', 'orange']
    mev_labels = ['Arb', 'Liquid', 'Sandwich']
    poly_collections = []  # List to store fill_between references for the legend

    for i, col in enumerate(['arb_count_pct', 'liquid_count_pct', 'sandwich_count_pct']):
        poly = ax1.fill_between(data['block_date'], bottom, bottom + data[col], 
                                color=colors[i], label=mev_labels[i], step='mid', alpha=0.4)
        bottom += data[col]
        poly_collections.append(poly)

    line2, = ax2.plot(data['block_date'], data['mev_share_count'], linestyle='-', color='navy', label='MEV-Share Transactions')

    significant_dates = {
        '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
        '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
        '2023-04-01': ('limegreen', '-.', 'MEV Share Launch Date (Apr 2023)'),
        '2023-04-27': ('lawngreen', '-.', 'MEV Blocker Launch Date (Apr 2023)')
    }

    lines = [line2]  # Initialize with known valid plot
    labels = ['Mev Share Transactions']  # Start with known labels

    for date, (color, linestyle, label) in significant_dates.items():
        line = ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)
        lines.append(line)
        labels.append(label)

    # Adding fill_between collections and their labels to the lines and labels list
    lines.extend(poly_collections)
    labels.extend(mev_labels)

    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # Apply the tick parameters after setting up the formatter and locator
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")  
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    ax1.set_ylabel('Percentage')  # Label for left y-axis
    ax2.set_ylabel('Count', color='navy')  # Label for right y-axis with color matching the plot

    ax2.tick_params(axis='y', labelcolor='navy')  # Ensure the right y-axis tick labels match the plot color

    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())
    ax1.set_ylim(0, 100)  # Setting the left y-axis from 0 to 100
    ax2.set_ylim(0, ax2.get_ylim()[1])  # Setting the right y-axis to start at 0

    leg = plt.legend(lines, labels, loc='upper left', frameon=True)
    leg.set_zorder(100)  # Ensure legend is on top
    
    plt.title('MEV Types vs MEV-Share Transactions per Day During Post-Merge Period')
    plt.tight_layout()  # Use tight_layout to ensure the labels and legend fit well within the plot area
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.3.2_mev_types_ofa.png')

if __name__ == "__main__":
    main()