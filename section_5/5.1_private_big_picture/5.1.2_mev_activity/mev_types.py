import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    # filtered_data = data[(data['block_date'] >= pd.Timestamp('2020-12-01')) & 
    #                      (data['block_date'] <= pd.Timestamp('2024-04-01'))]

    data['arb_pct'] = np.where(data['mev_tx_count'] == 0, 0, (data['arb_count'] / data['mev_tx_count']) * 100)
    data['sandwich_pct'] = np.where(data['mev_tx_count'] == 0, 0, (data['sandwich_count'] / data['mev_tx_count']) * 100)
    data['liquid_pct'] = np.where(data['mev_tx_count'] == 0, 0, (data['liquid_count'] / data['mev_tx_count']) * 100)
    return data

def aggregate_data(data):
    aggregated_data = data.groupby('block_date').agg({
    'arb_count': 'sum',
    'liquid_count': 'sum',
    'sandwich_count': 'sum',
    'mev_tx_count': 'sum'
    }).reset_index()
    # Apply a 14-day rolling average to the raw counts.
    cols_to_smooth = ['arb_count', 'liquid_count', 'sandwich_count', 'mev_tx_count']
    for col in cols_to_smooth:
        aggregated_data[col] = aggregated_data[col].rolling(window=14, min_periods=1).mean()
    for col in ['arb_count', 'liquid_count', 'sandwich_count']:
        aggregated_data[f'{col}_pct'] = (aggregated_data[col] / aggregated_data['mev_tx_count']) * 100
    return aggregated_data

def plot_data(data, filepath):
    colors = ['skyblue', 'magenta', 'orange']
    labels = ['Arb', 'Liquid', 'Sandwich']

    # Start from 0 bottom, incrementally add the previous smoothed count
    bottom = np.zeros(len(data))
    for i, col in enumerate(['arb_count_pct', 'liquid_count_pct', 'sandwich_count_pct']):
        plt.fill_between(data['block_date'], bottom, bottom + data[col], color=colors[i], label=labels[i], step='mid', alpha=0.4)
        bottom += data[col]
    # plt.plot(data['block_date'], data['private_tx_pct'], linestyle='-', color='navy', label='Blocknative Private Transactions')

    plt.title('MEV Transactions Percentages Over Time')
    plt.xlabel('Date')
    plt.ylabel('Percentage')
    plt.xlim(left=data['block_date'].min())
    plt.ylim(0, 100)

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    significant_dates = {
        '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
        '2022-09-15': ('red', '-.', 'The Merge (Sept 2022)'),
        '2022-11-11': ('deepskyblue', ':', 'FTX Collapse Date (Nov 2022)'),
        '2023-03-11': ('fuchsia', '--', 'USDC Depeg Date (Mar 2023)'),
        '2023-04-27': ('lime', '-.', 'MEV Blocker Launch Date (Apr 2023)')
    }
    for date, (color, linestyle, label) in significant_dates.items():
        ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5_mev_types.png')

if __name__ == "__main__":
    main()