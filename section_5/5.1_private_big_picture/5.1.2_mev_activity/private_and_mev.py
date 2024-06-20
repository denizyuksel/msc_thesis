import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    # filtered_data = data[(data['block_date'] >= pd.Timestamp('2020-12-01')) & 
    #                      (data['block_date'] <= pd.Timestamp('2024-04-01'))]
    return data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'private_tx_count' : 'sum',
        'mev_tx_count' : 'sum',
        'swap_count': 'sum',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14).mean()
    data_by_date['mev_tx_count'] = data_by_date['mev_tx_count'].rolling(window=14).mean()
    data_by_date['swap_count'] = data_by_date['swap_count'].rolling(window=14).mean()
    return data_by_date

def plot_data(data_by_date, filepath):
    plt.plot(data_by_date['block_date'], data_by_date['private_tx_count'], linestyle='-', color='navy', label='Private Transactions')
    plt.plot(data_by_date['block_date'], data_by_date['mev_tx_count'], linestyle='-', color='darkorange', label='MEV Count')
    plt.plot(data_by_date['block_date'], data_by_date['swap_count'], linestyle='-', color='orchid', label='Swap Count')

    plt.title('MEV Transactions And Swaps')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.xlim(left=data_by_date['block_date'].min(), right=data_by_date['block_date'].max())

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # Set y-axis to logarithmic scale
    ax.set_yscale('log')

    significant_dates = {
        '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch'),
        '2022-09-15': ('red', '-.', 'The Merge'),
        '2022-11-11': ('deepskyblue', ':', 'FTX Collapse'),
        '2023-03-11': ('fuchsia', '--', 'USDC Depeg'),
        '2023-04-27': ('lime', '-.', 'MEV Blocker Launch')
    }
    for date, (color, linestyle, label) in significant_dates.items():
        ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(loc='lower left')
    plt.tight_layout()
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.1.2_mev_and_swap.png')

if __name__ == "__main__":
    main()