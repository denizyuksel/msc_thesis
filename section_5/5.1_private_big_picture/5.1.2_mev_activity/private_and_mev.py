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
        'private_tx_pct' : 'mean',
        'mev_tx_pct' : 'mean',
        'mev_tx_pct_swaps' : 'mean',
    }).reset_index()
    data_by_date['private_tx_pct'] = data_by_date['private_tx_pct'].rolling(window=14).mean()
    data_by_date['mev_tx_pct'] = data_by_date['mev_tx_pct'].rolling(window=14).mean()
    data_by_date['mev_tx_pct_swaps'] = data_by_date['mev_tx_pct_swaps'].rolling(window=14).mean()
    return data_by_date

def plot_data(data_by_date, filepath):
    plt.plot(data_by_date['block_date'], data_by_date['private_tx_pct'], linestyle='-', color='navy', label='Blocknative Private Transactions')
    plt.plot(data_by_date['block_date'], data_by_date['mev_tx_pct'], linestyle='-', color='orchid', label='Mev Count by Zeromev / Total Count by Blocknative')
    plt.plot(data_by_date['block_date'], data_by_date['mev_tx_pct_swaps'], linestyle='-', color='darkorange', label='Mev Count by Zeromev / Swap Count by Zeromev')


    plt.title('Private and MEV Transaction Percentages Over Time')
    plt.xlabel('Date')
    plt.ylabel('Percentage')
    plt.xlim(left=data_by_date['block_date'].min())
    plt.ylim(0)

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
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5_private_vs_mev_overview.png')

if __name__ == "__main__":
    main()