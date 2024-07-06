import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
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
    aggregated_data["arb_count"] = aggregated_data["arb_count"].rolling(window=14, min_periods=7, center=True).mean()
    # aggregated_data["liquid_count"] = aggregated_data["liquid_count"].rolling(window=3, min_periods=1).median()
    aggregated_data["sandwich_count"] = aggregated_data["sandwich_count"].rolling(window=14, min_periods=7, center=True).mean()

    return aggregated_data

def plot_data(data, filepath):
    plt.title('MEV Activity With Categories Over Time')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.xlim(left=data['block_date'].min(), right=data['block_date'].max())
    # plt.ylim(0)

    plt.plot(data['block_date'], data['arb_count'], color='#ffc107', label='Arbitrage') # amber
    plt.plot(data['block_date'], data['sandwich_count'], linestyle='-', color='#00FFFF', label='Sandwich') # cyan
    plt.plot(data['block_date'], data['liquid_count'], color='#800080', label='Liquidation') # deep purple

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    significant_dates = {
        '2021-10-06': ('midnightblue', '--', 'Flashbots Protect Launch'),
        '2022-09-15': ('goldenrod', '-.', 'The Merge'),
        '2022-11-11': ('steelblue', ':', 'FTX Collapse'),
        '2023-03-11': ('sienna', '--', 'USDC Depeg'),
        '2023-04-05': ('olive', '-.', 'OFAs Launch'),
    }
    for date, (color, linestyle, label) in significant_dates.items():
        ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(loc='upper left', ncol=2)
    plt.tight_layout()
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.1.2_mev_types.png')

if __name__ == "__main__":
    main()