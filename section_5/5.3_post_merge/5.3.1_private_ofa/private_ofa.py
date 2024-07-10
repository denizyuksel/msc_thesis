import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[data['block_date'] >= pd.Timestamp('2022-09-15')]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'private_tx_count' : 'sum',
        'fb_postmerge_tx_count': 'sum',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['fb_postmerge_tx_count'] = data_by_date['fb_postmerge_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    return data_by_date

def plot_data(data, mev_blocker_data, filepath):
    fig, ax1 = plt.subplots()

    mev_blocker_data['mined'] = mev_blocker_data['mined'].rolling(window=14, min_periods=7, center=True).mean()

    # Plotting only on the primary axis, ax1
    line1, = ax1.plot(data['block_date'], data['private_tx_count'], linestyle='-', color='#003f5c', label='Private Transactions') # deep blue
    line2, = ax1.plot(data['block_date'], data['fb_postmerge_tx_count'], linestyle='-', color='#9dc183', label='Flashbots Protect Transactions') # sage green
    line3, = ax1.plot(mev_blocker_data['block_date'], mev_blocker_data['mined'], color='#c2185b', label='MEV Blocker Transactions') # magenta

    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count')

    plt.title('Private Transactions and OFA Order Flow After the Merge')
    ax1.set_xlim(left=data['block_date'].min(),right=data['block_date'].max())
    ax1.set_ylim(0)

    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # Setting tick rotation and alignment to center
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")

    significant_dates = {
        '2023-04-01': ('#228B22', '-.', 'MEV-Share Launch'), # forest green
        '2023-04-05': ('#cc5500', '-.', 'MEV Blocker Launch'), # burnt orange
    }
    lines = [line1, line2, line3]
    labels = [line1.get_label(), line2.get_label(), line3.get_label()]
    for date, (color, linestyle, label) in significant_dates.items():
        line = ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)
        lines.append(line)
        labels.append(label)

    # Combine lines and labels for the legend
    leg = plt.legend(lines, labels, loc='upper left', frameon=True, ncol=1)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filepath)

def plot_data_double_axis(data, mev_blocker_data, filepath):
    # Smoothing data
    mev_blocker_data['mined'] = mev_blocker_data['mined'].rolling(window=14, min_periods=7, center=True).mean()

    fig, ax1 = plt.subplots()

    # First axis plots
    color = '#003f5c' # deep blue
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transaction Count', color="navy")
    line1, = ax1.plot(data['block_date'], data['private_tx_count'], color=color, label='Private Transactions')
    ax1.tick_params(axis='y', labelcolor="navy")

    color = '#c2185b' # magenta
    line2, = ax1.plot(mev_blocker_data['block_date'], mev_blocker_data['mined'], color=color, label='MEV Blocker Transactions')

    # Second axis plots
    ax2 = ax1.twinx()  
    color = 'limegreen'
    ax2.set_ylabel('MEV-Share Bundle Count', color=color)
    line3, = ax2.plot(data['block_date'], data['fb_postmerge_tx_count'], linestyle='-', color=color, label='Flashbots Bundles')
    ax2.tick_params(axis='y', labelcolor='green')

    # Date formatting
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.title('Private Transactions and MEV-Share Bundles')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    ax1.set_ylim(0)
    ax2.set_ylim(0)

    # Significant dates
    significant_dates = {
        '2023-04-01': ('#cc5500', '-.', 'MEV-Share Launch'), # burnt orange
        '2023-04-05': ('#228B22', '-.', 'MEV Blocker Launch'), # forest green
    }
    date_lines = []
    for date, (color, linestyle, label) in significant_dates.items():
        line = ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)
        date_lines.append((line, label))

    fig.tight_layout()  # Adjust layout to make room for the legend

    # Manually setting legend order
    handles = [line1, line2, line3] + [dl[0] for dl in date_lines]
    labels = ['Private Transactions', 'MEV Blocker Transactions', 'MEV-Share Bundles'] + [dl[1] for dl in date_lines]
    ax1.legend(handles, labels, loc='upper left')

    plt.grid(True)
    plt.setp(ax1.get_xticklabels(), rotation=45)
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    mev_blocker_data = load_and_prepare_data('../../../mevblocker_deniz_18_distinct.csv')
    data_by_date = aggregate_data(data)
    
    # plot_data_double_axis(data_by_date, mev_blocker_data, '5.3.1_private_ofa.png')
    plot_data(data_by_date, mev_blocker_data, '5.3.1_private_ofa.png')

if __name__ == "__main__":
    main()