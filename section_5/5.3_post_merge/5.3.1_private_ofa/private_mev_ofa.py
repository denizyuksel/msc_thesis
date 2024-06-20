import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[data['block_date'] >= pd.Timestamp('2022-09-15')]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'private_tx_count' : 'sum',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14).mean()
    return data_by_date

def plot_data_double_axis(data, mev_share_data, mev_blocker_data, filepath):
    # smoothen mev_share_data:
    mev_share_data['mev_share_bundle_count'] = mev_share_data['mev_share_bundle_count'].rolling(window=14).mean()
    
    # smoothen mev_blocker_data:
    mev_blocker_data['mined'] = mev_blocker_data['mined'].rolling(window=14).mean()
    
    fig, ax1 = plt.subplots()

    color = 'navy'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transaction Count', color=color)
    ax1.plot(data['block_date'], data['private_tx_count'], color=color, label='Private Transactions')
    ax1.tick_params(axis='y', labelcolor=color)
    
    color = 'orange'
    ax1.plot(mev_blocker_data['block_date'], mev_blocker_data['mined'], color=color, label='MEV Blocker Transactions')
    
    ax2 = ax1.twinx()  # Instantiate a second axes that shares the same x-axis
    color = 'limegreen'
    ax2.set_ylabel('MEV-Share Bundle Count', color=color)
    ax2.plot(mev_share_data['block_date'], mev_share_data['mev_share_bundle_count'], linestyle='-', color=color, label='MEV-Share Bundles')
    ax2.tick_params(axis='y', labelcolor='green')
    ax2.tick_params(axis='x', colors='green')

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    plt.title('Private Transactions and MEV-Share Bundles')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    ax1.set_ylim(0)
    ax2.set_ylim(0)

    significant_dates = {
        '2022-11-11': ('deepskyblue', ':', 'FTX Collapse'),
        '2023-03-11': ('fuchsia', '--', 'USDC Depeg'),
        '2023-04-01': ('limegreen', '-.', 'MEV Share Launch'),
        '2023-04-05': ('lawngreen', '-.', 'MEV Blocker Launch'),
    }
    for date, (color, linestyle, label) in significant_dates.items():
        ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

    fig.tight_layout()  # Adjust layout to make room for the legend

    # Collect all the legend handles and labels
    handles, labels = [], []
    for ax in [ax1, ax2]:
        for handle, label in zip(*ax.get_legend_handles_labels()):
            handles.append(handle)
            labels.append(label)
    ax1.legend(handles, labels, loc='upper left')

    plt.grid(True)
    # Move the rotation setting to the very end after all plotting
    plt.setp(ax1.get_xticklabels(), rotation=45)
    plt.savefig(filepath)

def plot_data_single_axis(data_by_date, mev_share_data, filepath):

    # smoothen mev_share_data:
    # mev_share_data['mev_share_bundle_count'] = mev_share_data['mev_share_bundle_count'].rolling(window=14).mean()

    plt.plot(data_by_date['block_date'], data_by_date['private_tx_count'], linestyle='-', color='navy', label='Private Transactions')
    plt.plot(mev_share_data['block_date'], mev_share_data['mev_share_bundle_count'], linestyle='-', color='limegreen', label='MEV-Share Transactions')

    plt.title('Private Transactions and MEV-Share Bundles')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.xlim(left=data_by_date['block_date'].min(), right=data_by_date['block_date'].max())
    plt.ylim(0)

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    significant_dates = {
        '2022-11-11': ('deepskyblue', ':', 'FTX Collapse'),
        '2023-03-11': ('fuchsia', '--', 'USDC Depeg'),
        '2023-04-01': ('limegreen', '-.', 'MEV Share Launch'),
        '2023-04-05': ('lawngreen', '-.', 'MEV Blocker Launch'),
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
    mev_share_data = load_and_prepare_data('../../../mevshare_by_date.csv')
    mev_blocker_data = load_and_prepare_data('../../../mevblocker_deniz_18_distinct.csv')
    data_by_date = aggregate_data(data)
    plot_data_double_axis(data_by_date, mev_share_data, mev_blocker_data, '5.3.1_private_ofa.png')

if __name__ == "__main__":
    main()