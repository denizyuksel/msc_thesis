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
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14).mean()
    return data_by_date

def plot_data_double_axis(data, mev_share_data, mev_blocker_data, filepath):
    # Smoothing data
    mev_share_data['mev_share_bundle_count'] = mev_share_data['mev_share_bundle_count'].rolling(window=14).mean()
    mev_blocker_data['mined'] = mev_blocker_data['mined'].rolling(window=14).mean()

    fig, ax1 = plt.subplots()

    # First axis plots
    color = 'navy'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transaction Count', color="black")
    line1, = ax1.plot(data['block_date'], data['private_tx_count'], color=color, label='Private Transactions')
    ax1.tick_params(axis='y', labelcolor="black")

    color = 'darkorange'
    line2, = ax1.plot(mev_blocker_data['block_date'], mev_blocker_data['mined'], color=color, label='MEV Blocker Transactions')

    # Second axis plots
    ax2 = ax1.twinx()  
    color = 'limegreen'
    ax2.set_ylabel('MEV-Share Bundle Count', color=color)
    line3, = ax2.plot(mev_share_data['block_date'], mev_share_data['mev_share_bundle_count'], linestyle='-', color=color, label='MEV-Share Bundles')
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
        '2023-04-01': ('limegreen', '-.', 'MEV Share Launch'),
        '2023-04-05': ('orange', '-.', 'MEV Blocker Launch'),
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

def calculate_correlation(data, mevblocker, mevshare):
    # Ensure that all dataframes are indexed by 'timestamp' if not already set
    if 'timestamp' in data.columns:
        data.set_index('timestamp', inplace=True)
    if 'timestamp' in mevblocker.columns:
        mevblocker.set_index('timestamp', inplace=True)
    if 'timestamp' in mevshare.columns:
        mevshare.set_index('timestamp', inplace=True)

    # Merging the data on the index (timestamp)
    merged_data = pd.concat([data, mevblocker, mevshare], axis=1, join='outer')

    # Optionally handle missing values by dropping or filling them
    merged_data.dropna(inplace=True)  # Drops any rows with NaN values, which ensures valid correlation computation

    # Calculate Pearson correlation for 'private_tx_count' and 'mined'
    pearson_corr = merged_data[['private_tx_count', 'mined']].corr(method='pearson')

    # Calculate Spearman correlation for 'private_tx_count' and 'mev_share_bundle_count'
    spearman_corr = merged_data[['private_tx_count', 'mev_share_bundle_count']].corr(method='spearman')

    return pearson_corr, spearman_corr

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    mev_share_data = load_and_prepare_data('../../../mevshare_by_date.csv')
    mev_blocker_data = load_and_prepare_data('../../../mevblocker_deniz_18_distinct.csv')
    data_by_date = aggregate_data(data)
    
    plot_data_double_axis(data_by_date, mev_share_data, mev_blocker_data, '5.3.1_private_ofa.png')
    
    pearson, spearman = calculate_correlation(data, mev_blocker_data, mev_share_data)
    print("Pearson Correlation Coefficients:\n", pearson)
    print("Spearman Correlation Coefficients:\n", spearman)

if __name__ == "__main__":
    main()