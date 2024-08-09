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
        'public_tx_count' : 'sum',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['public_tx_count'] = data_by_date['public_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['fb_postmerge_tx_count'] = data_by_date['fb_postmerge_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    return data_by_date

def plot_data(data, mev_blocker_data, filepath):
    fig, ax1 = plt.subplots()

    mev_blocker_data['mined'] = mev_blocker_data['mined'].rolling(window=14, min_periods=7, center=True).mean()

    # Plotting only on the primary axis, ax1
    line1, = ax1.plot(data['block_date'], data['private_tx_count'], linestyle='-', color='#003f5c', label='Private Transactions') # deep blue
    line2, = ax1.plot(data['block_date'], data['fb_postmerge_tx_count'], linestyle='-', color='#9dc183', label='Flashbots Protect Transactions') # sage green
    line3, = ax1.plot(mev_blocker_data['block_date'], mev_blocker_data['mined'], color='#c2185b', label='MEVBlocker Transactions') # magenta

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
        '2022-11-11': ('steelblue', ':', 'FTX Collapse'),
        '2023-03-11': ('sienna', '--', 'USDC Depeg'),
        '2023-04-01': ('rebeccapurple', '-.', 'MEV-Share Launch'),
        '2023-04-27': ('#770737', '-.', 'MEVBlocker Launch'), #mulberry
    }
    lines = [line1, line2, line3]
    labels = [line1.get_label(), line2.get_label(), line3.get_label()]
    for date, (color, linestyle, label) in significant_dates.items():
        line = ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)
        lines.append(line)
        labels.append(label)

    # Combine lines and labels for the legend
    leg = plt.legend(lines, labels, loc='upper left', frameon=True, ncol=2)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filepath)

def calculate_correlation_private_MEVBlocker(data, mev_blocker):
    # data_filtered = data[(data['block_date'] >= pd.Timestamp('2023-05-01')) & (data['block_date'] <= pd.Timestamp('2024-03-23'))]
    # mev_blocker_filtered = mev_blocker[(mev_blocker['block_date'] >= pd.Timestamp('2023-07-01')) & (mev_blocker['block_date'] <= pd.Timestamp('2024-03-23'))]

    merged_data = pd.merge(data, mev_blocker, on='block_date')

    merged_data['mined'] = merged_data['mined'].rolling(window=14, min_periods=7, center=True).mean()

    pearson_corr, pearson_p_value = pearsonr(merged_data.dropna()['private_tx_count'], merged_data.dropna()['mined'])
    spearman_corr, spearman_p_value = spearmanr(merged_data.dropna()['private_tx_count'], merged_data.dropna()['mined'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def calculate_correlation_FB_MEVBlocker(data, mev_blocker):
    merged_data = pd.merge(data, mev_blocker, on='block_date')
    merged_data = merged_data[(merged_data['block_date'] >= pd.Timestamp('2023-07-01')) & (merged_data['block_date'] <= pd.Timestamp('2024-03-23'))]

    merged_data['mined'] = merged_data['mined'].rolling(window=14, min_periods=7, center=True).mean()

    pearson_corr, pearson_p_value = pearsonr(merged_data.dropna()['fb_postmerge_tx_count'], merged_data.dropna()['mined'])
    spearman_corr, spearman_p_value = spearmanr(merged_data.dropna()['fb_postmerge_tx_count'], merged_data.dropna()['mined'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    mev_blocker_data = load_and_prepare_data('../../../mevblocker_deniz_18_distinct.csv')
    data_by_date = aggregate_data(data)

    pearson, pearson_p, spearman, spearman_p = calculate_correlation_private_MEVBlocker(data_by_date, mev_blocker_data)
    print(f"Private and MEVBlocker Pearson correlation and p value: {pearson}, {pearson_p}")
    print(f"Private and MEVBlocker Spearman correlation and p value: {spearman}, {spearman_p}")

    pearson, pearson_p, spearman, spearman_p = calculate_correlation_FB_MEVBlocker(data_by_date, mev_blocker_data)
    print(f"Flashbots Protect and MEVBlocker Pearson correlation and p value: {pearson}, {pearson_p}")
    print(f"Flashbots Protect and MEVBlocker Spearman correlation and p value: {spearman}, {spearman_p}")
    
    
    plot_data(data_by_date, mev_blocker_data, '5.3.1_private_ofa.png')

if __name__ == "__main__":
    main()