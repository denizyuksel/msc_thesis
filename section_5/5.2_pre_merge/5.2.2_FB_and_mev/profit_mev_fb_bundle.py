import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'fb_bundle_count' : 'sum',
        'mev_tx_count': 'sum',
        'total_extractor_profit': 'sum',
    }).reset_index()
    data_by_date['fb_bundle_count'] = data_by_date['fb_bundle_count'].rolling(window=14).mean()
    data_by_date['mev_tx_count'] = data_by_date['mev_tx_count'].rolling(window=14).mean()
    data_by_date['total_extractor_profit'] = data_by_date['total_extractor_profit'].rolling(window=14).mean()
    return data_by_date

def plot_data(data, filepath):
    fig, ax1 = plt.subplots()

    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count')
    ax1.plot(data['block_date'], data['fb_bundle_count'], color='blue', label='Flashbots Bundles per Block')
    ax1.plot(data['block_date'], data['mev_tx_count'], color='darkorange', label='MEV Transactions')
    
    ax2 = ax1.twinx()  # Instantiate a second axes that shares the same x-axis
    color = 'limegreen'
    ax2.set_ylabel('Profit (USD)', color=color)
    ax2.plot(data['block_date'], data['total_extractor_profit'], color=color, label='Total Extractor Profit')
    
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    plt.title('Flashbots Bundles and MEV Transactions During Pre-Merge Period')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    ax1.set_ylim(0)
    ax2.set_ylim(0)

    significant_dates = {
        '2021-10-06': ('darkred', '--', 'Flashbots Protect Launch Date (Oct 2021)'),
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
    
    leg = plt.legend(handles, labels, loc='upper left', frameon=True)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    # Move the rotation setting to the very end after all plotting
    plt.setp(ax1.get_xticklabels(), rotation=45)
    plt.savefig(filepath)

def calculate_correlation(data):
    # Calculate Pearson and Spearman correlation coefficients on the raw data
    pearson_corr, pearson_p_value = pearsonr(data.dropna()['fb_bundle_count'], data.dropna()['mev_tx_count'])
    spearman_corr, spearman_p_value = spearmanr(data.dropna()['fb_bundle_count'], data.dropna()['mev_tx_count'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.2.2_profit_mev_tx_fb.png')
    pearson, pearson_p, spearman, spearman_p = calculate_correlation(data_by_date)
    print(f"Pearson correlation and p value: {pearson}, {pearson_p}")
    print(f"Spearman correlation and p value: {spearman}, {spearman_p}")

if __name__ == "__main__":
    main()