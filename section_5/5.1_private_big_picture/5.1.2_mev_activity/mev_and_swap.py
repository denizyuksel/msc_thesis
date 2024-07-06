import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr
from matplotlib.ticker import FuncFormatter

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
        'total_extractor_profit': 'sum',
        'arb_extractor_profit': 'sum',
        'sandwich_extractor_profit': 'sum',
        'liquid_extractor_profit': 'sum',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['mev_tx_count'] = data_by_date['mev_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['swap_count'] = data_by_date['swap_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['total_extractor_profit'] = data_by_date['total_extractor_profit'].rolling(window=14, min_periods=7, center=True).mean()

    # Summing specific profits to create a new column
    data_by_date['total_extractor_profit_without_swaps'] = (
        data_by_date['arb_extractor_profit'] + 
        data_by_date['sandwich_extractor_profit'] + 
        data_by_date['liquid_extractor_profit']
    )
    data_by_date['total_extractor_profit_without_swaps'] = data_by_date['total_extractor_profit_without_swaps'].rolling(window=14, min_periods=7, center=True).mean()

    return data_by_date

# Function to format the y-axis labels
def millions_formatter(x, pos):
    return f'${int(x / 1_000_000)}M'

def plot_data_double_axis(data, filepath):
    fig, ax1 = plt.subplots()

    data['cumulative_profit'] = data['total_extractor_profit_without_swaps'].cumsum()

    # Plotting counts on the left y-axis
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count')
    ax1.plot(data['block_date'], data['private_tx_count'], color='#003f5c', label='Private Transactions') # deep blue
    ax1.plot(data['block_date'], data['mev_tx_count'], color='#f08080', label='MEV Activity') # light coral
    ax1.plot(data['block_date'], data['swap_count'], linestyle='-', color='darkcyan', label='Swap Count')

    ax1.tick_params(axis='y')
    ax1.set_yscale('log')

    # Setting up the right y-axis for cumulative profit
    ax2 = ax1.twinx()
    ax2.set_ylabel('Total Profit (USD)', color='#DC143C')
    ax2.plot(data['block_date'], data['cumulative_profit'], linestyle='--', color='#DC143C', label='Cumulative Extractor Profit') # crimson
    ax2.tick_params(axis='y', labelcolor='#DC143C')

    # Format the y-axis with millions
    def millions_formatter(x, pos):
        return f'${int(x / 1_000_000)}M'
    ax2.yaxis.set_major_formatter(FuncFormatter(millions_formatter))

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    plt.title('MEV Activity, Swaps and Extractor Profit')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    ax1.set_ylim(1)
    ax2.set_ylim(0)

    significant_dates = {
        '2021-10-06': ('midnightblue', '--', 'Flashbots Protect Launch'),
        '2022-09-15': ('goldenrod', '-.', 'The Merge'),
        '2022-11-11': ('steelblue', ':', 'FTX Collapse'),
        '2023-03-11': ('sienna', '--', 'USDC Depeg'),
        '2023-04-05': ('olive', '-.', 'OFAs Launch'),
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
    
    # Display a single combined legend
    leg = plt.legend(handles, labels, loc='lower left', frameon=True, ncol = 2)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.savefig(filepath)

def plot_data(data_by_date, filepath):
    plt.plot(data_by_date['block_date'], data_by_date['private_tx_count'], linestyle='-', color='navy', label='Private Transactions')
    plt.plot(data_by_date['block_date'], data_by_date['mev_tx_count'], linestyle='-', color='darkorange', label='MEV Count')
    plt.plot(data_by_date['block_date'], data_by_date['swap_count'], linestyle='-', color='orchid', label='Swap Count')

    plt.title('Private, MEV, and Swap Transactions')
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

def calculate_correlation(data):
    # Calculate Pearson and Spearman correlation coefficients on the raw data
    data_portion = data[(data['block_date'] >= pd.Timestamp('2021-10-06')) & (data['block_date'] <= pd.Timestamp('2023-04-27'))]
    # data_portion = data[data['block_date'] >= pd.Timestamp('2023-04-27')]
    pearson_corr, pearson_p_value = pearsonr(data_portion.dropna()['private_tx_count'], data_portion.dropna()['mev_tx_count'])
    spearman_corr, spearman_p_value = spearmanr(data_portion.dropna()['private_tx_count'], data_portion.dropna()['mev_tx_count'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data_double_axis(data_by_date, '5.1.2_mev_and_swap.png')
    pearson, pearson_p, spearman, spearman_p = calculate_correlation(data_by_date)

    print(f"Pearson correlation: {pearson}, p value: {pearson_p}")
    print(f"Spearman correlation: {spearman}, p value: {spearman_p}")

if __name__ == "__main__":
    main()