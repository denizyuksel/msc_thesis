import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr
from matplotlib.ticker import FuncFormatter

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'flashbots_bundle_count' : 'sum',
        'mev_tx_count': 'sum',
        'total_extractor_profit': 'sum',
        'arb_extractor_profit': 'sum',
        'sandwich_extractor_profit': 'sum',
        'liquid_extractor_profit': 'sum',
    }).reset_index()
    data_by_date['flashbots_bundle_count'] = data_by_date['flashbots_bundle_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['mev_tx_count'] = data_by_date['mev_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
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

    data['cumulative_flashbots_bundle_count'] = data['flashbots_bundle_count'].cumsum()
    data['cumulative_mev_tx_count'] = data['mev_tx_count'].cumsum()
    data['cumulative_profit'] = data['total_extractor_profit_without_swaps'].cumsum()

    # Plotting counts on the left y-axis
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count')
    ax1.plot(data['block_date'], data['flashbots_bundle_count'], color='#9dc183', label='Flashbots Bundles') # sage green
    ax1.plot(data['block_date'], data['mev_tx_count'], color='#f08080', label='MEV Activity') # light coral
    ax1.tick_params(axis='y')

    # Setting up the right y-axis for cumulative profit
    ax2 = ax1.twinx()
    ax2.set_ylabel('Cumulative Extractor Profit (USD)', color='darkred')
    ax2.plot(data['block_date'], data['cumulative_profit'], linestyle='--', color='#DC143C', label='Cumulative Extractor Profit') #crimson
    ax2.tick_params(axis='y', labelcolor='darkred')

    ax2.yaxis.set_major_formatter(FuncFormatter(millions_formatter))

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    plt.title('Pre-Merge MEV Transactions, Flashbots Bundles, and Extractor Profit')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    ax1.set_ylim(0)
    ax2.set_ylim(0)

    significant_dates = {
        '2021-10-06': ('midnightblue', '--', 'Flashbots Protect Launch'),
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
    leg = plt.legend(handles, labels, loc='upper left', frameon=True, ncol=2)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.savefig(filepath)

def plot_data(data, filepath):
    fig, ax1 = plt.subplots()

    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count')
    ax1.plot(data['block_date'], data['flashbots_bundle_count'], color='blue', label='Flashbots Bundles')
    ax1.plot(data['block_date'], data['mev_tx_count'], color='darkorange', label='MEV Activity')
    
    ax2 = ax1.twinx()  # Instantiate a second axes that shares the same x-axis
    color = 'limegreen'
    ax2.set_ylabel('Profit (USD)', color="green")
    ax2.tick_params(axis='y', labelcolor='green') # Modified line
    ax2.plot(data['block_date'], data['total_extractor_profit'], color=color, linestyle='--', label='Total Extractor Profit')
    
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

def find_highest_correlation(data):
    # start_date = pd.Timestamp('2021-10-06')
    start_date = pd.Timestamp('2021-10-06')
    end_date = pd.Timestamp('2022-07-01')
    current_date = pd.Timestamp('2022-06-15')

    pearson_results = []
    spearman_results = []

    while current_date <= end_date:
        # Filter data for the current date range
        filtered_data = data[(data['block_date'] >= start_date) & (data['block_date'] <= current_date)]
        if not filtered_data.dropna().empty:
            pearson_corr, _ = pearsonr(filtered_data.dropna()['mev_tx_count'], filtered_data.dropna()['flashbots_bundle_count'])
            spearman_corr, _ = spearmanr(filtered_data.dropna()['mev_tx_count'], filtered_data.dropna()['flashbots_bundle_count'])

            # Store the results with their corresponding date
            pearson_results.append((pearson_corr, current_date.strftime('%Y-%m-%d')))
            spearman_results.append((spearman_corr, current_date.strftime('%Y-%m-%d')))

        current_date += pd.DateOffset(days=1)  # Increment the date by one day

    # Sort the results by correlation value in descending order and take the top 10
    pearson_results.sort(reverse=True, key=lambda x: x[0])
    spearman_results.sort(reverse=True, key=lambda x: x[0])

    return pearson_results[:60], spearman_results[:60]

def calculate_correlation(data):
    # Calculate Pearson and Spearman correlation coefficients on the raw data
    portion_data = data[((data['block_date'] >= pd.Timestamp('2021-10-06')) & (data['block_date'] < pd.Timestamp('2022-06-15')))]
    pearson_corr, pearson_p_value = pearsonr(portion_data.dropna()['flashbots_bundle_count'], portion_data.dropna()['mev_tx_count'])
    spearman_corr, spearman_p_value = spearmanr(portion_data.dropna()['flashbots_bundle_count'], portion_data.dropna()['mev_tx_count'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    # plot_data(data_by_date, '5.2.2_profit_mev_tx_fb.png')
    plot_data_double_axis(data_by_date, '5.2.2_profit_mev_fb.png')

    pearson, pearson_p, spearman, spearman_p = calculate_correlation(data_by_date)
    print(f"Pearson correlation and p value: {pearson}, {pearson_p}")
    print(f"Spearman correlation and p value: {spearman}, {spearman_p}")

    top_pearson, top_spearman = find_highest_correlation(data_by_date)
    print("Top 10 Pearson correlations and dates:")
    for corr, date in top_pearson:
        print(f"{date}: {corr:.4f}")
    print("\nTop 10 Spearman correlations and dates:")
    for corr, date in top_spearman:
        print(f"{date}: {corr:.4f}")

if __name__ == "__main__":
    main()