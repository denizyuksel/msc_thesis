import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    # pre-merge data
    filtered_data = data[(data['block_date'] >= pd.Timestamp('2021-03-01')) & (data['block_date'] <= pd.Timestamp('2022-09-01'))]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'private_tx_count' : 'sum',
        'flashbots_bundle_count' : 'sum',
        # 'flashbots_exclusive_transaction_count': 'sum',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14).mean()
    data_by_date['flashbots_bundle_count'] = data_by_date['flashbots_bundle_count'].rolling(window=14).mean()
    # data_by_date['flashbots_exclusive_transaction_count'] = data_by_date['flashbots_exclusive_transaction_count'].rolling(window=14).mean()
    return data_by_date

def plot_data_double_axis(data, filepath):
    fig, ax1 = plt.subplots()

    color = '#003f5c' # deep blue
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Percentage', color=color)
    ax1.plot(data['block_date'], data['private_tx_pct'], color=color, label='Private Transactions')
    ax1.tick_params(axis='y', labelcolor=color)
    
    ax2 = ax1.twinx()  # Instantiate a second axes that shares the same x-axis
    color = '#708090' # slate gray
    ax2.set_ylabel('Count', color='gray')
    ax2.plot(data['block_date'], data['flashbots_bundle_count'], linestyle='-', color=color, label='Flashbots Bundles')
    ax2.tick_params(axis='y', labelcolor='gray')
    ax2.tick_params(axis='x', colors='gray')

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    plt.title('Private Transactions and Flashbots Bundles')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    ax1.set_ylim(0, 10)
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
    ax1.legend(handles, labels, loc='upper left')

    # Combine lines and labels for the legend
    leg = plt.legend(handles, labels, loc='upper left', frameon=True)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    # Move the rotation setting to the very end after all plotting
    plt.setp(ax1.get_xticklabels(), rotation=45)
    plt.savefig(filepath)

def plot_data(data, filepath):
    fig, ax1 = plt.subplots()

    # Plotting only on the primary axis, ax1
    line1, = ax1.plot(data['block_date'], data['private_tx_count'], linestyle='-', color='#003f5c', label='Private Transactions') # deep blue
    line2, = ax1.plot(data['block_date'], data['flashbots_bundle_count'], linestyle='-', color='#9dc183', label='Flashbots Bundles') # sage green

    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count')

    plt.title('Private Transactions and Flashbots Bundles Before the Merge')
    ax1.set_xlim(left=data['block_date'].min(),right=data['block_date'].max())
    ax1.set_ylim(0)

    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # Setting tick rotation and alignment to center
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")

    significant_dates = {
        '2021-10-06': ('midnightblue', '--', 'Flashbots Protect Launch'),
    }
    lines = [line1, line2]
    labels = [line1.get_label(), line2.get_label()]
    for date, (color, linestyle, label) in significant_dates.items():
        line = ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)
        lines.append(line)
        labels.append(label)

    # Combine lines and labels for the legend
    leg = plt.legend(lines, labels, loc='upper left', frameon=True)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filepath)

def find_highest_correlation(data):
    # start_date = pd.Timestamp('2021-10-06')
    start_date = pd.Timestamp('2021-11-01')
    end_date = pd.Timestamp('2022-07-01')
    current_date = pd.Timestamp('2022-01-01')

    pearson_results = []
    spearman_results = []

    while current_date <= end_date:
        # Filter data for the current date range
        filtered_data = data[(data['block_date'] >= start_date) & (data['block_date'] <= current_date)]
        if not filtered_data.dropna().empty:
            pearson_corr, _ = pearsonr(filtered_data.dropna()['private_tx_count'], filtered_data.dropna()['flashbots_bundle_count'])
            spearman_corr, _ = spearmanr(filtered_data.dropna()['private_tx_count'], filtered_data.dropna()['flashbots_bundle_count'])

            # Store the results with their corresponding date
            pearson_results.append((pearson_corr, current_date.strftime('%Y-%m-%d')))
            spearman_results.append((spearman_corr, current_date.strftime('%Y-%m-%d')))

        current_date += pd.DateOffset(days=1)  # Increment the date by one day

    # Sort the results by correlation value in descending order and take the top 10
    pearson_results.sort(reverse=True, key=lambda x: x[0])
    spearman_results.sort(reverse=True, key=lambda x: x[0])

    return pearson_results[:10], spearman_results[:10]

def calculate_correlation(data):
    # Calculate Pearson and Spearman correlation coefficients on the data
    portion_data = data[(data['block_date'] >= pd.Timestamp('2021-10-06')) & (data['block_date'] <= pd.Timestamp('2022-05-03'))]
    pearson_corr, pearson_p_value = pearsonr(portion_data.dropna()['private_tx_count'], portion_data.dropna()['flashbots_bundle_count'])
    spearman_corr, spearman_p_value = spearmanr(portion_data.dropna()['private_tx_count'], portion_data.dropna()['flashbots_bundle_count'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.2.1_private_vs_FB.png')
    # plot_data_double_axis(data_by_date, '5.2.1_private_vs_FB.png')
    
    pearson, pearson_p, spearman, spearman_p = calculate_correlation(data_by_date)
    print(f"Pearson correlation: {pearson}, p value: {pearson_p}")
    print(f"Spearman correlation: {spearman}, p value: {spearman_p}")

    top_pearson, top_spearman = find_highest_correlation(data_by_date)
    print("Top 10 Pearson correlations and dates:")
    for corr, date in top_pearson:
        print(f"{date}: {corr:.4f}")
    print("\nTop 10 Spearman correlations and dates:")
    for corr, date in top_spearman:
        print(f"{date}: {corr:.4f}")

if __name__ == "__main__":
    main()