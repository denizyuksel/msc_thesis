import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    # data = data.dropna(subset=['median_timepending'])
    # filtered_data = data[data['block_date'] <= pd.Timestamp('2022-09-15')]
    return data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'private_tx_count' : 'sum',
        'median_timepending' : 'median',
        'private_gasused_pct': 'median',
    }).reset_index()
    data_by_date['private_tx_count'] = data_by_date['private_tx_count'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['median_timepending'] = data_by_date['median_timepending'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['private_gasused_pct'] = data_by_date['private_gasused_pct'].rolling(window=14, min_periods=7, center=True).mean()

    return data_by_date

def plot_data(data, filepath):
    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    line1, = ax1.plot(data['block_date'], data['private_tx_count'], linestyle='-', color='#003f5c', label='Private Transaction Volume') # deep blue
    line2, = ax2.plot(data['block_date'], data['median_timepending'], linestyle='-', color='#DDA0DD', label='Inclusion Time') # plum

    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count', color='black')
    ax2.set_ylabel('Time pending (miliseconds)', color='#DDA0DD')

    ax1.tick_params(axis='y', labelcolor='black')
    ax2.tick_params(axis='y', labelcolor='#DDA0DD')

    plt.title('Private Transaction Volume vs Inclusion Time')
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())
    ax1.set_ylim(0)
    ax2.set_ylim(0)

    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    significant_dates = {
        '2021-08-05': ('darkviolet', '-.', 'EIP-1559 Upgrade'),
        '2021-10-06': ('midnightblue', '--', 'Flashbots Protect'),
        '2022-09-15': ('goldenrod', '-.', 'The Merge'),
        '2022-11-11': ('steelblue', ':', 'FTX Collapse'),
        '2023-03-11': ('sienna', '--', 'USDC Depeg'),
        '2023-04-27': ('#770737', '-.', 'MEV-Blocker Launch'), #mulberry
    }
    lines = [line1, line2]  # Start with the main lines
    labels = [line1.get_label(), line2.get_label()]  # Start with the main labels

    # Add vertical lines for significant dates
    for date, (color, linestyle, label) in significant_dates.items():
        line = ax1.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)
        lines.append(line)
        labels.append(label)

    leg = plt.legend(lines, labels, loc='lower left', ncol=2)
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filepath)

def find_highest_correlation(data):
    start_date = pd.Timestamp('2021-10-06')
    end_date = pd.Timestamp('2022-09-01')
    current_date = pd.Timestamp('2022-05-01')

    pearson_results = []
    spearman_results = []

    while current_date <= end_date:
        # Filter data for the current date range
        filtered_data = data[(data['block_date'] >= start_date) & (data['block_date'] <= current_date)]
        if not filtered_data.dropna().empty:
            pearson_corr, _ = pearsonr(filtered_data.dropna()['private_tx_count'], filtered_data.dropna()['fb_bundle_count'])
            spearman_corr, _ = spearmanr(filtered_data.dropna()['private_tx_count'], filtered_data.dropna()['fb_bundle_count'])

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
    pearson_corr, pearson_p_value = pearsonr(portion_data.dropna()['private_tx_count'], portion_data.dropna()['fb_bundle_count'])
    spearman_corr, spearman_p_value = spearmanr(portion_data.dropna()['private_tx_count'], portion_data.dropna()['fb_bundle_count'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.4_inclusion_time.png')
    # pearson, pearson_p, spearman, spearman_p = calculate_correlation(data_by_date)
    # print(f"Pearson correlation: {pearson}, p value: {pearson_p}")
    # print(f"Spearman correlation: {spearman}, p value: {spearman_p}")

    # top_pearson, top_spearman = find_highest_correlation(data_by_date)
    # print("Top 10 Pearson correlations and dates:")
    # for corr, date in top_pearson:
    #     print(f"{date}: {corr:.4f}")
    # print("\nTop 10 Spearman correlations and dates:")
    # for corr, date in top_spearman:
    #     print(f"{date}: {corr:.4f}")

if __name__ == "__main__":
    main()