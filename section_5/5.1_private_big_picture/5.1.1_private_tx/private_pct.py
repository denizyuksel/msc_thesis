import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[(data['block_date'] >= pd.Timestamp('2021-05-31')) & 
                        (data['block_date'] <= pd.Timestamp('2024-04-01'))]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'private_tx_pct' : 'median',
        'private_gasused_pct' : 'median',
    }).reset_index()
    data_by_date['private_tx_pct'] = data_by_date['private_tx_pct'].rolling(window=14, min_periods=7, center=True).mean()
    data_by_date['private_gasused_pct'] = data_by_date['private_gasused_pct'].rolling(window=14, min_periods=7, center=True).mean()
    return data_by_date

def plot_data(data_by_date, filepath):
    plt.plot(data_by_date['block_date'], data_by_date['private_tx_pct'], linestyle='-', color='#003f5c', label='Transaction Volume') #deep blue
    plt.plot(data_by_date['block_date'], data_by_date['private_gasused_pct'], linestyle='-', color='#009d57', label='Gas Usage') #emerald green

    # Fill with cyan
    plt.fill_between(data_by_date['block_date'], data_by_date['private_tx_pct'], data_by_date['private_gasused_pct'], color='aqua', alpha=0.2)

    plt.title('Evolution of Private Transaction Flow Over Time')
    plt.xlabel('Date')
    plt.ylabel('Percentage')
    plt.xlim(left=data_by_date['block_date'].min() - pd.Timedelta(days=1), right=data_by_date['block_date'].max() + pd.Timedelta(days=1))
    plt.ylim(0)

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    significant_dates = {
        '2021-10-06': ('midnightblue', '--', 'Flashbots Protect Launch'),
        '2022-09-15': ('goldenrod', '-.', 'The Merge'),
        '2022-11-11': ('steelblue', ':', 'FTX Bankruptcy'),
        '2023-03-11': ('sienna', '--', 'USDC Depeg'),
        '2023-04-05': ('olive', '-.', 'MEVBlocker Launch'),
    }
    for date, (color, linestyle, label) in significant_dates.items():
        ax.axvline(pd.Timestamp(date), color=color, linestyle=linestyle, linewidth=2, label=label)

    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(loc='upper left', ncol=2)
    plt.tight_layout()
    plt.savefig(filepath)

def calculate_correlation(data):
    # Calculate Pearson and Spearman correlation coefficients on the raw data
    pearson_corr, pearson_p_value = pearsonr(data.dropna()['private_tx_pct'], data.dropna()['private_gasused_pct'])
    spearman_corr, spearman_p_value = spearmanr(data.dropna()['private_tx_pct'], data.dropna()['private_gasused_pct'])
    return pearson_corr, pearson_p_value, spearman_corr, spearman_p_value

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    data_by_date = aggregate_data(data)
    plot_data(data_by_date, '5.1.1_private_tx_overview.png')
    pearson, pearson_p, spearman, spearman_p = calculate_correlation(data_by_date)
    print(f"Pearson correlation and p value: {pearson}, {pearson_p}")
    print(f"Spearman correlation and p value: {spearman}, {spearman_p}")

if __name__ == "__main__":
    main()