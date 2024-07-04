import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

def load_and_prepare_data(filepath):
    data = pd.read_csv(filepath)
    data['block_date'] = pd.to_datetime(data['block_date'])
    filtered_data = data[data['block_date'] >= pd.Timestamp('2022-09-15')]
    return filtered_data

def aggregate_data(data):
    data_by_date = data.groupby('block_date').agg({
        'mev_tx_count' : 'sum',
        'total_extractor_profit': 'sum',
        'arb_extractor_profit': 'sum',
        'sandwich_extractor_profit': 'sum',
        'liquid_extractor_profit': 'sum'
    }).reset_index()
    data_by_date['mev_tx_count'] = data_by_date['mev_tx_count'].rolling(window=14).mean()
    
    data_by_date['total_extractor_profit_without_swaps'] = (
        data_by_date['arb_extractor_profit'] + 
        data_by_date['sandwich_extractor_profit'] + 
        data_by_date['liquid_extractor_profit']
    )
    # data_by_date['total_extractor_profit_without_swaps'] = data_by_date['total_extractor_profit_without_swaps'].rolling(window=5).mean()

    return data_by_date

# Function to format the y-axis labels to show numbers in thousands
def thousands_formatter(x, pos):
    return f'${int(x / 1_000)}K'

def plot_data_double_axis(data, mev_share_data, mev_blocker_data, filepath):
    # Smoothing data
    mev_share_data['mev_share_bundle_count'] = mev_share_data['mev_share_bundle_count'].rolling(window=14).mean()
    mev_blocker_data['mined'] = mev_blocker_data['mined'].rolling(window=14).mean()
    
    fig, ax1 = plt.subplots()

    data['cumulative_profit'] = data['total_extractor_profit_without_swaps'].cumsum()

    # First axis plots
    color = 'navy'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Count', color="black")
    line1, = ax1.plot(mev_share_data['block_date'], mev_share_data['mev_share_bundle_count'], color="cornflowerblue", label='MEV-Share Bundles')
    line2, = ax1.plot(mev_blocker_data['block_date'], mev_blocker_data['mined'], color="cyan", label='MEV Blocker Transactions')
    line3, = ax1.plot(data['block_date'], data['mev_tx_count'], color="orange", label='MEV Activity')
    ax1.set_yscale('log')

    ax1.tick_params(axis='y', labelcolor="black")

    # Setting up the right y-axis for cumulative profit
    ax2 = ax1.twinx()
    ax2.set_ylabel('Cumulative Profit (USD)', color='green')
    ax2.plot(data['block_date'], data['cumulative_profit'], linestyle='--', color='limegreen', label='Extractor Profit')
    ax2.tick_params(axis='y', labelcolor='green')

    ax2.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))

    plt.setp(ax1.get_xticklabels(), rotation=45, ha="center")
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="center")

    plt.title('Post-Merge MEV Activity, OFAs, and Extractor Profit')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.set_xlim(left=data['block_date'].min(), right=data['block_date'].max())

    # ax1.set_ylim(0)
    ax2.set_ylim(0)

    significant_dates = {
        '2023-04-01': ('blue', '-.', 'MEV Share Launch'),
        '2023-04-05': ('magenta', '-.', 'MEV Blocker Launch')
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
    leg = plt.legend(handles, labels, loc='upper left', fontsize="medium")
    leg.set_zorder(100)  # Ensure legend is on top

    plt.grid(True)
    plt.savefig(filepath)

def main():
    data = load_and_prepare_data('../../../final_data.csv')
    mev_share_data = load_and_prepare_data('../../../mevshare_by_date.csv')
    mev_blocker_data = load_and_prepare_data('../../../mevblocker_deniz_18_distinct.csv')
    data_by_date = aggregate_data(data)
    plot_data_double_axis(data_by_date, mev_share_data, mev_blocker_data, '5.3.2_mev_ofa.png')

if __name__ == "__main__":
    main()