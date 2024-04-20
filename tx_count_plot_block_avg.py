
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

localhost_name = 'localhost'

db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

table_name = 'transactions'
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

query_dates = f"SELECT DISTINCT detect_date FROM {table_name} ORDER BY detect_date ASC"
dates = pd.read_sql(query_dates, engine)['detect_date']
print("Date query over")

# Initialize list to store daily data
daily_data_list = []

for date in dates:
    # Query to retrieve all transactions for the current date
    query_transactions = f"""
    SELECT * FROM {table_name}
    WHERE detect_date = '{date}'
    """
    transactions = pd.read_sql(query_transactions, engine)
    transactions = transactions.dropna(subset=['timepending'])
    
    # Process transactions for the current date
    if not transactions.empty:
        total_transactions = len(transactions)
        private_transactions = transactions[transactions['timepending'] == 0]
        num_private_transactions = len(private_transactions)
        percentage_private = (num_private_transactions / total_transactions) * 100 if total_transactions > 0 else 0
        
        # Calculate transactions per block and then the average
        transactions_per_block = transactions['curblocknumber'].value_counts()
        avg_tx_by_block = transactions_per_block.mean() if len(transactions_per_block) > 0 else 0
        
        # Calculate private transactions per block and then the average
        private_transactions_per_block = private_transactions['curblocknumber'].value_counts()
        avg_private_tx_by_block = private_transactions_per_block.mean() if len(private_transactions_per_block) > 0 else 0
        
        avg_percentage_private_tx_by_block = (avg_private_tx_by_block / avg_tx_by_block) * 100 if avg_tx_by_block > 0 else 0
        
        # Append the daily data to the list
        daily_data_list.append({
            'date': date,
            'avg_tx_by_block': avg_tx_by_block,
            'avg_percentage_private_tx_by_block': avg_percentage_private_tx_by_block
        })
        
# Convert the list of daily data into a DataFrame
daily_data_df = pd.DataFrame(daily_data_list)

# Convert 'date' to datetime format for plotting
daily_data_df['date'] = pd.to_datetime(daily_data_df['date'])

min_detect_date = daily_data_df['date'].min()
print("Minimum date in the dataset without NAN timepending values is: ", min_detect_date)

# Plotting
plt.figure(figsize=(15, 6))
plt.plot(daily_data_df['date'], daily_data_df['avg_percentage_private_tx_by_block'], linestyle='-', color='blue')
plt.title('Daily Average of Private Transactions Percentages per Block')
plt.xlabel('Date')
plt.ylabel('Percentage of Private Transactions (%)')
plt.ylim(0, 100)  # Percentage range

plt.xlim(left=min_detect_date)

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator())  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('figures/tx_count_plot_block_avg.png')
