
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

## GASUSED PLOT

query_dates = f"SELECT DISTINCT detect_date FROM {table_name} WHERE detect_date > '2021-12-01' ORDER BY detect_date ASC"
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
        
        # convert to gwei
        transactions['gasused'] = transactions['gasused'] * 1e-9
        total_gas_used_all_tx = transactions['gasused'].sum()
        
        private_transactions = transactions[transactions['timepending'] == 0]
        private_tx_gasused = private_transactions['gasused'].sum()
        
        public_transactions = transactions[transactions['timepending'] != 0]
        public_tx_gasused = public_transactions['gasused'].sum()
        
        # Calculate total gas used per block for all transactions
        gas_used_per_block = transactions.groupby('curblocknumber')['gasused'].sum()
        avg_gas_used_per_block = gas_used_per_block.mean() if len(gas_used_per_block) > 0 else 0
        
        # Calculate total gas used per block for public transactions
        public_gas_used_per_block = public_transactions.groupby('curblocknumber')['gasused'].sum()
        avg_public_gas_used_per_block = public_gas_used_per_block.mean() if len(public_gas_used_per_block) > 0 else 0

        # Calculate total gas used per block for private transactions
        private_gas_used_per_block = private_transactions.groupby('curblocknumber')['gasused'].sum()
        avg_private_gas_used_per_block = private_gas_used_per_block.mean() if len(private_gas_used_per_block) > 0 else 0
        
        avg_gas_usage_percentage_private_per_block = (avg_private_gas_used_per_block / avg_gas_used_per_block) * 100 if avg_gas_used_per_block > 0 else 0
        avg_gas_usage_percentage_public_per_block = (avg_public_gas_used_per_block / avg_gas_used_per_block) * 100 if avg_gas_used_per_block > 0 else 0
        
        print(f"avg percentage private gas used per block: {avg_gas_usage_percentage_private_per_block}, avg percentage public gas used per block: {avg_gas_usage_percentage_public_per_block}")

        # Append the daily data to the list
        daily_data_list.append({
            'date': date,
            'avg_gas_usage_percentage_private_per_block': avg_gas_usage_percentage_private_per_block,
            'avg_gas_usage_percentage_public_per_block': avg_gas_usage_percentage_public_per_block,
        })
        
# Convert the list of daily data into a DataFrame
daily_data_df = pd.DataFrame(daily_data_list)

# Convert 'date' to datetime format for plotting
daily_data_df['date'] = pd.to_datetime(daily_data_df['date'])

min_detect_date = daily_data_df['date'].min()
print("Minimum date in the dataset without NAN timepending values is: ", min_detect_date)

# Plotting
plt.figure(figsize=(15, 6))
plt.fill_between(daily_data_df['date'], daily_data_df['avg_gas_usage_percentage_private_per_block'], color='skyblue', alpha=0.4, label='Private Transactions')
plt.fill_between(daily_data_df['date'], daily_data_df['avg_gas_usage_percentage_private_per_block'], daily_data_df['avg_gas_usage_percentage_public_per_block'], color='orange', alpha=0.4, label='Public Transactions')

plt.title('Daily Gas Used Percentage per Block by Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Daily Block Gas Used Percentage')
plt.xlim(left=min_detect_date)

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator())  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig('figures/gasused_area_block_avg.png')