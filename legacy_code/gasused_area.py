
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
        public_transactions = transactions[transactions['timepending'] != 0]
        private_tx_gasused = private_transactions['gasused'].sum()
        public_tx_gasused = total_gas_used_all_tx - private_tx_gasused

        gas_usage_percentage_private = (private_tx_gasused / total_gas_used_all_tx) * 100 if total_gas_used_all_tx > 0 else 0
        gas_usage_percentage_public = (public_tx_gasused / total_gas_used_all_tx) * 100 if total_gas_used_all_tx > 0 else 0
        
        # Append the daily data to the list
        daily_data_list.append({
            'date': date,
            'gas_usage_percentage_private': gas_usage_percentage_private,
            'gas_usage_percentage_public': gas_usage_percentage_public
        })
        
# Convert the list of daily data into a DataFrame
daily_data_df = pd.DataFrame(daily_data_list)

# Convert 'date' to datetime format for plotting
daily_data_df['date'] = pd.to_datetime(daily_data_df['date'])

min_detect_date = daily_data_df['date'].min()
print("Minimum date in the dataset without NAN timepending values is: ", min_detect_date)

# Plotting
plt.figure(figsize=(15, 6))
plt.fill_between(daily_data_df['date'], daily_data_df['gas_usage_percentage_private'], color='skyblue', alpha=0.4, label='Private Transactions')
plt.fill_between(daily_data_df['date'], daily_data_df['gas_usage_percentage_private'], daily_data_df['gas_usage_percentage_public'], color='orange', alpha=0.4, label='Public Transactions')

plt.title('Daily Gas Used Percentage by Transaction Type Over Time')
plt.xlabel('Date')
plt.ylabel('Gas Used Percentage')
plt.xlim(left=min_detect_date)

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.MonthLocator())  # Adjust depending on the date range
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig('figures/gasused_area.png')