import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Setting up the connection to the database
localhost_name = 'localhost'
db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

# SQL query to retrieve transactions data aggregated by date
table_name = 'blocknative_zeromev'
query = f"SELECT * FROM {table_name} ORDER BY block_number ASC"
data = pd.read_sql(query, engine)

# Convert 'block_date' to datetime format for plotting
data['block_date'] = pd.to_datetime(data['block_date'])

# File path for flashbots csv
file_path = 'output.csv'

def find_max_min_block_number_and_df(file_path, column_name='block_number', chunk_size=500000):
    max_value = -float('inf')  # Initialize to negative infinity for maximum
    min_value = float('inf')  # Initialize to infinity for minimum
    df = pd.DataFrame()  # Initialize an empty DataFrame to store all chunks
    
    # Use an iterator to load chunks of the dataset
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        if column_name in chunk.columns:
            current_max = chunk[column_name].max()
            current_min = chunk[column_name].min()
            if current_max > max_value:
                max_value = current_max
            if current_min < min_value:
                min_value = current_min
        df = pd.concat([df, chunk], ignore_index=True)  # Append the current chunk to the DataFrame
    
    return max_value, min_value, df  # Return the max, min, and the complete DataFrame

# Run the function and unpack the returned values
max_block_number, min_block_number, df = find_max_min_block_number_and_df(file_path)

# Display the maximum and minimum block number
print("Maximum Block Number:", max_block_number)
print("Minimum Block Number:", min_block_number)

# You can now inspect the DataFrame 'df' to see the structure of your data
print(df.head())  # Display the first few rows of the DataFrame

# Function to calculate max bundle index + 1
def calculate_max_bundle_index(transactions):
    if transactions and isinstance(transactions, list):
        # Extract bundle_index from each transaction dictionary
        max_index = max(transaction['bundle_index'] for transaction in transactions)
        return max_index + 1
    return 1  # Default value if no transactions or wrong format

# Apply this function to each row in the DataFrame
df['bundle_tx_count'] = df['transactions'].apply(calculate_max_bundle_index)

print("AFTER BUNDLE TX COUNT IS CALCULATED")

df['block_number'] = df['block_number'].astype(data['block_number'].dtype)

# Merge the dataframes on 'block_number'
combined_data = pd.merge(data, df[['block_number', 'bundle_tx_count']], on='block_number', how='left')
print("AFTER JOIN")

# Now you can use `combined_data` for further analysis or plotting
print(combined_data.head())
# Write the combined DataFrame to a CSV file
combined_data.to_csv('flashbots_blocknative_all.csv', index=False)

# Confirmation message
print("Data has been successfully written to 'flashbots_blocknative.csv'")
