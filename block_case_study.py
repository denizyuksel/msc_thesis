import pandas as pd
import random

# Load the CSV file into a DataFrame
df = pd.read_csv('final_data.csv')

# Convert 'block_date' from text to datetime
df['block_date'] = pd.to_datetime(df['block_date'])

# Filter the DataFrame for rows where total_extractor_profit is not zero and block_date is before 2022-09-15
filtered_df = df[(df['total_extractor_profit'] != 0) & (df['block_date'] < '2022-09-15') & (df['private_tx_count'] > 100) & (df['private_gasused_gwei'] != 0) & (df['mev_tx_count'] > 1)]

# Extract block_numbers into a list
block_numbers = filtered_df['block_number'].tolist()

# Check if the list is empty
if block_numbers:
    # Choose a random block number from the list
    random_block_number = random.choice(block_numbers)

    # Filter the DataFrame for the row with the selected random block number
    filtered_row = df[df['block_number'] == random_block_number]

    # Check if the filtered_row is empty
    if not filtered_row.empty:
        # Iterate through each column and print the name and value
        for column in filtered_row.columns:
            print(f"{column}: {filtered_row.iloc[0][column]}")
    else:
        print("No details found for the selected block number.")
else:
    print("No blocks found that match the criteria of non-zero total_extractor_profit and date before 2022-09-15.")
