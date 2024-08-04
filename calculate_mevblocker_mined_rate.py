import pandas as pd

# Load the data into a DataFrame
df = pd.read_csv('mevblocker_deniz_18_distinct.csv')

# Sum all the 'mined' and 'total_transactions'
total_mined = df['mined'].sum()
total_transactions = df['total_transactions'].sum()

# Calculate the overall ratio (percentage) of mined transactions to total transactions
overall_ratio = total_mined / total_transactions

# Print the overall ratio as a percentage
print(f"The overall percentage of mined transactions to total transactions is: {overall_ratio * 100:.2f}%")
