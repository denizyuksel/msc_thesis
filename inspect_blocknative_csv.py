import pandas as pd

def process_chunk(chunk):
    # Filter by 'region' and 'status'
    filtered_chunk = chunk[(chunk['region'] == 'us-east-1') & (chunk['status'] == 'confirmed')]
    
    # Calculate 'private_tx_count' (timepending == 0 indicates a private transaction)
    filtered_chunk['private_tx'] = (filtered_chunk['timepending'] == 0).astype(int)
    
    # Group by 'curblocknumber' and 'detect_date', then aggregate
    aggregated = filtered_chunk.groupby(['curblocknumber', 'detect_date']).agg(
        tx_count=('hash', 'size'),                    # Count the number of transactions
        private_tx_count=('private_tx', 'sum')        # Sum the private transactions
    ).reset_index()
    
    # Rename columns appropriately
    aggregated.rename(columns={'curblocknumber': 'block_number', 'detect_date': 'block_date'}, inplace=True)
    return aggregated

# File path for the CSV file
file_path = '18.csv'

# Create an empty DataFrame to hold aggregated data
final_aggregated_df = pd.DataFrame()

# Set chunk size
chunk_size = 50000  # Adjust chunk size based on your memory capacity

# Read the file in chunks
for chunk in pd.read_csv(file_path, chunksize=chunk_size, sep='\t', usecols=['hash', 'region', 'curblocknumber', 'detect_date', 'timepending', 'status']):
    aggregated_chunk = process_chunk(chunk)
    final_aggregated_df = pd.concat([final_aggregated_df, aggregated_chunk], ignore_index=True)

# Final aggregation over all chunks to ensure unique block numbers (if needed)
final_aggregated_df = final_aggregated_df.groupby(['block_number', 'block_date']).agg(
    tx_count=('tx_count', 'sum'),
    private_tx_count=('private_tx_count', 'sum')
).reset_index()

# Write the final DataFrame to a CSV file
final_aggregated_df.to_csv('aggregated_transactions.csv', index=False)

print("Finished processing and saving aggregated data.")

