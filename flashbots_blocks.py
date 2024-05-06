import requests
import csv
from collections import Counter

def fetch_data():
    # URL of the API endpoint
    url_all = "https://blocks.flashbots.net/v1/all_blocks"
    url = "https://blocks.flashbots.net/v1/transactions"
    
    # Query parameters
    params = {'limit': 100, 'before': 15540734}

    # Send a GET request with query parameters
    response = requests.get(url_all)
    
    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to retrieve data")
        return None

def process_transactions(transactions):
    # Initialize a counter to keep track of occurrences of each block number
    block_counter = Counter()

    # Iterate over each transaction and increment the counter for its block number
    for transaction in transactions:
        block_number = transaction.get('block_number')
        if block_number:
            block_counter[block_number] += 1
    
    return block_counter

def write_to_csv(block_counts):
    # Define the CSV file name
    csv_file = "flashbots_blocks_test.csv"
    
    # Create a CSV file and write the data
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['flashbots_blocknumber', 'count_of_flashbots_tx'])
        for block_number, count in block_counts.items():
            writer.writerow([block_number, count])
        
    print("Data written to CSV")

def main():
    # Fetch the data from the API
    data = fetch_data()
    
    # If data is successfully fetched and contains 'transactions', process and write it to a CSV file
    if data and 'transactions' in data:
        block_counts = process_transactions(data['transactions'])
        write_to_csv(block_counts)

if __name__ == "__main__":
    main()
