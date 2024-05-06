import requests
import csv

def fetch_data():
    # URL of the API endpoint
    url = "https://blocks.flashbots.net/v1/blocks"
    
    # Send a GET request
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to retrieve data")
        return None

def write_to_csv(data):
    # Define the CSV file name
    csv_file = "blocks_100.csv"
    
    # Fieldnames for the CSV based on the key names of the JSON objects
    fieldnames = data['blocks'][0].keys()

    # Create a CSV file and write the data
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for block in data['blocks']:
            writer.writerow(block)
    
    print("Data written to CSV")

def main():
    # Fetch the data from the API
    data = fetch_data()
    
    # If data is successfully fetched, write it to a CSV file
    if data:
        write_to_csv(data)

if __name__ == "__main__":
    main()
