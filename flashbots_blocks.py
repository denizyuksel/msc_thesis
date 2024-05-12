import requests
import csv

def fetch_data(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Raise an exception if the response status is not 200
    response.raise_for_status()
    return response.json()

def write_to_csv(data, filename):
    # Open the CSV file for writing
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Check if data contains any entries
        if data:
            # Write headers based on the keys of the first item
            headers = data[0].keys()
            writer.writerow(headers)
            # Write data rows
            for item in data:
                writer.writerow(item.values())

def main():
    url = "https://blocks.flashbots.net/v1/all_blocks"
    # Fetch data from the URL
    data = fetch_data(url)
    # Specify the filename to write to
    csv_filename = 'output.csv'
    # Write data to CSV, assuming the JSON object is a list of dictionaries
    write_to_csv(data, csv_filename)
    print(f"Data successfully written to {csv_filename}")

if __name__ == "__main__":
    main()
