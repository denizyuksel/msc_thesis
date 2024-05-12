import requests
import json

def fetch_data(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Raise an exception if the response status is not 200
    response.raise_for_status()
    return response.json()

def save_to_json(data, filename):
    # Open the JSON file for writing
    with open(filename, 'w') as file:
        # Write data to the file in JSON format
        json.dump(data, file, indent=4)

def main():
    url = "https://blocks.flashbots.net/v1/all_blocks"
    # Fetch data from the URL
    data = fetch_data(url)
    # Specify the filename to save to
    json_filename = 'output.json'
    # Save data to JSON
    save_to_json(data, json_filename)
    print(f"Data successfully written to {json_filename}")

if __name__ == "__main__":
    main()
