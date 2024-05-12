import pandas as pd
import json

# Function to load the first N JSON objects from a file
def load_first_n_objects(filepath, n):
    # Open and read the JSON file
    with open(filepath, 'r', encoding='utf-8') as file:
        data = json.load(file)[:n]  # Load the entire JSON array and slice the first n elements
    return pd.DataFrame(data)

# File path to your JSON file
filepath = 'output.json'

# Load the first 10 JSON objects into a DataFrame
df = load_first_n_objects(filepath, 10)

# Print the DataFrame
print(df)
