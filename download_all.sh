#!/bin/bash

# Check if a file argument is provided
if [ $# -eq 0 ]; then
  echo "Please provide a file containing dates in YYYYMMDD format."
  echo "Example: $0 dates.txt"
  exit 1
fi

file=$1

# Check if the file exists
if [ ! -f "$file" ]; then
  echo "File not found: $file"
  exit 1
fi

# Loop through each date in the file
while IFS= read -r date || [ -n "$date" ]; do
  # Remove any trailing carriage returns
  date=$(echo "$date" | tr -d '\r')

  # Validate date format (YYYYMMDD)
  if [[ ! "$date" =~ ^[0-9]{8}$ ]]; then
    echo "Invalid date format: $date. Please enter in YYYYMMDD format."
    continue  # Skip to next date if invalid
  fi

  echo "Running save_slices.sh for date: $date"

  # Run save_slices.sh with the current date
  sh save_slices.sh "$date"

  # Handle any errors returned by save_slices.sh (optional)
  if [ $? -ne 0 ]; then
    echo "Error running save_slices.sh for $date. Please check its logs."
  fi
done < "$file"

echo "Done processing all dates from the file."