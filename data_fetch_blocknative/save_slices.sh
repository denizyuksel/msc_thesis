#!/bin/bash

# Get the date as an argument
date="$1"

# Check if date argument is provided
if [[ -z "$date" ]]; then
  echo "Please provide a date in YYYYMMDD format."
  exit 1
fi

# Validate date format (YYYYMMDD)
if [[ ! "$date" =~ ^[0-9]{8}$ ]]; then
  echo "Invalid date format: $date. Please enter in YYYYMMDD format."
  exit 1
fi

# Create directory named after the date
mkdir -p "$date" || { echo "Error: Directory '$date' already exists or cannot be created."; exit 1; }

echo "Executing download_slices.sh from: $PWD"
# Run your existing download script "download_slices.sh"
sh download_slices.sh "$date" || { echo "Error running download_slices.sh script."; exit 1; }

echo "Data downloaded and processed successfully for date: $date."