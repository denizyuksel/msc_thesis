#!/bin/bash

# Check if date file argument is provided
if [ $# -ne 1 ]; then
  echo "Usage: $0 <date_file>"
  echo "Example: $0 dates.txt"
  exit 1
fi

# Get the date file argument
DATE_FILE="$1"

# Set the base URL
DOMAIN="https://archive.blocknative.com/"

# Create the logs directory if it does not exist
LOGS_DIR="logs"
mkdir -p "${LOGS_DIR}"

# Log file name
LOG_FILE="${LOGS_DIR}/download_errors.log"

# Check if the date file exists
if [ ! -f "$DATE_FILE" ]; then
  echo "Date file not found: $DATE_FILE"
  echo "Date file not found: $DATE_FILE" >> "$LOG_FILE"
  exit 1
fi

# Loop through each date in the date file
while IFS= read -r LINE || [ -n "$LINE" ]; do
  # Remove carriage returns (\r) if present
  DATE=$(echo "$LINE" | tr -d '\r')

  # Path for the directory inside "data"
  DATA_DIR="data/$DATE"
  echo "DIRECTORY NAME IS $DATA_DIR"

  # Create the directory inside "data", including parent if it does not exist
  mkdir -p "${DATA_DIR}" || {
    echo "Error: Directory '$DATA_DIR' already exists or cannot be created."
    echo "Error: Directory '$DATA_DIR' already exists or cannot be created." >> "$LOG_FILE"
    continue  # Skip to the next date
  }

  # Initialize a variable to track successful downloads
  SUCCESSFUL_DOWNLOADS=0
  BASE_URL="${DOMAIN}${DATE}/"
  
  HOUR=18
  # Construct the URL for the current hour's data
  URL="${BASE_URL}${HOUR}.csv.gz"

  # Define the filename for the current hour's data
  FILENAME="${HOUR}.csv.gz"

  # Initialize a variable to keep track of retries
  RETRIES=0

  # Loop to handle retries on 404, 429, and 504 responses
  while true; do
      # Download the data and check the response status code
      HTTP_STATUS=$(curl -o "${DATA_DIR}/${FILENAME}" -w "%{http_code}" "$URL")

      # Check the status code and print a message
      if [ "$HTTP_STATUS" -eq 200 ]; then
          echo "Downloaded $FILENAME"
          ((SUCCESSFUL_DOWNLOADS++))
          break  # Exit the retry loop on success
      elif [ "$HTTP_STATUS" -eq 429 ] || [ "$HTTP_STATUS" -eq 504 ]; then
          echo "Received $HTTP_STATUS. Retrying in 1 second..."
          echo "Received $HTTP_STATUS for $FILENAME on $DATE. Retrying..." >> "$LOG_FILE"
          sleep 1  # Wait for 1 second before retrying
          ((RETRIES++))
          if [ $RETRIES -ge 3 ]; then
               echo "Retry limit reached for $DATE. Moving to next date."
               echo "Retry limit reached for $FILENAME on $DATE." >> "$LOG_FILE"
               break
          fi
      elif [ "$HTTP_STATUS" -eq 404 ]; then
          echo "File not found (404) for $DATE. Exiting for $FILENAME."
          echo "File not found (404) for $FILENAME on $DATE." >> "$LOG_FILE"
          break  # Exit the retry loop for 404
      else
          echo "Error downloading $FILENAME for $DATE - Status code: $HTTP_STATUS"
          echo "Error downloading $FILENAME for $DATE - Status code: $HTTP_STATUS" >> "$LOG_FILE"
          rm "${DATA_DIR}/${FILENAME}"  # Remove the empty file
          break  # Exit the retry loop on other errors
      fi
  done
done < "$DATE_FILE"

echo "Process completed."
