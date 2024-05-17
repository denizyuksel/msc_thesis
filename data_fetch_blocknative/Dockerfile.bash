# Use an official Ubuntu runtime as a parent image
FROM ubuntu:20.04

# Avoid prompts from apt
ARG DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /app

# Install curl and other dependencies you might need
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy the necessary files into the container
COPY download_slices.sh .
COPY one_hour_dates.txt .

# Create a directory for shared data
# Note: The actual shared directory will be mounted via docker-compose, so this is just to ensure
# the directory exists and has the correct permissions in the container.
RUN mkdir /app/data

# Command to run the Bash script
CMD ["./download_slices.sh", "one_hour_dates.txt"]