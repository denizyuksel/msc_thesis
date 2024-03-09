from datetime import date, timedelta
import os

def increment_month(current_date):
    """Increments the month of a date object, adjusting for 12-month periods."""
    year, month, day = current_date.year, current_date.month, current_date.day

    if month == 12:
        # Move to the first day of next year
        new_date = current_date.replace(year=year + 1, month=1, day=day)
    else:
        # Increment the month directly
        new_date = current_date.replace(month=month + 1)

    return new_date

# Define starting and end dates
start_date = date(2021, 7, 15)
end_date = date.today()

# Open the output file for writing
with open("dates.txt", "w") as file:
    # iterate through each month until reaching end date
    current_date = start_date
    while current_date <= end_date:
        # Always write the 1st day of the month
        file.write(f"{current_date.strftime('%Y%m%d')}\n")

        # Write the 15th only if it's not the same as the 1st (avoid duplicate)
        if current_date.day != 15:
            file.write(f"{current_date.replace(day=15).strftime('%Y%m%d')}\n")

        # Calculate last day using timedelta to handle varying month lengths
        month_end = current_date + timedelta(days=31 - current_date.day)

        # Check for edge case on December 31st to avoid overflow
        if month_end.month != current_date.month:
            month_end = month_end.replace(day=1) - timedelta(days=1)

        file.write(f"{month_end.strftime('%Y%m%d')}\n")

        # move to the next month using the corrected function
        current_date = increment_month(current_date)

# Print a message to confirm file creation
print(f"Dates generated and saved to: {os.path.abspath('dates.txt')}")
