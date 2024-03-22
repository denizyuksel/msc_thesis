from datetime import date, timedelta

today = date(2024, 3, 20)

# Open a text file named 'one_hour_dates.txt' for writing
with open('one_hour_dates_full.txt', 'w') as file:
    # Generate and write dates to the file
    current_date = date(2020, 11, 1)  # Modified initialization
    while current_date <= today:
        formatted_date = current_date.strftime('%Y%m%d')
        file.write(formatted_date + '\n')
        current_date += timedelta(days=1)

print("Dates have been generated and saved to 'one_hour_dates_full.txt'.")