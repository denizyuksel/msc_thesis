
import pandas as pd
import os
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine, Column, Integer, MetaData, Table, select
from sqlalchemy import Column, Table, MetaData, TIMESTAMP, VARCHAR, NUMERIC, INTEGER, BIGINT, Index

import logging

# Create the logs directory if it does not exist
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file_path = os.path.join(log_directory, "aggregate.log")
logging.basicConfig(level=logging.INFO, filename=log_file_path, filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

localhost_name = 'localhost'

db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

transactions_table_name = 'transactions'
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
blocknative_byblock_table_name = 'blocknative_blocks'

# Define the metadata context
metadata = MetaData()
# Define the table structure with curblocknumber as the primary key
block_table = Table(blocknative_byblock_table_name, metadata,
                    Column('block_number', NUMERIC(18), primary_key=True),
                    Column('block_date', VARCHAR(100)),
                    Column('tx_count', INTEGER),
                    Column('private_tx_count', INTEGER),
                    Column('public_tx_count', INTEGER),
                    # Column('gasprice_gwei', NUMERIC),
                    Column('gasused_gwei', NUMERIC),
                    Column('private_gasused_gwei', NUMERIC),
                    Column('public_gasused_gwei', NUMERIC),
                    extend_existing=True)

# Create the table in the database
metadata.create_all(engine)

# Fetch distinct detect dates from the database
date_query = "SELECT DISTINCT detect_date FROM transactions ORDER BY detect_date ASC"
detect_dates = pd.read_sql(date_query, engine)['detect_date']
total_dates = len(detect_dates)
print(f"Total days to process: {total_dates}")

print("Start processing...")

# Process each detect date
for i, detect_date in enumerate(detect_dates, start=1):
    try:
        # Fetch transactions for the current detect date
        transactions_query = f"""
        SELECT curblocknumber, detect_date, timepending, gasused FROM transactions
        WHERE detect_date = '{detect_date}'
        """
        df = pd.read_sql(transactions_query, engine)
        df = df.dropna(subset=['timepending'])
        
        # Apply the scaling factor to the 'gasused' column
        df['gasused'] *= 1e-9  # Scale gas used from wei to Gwei

        # Group by 'curblocknumber'
        grouped = df.groupby('curblocknumber')
        block_data = []
        for curblocknumber, group in grouped:
            tx_count = len(group)
            public_tx_count = group[group['timepending'] != 0].shape[0]
            private_tx_count = tx_count - public_tx_count
            gasused = group['gasused'].sum()
            private_gasused = group[group['timepending'] == 0]['gasused'].sum()
            public_gasused = group[group['timepending'] != 0]['gasused'].sum()
            
            # Prepare the data row for the detect date and curblocknumber
            block_data.append({
                'block_number': curblocknumber,
                'block_date': detect_date,
                'tx_count': tx_count,
                'private_tx_count': private_tx_count,
                'public_tx_count': public_tx_count,
                'gasused_gwei': gasused,
                'private_gasused_gwei': private_gasused,
                'public_gasused_gwei': public_gasused
            })

        # Insert the batch of data into the database
        if block_data:
            pd.DataFrame(block_data).to_sql(blocknative_byblock_table_name, con=engine, if_exists='append', index=False)
        
        # Log progress
        percentage_complete = (i / total_dates) * 100
        logging.info(f"Processed date {detect_date} ({i}/{total_dates}, {percentage_complete:.2f}% complete)")

    except Exception as e:
        logging.error(f"Failed to process transactions for date {detect_date}: {e}")


print("--------PART 2: MERGE WITH ZEROMEV_DATA: DO IT IN A SEPARATE SHEET-------- ")


# # Read the zeromev_data_backup table into a DataFrame
# zeromev_data_df = pd.read_sql_table(zeromev_table_name, engine)

# # Merge the DataFrames on the respective block number columns
# merged_df = pd.merge(zeromev_data_df, block_data_df, left_on='block_number', right_on='curblocknumber')

# # Writing the merged DataFrame to a new table in the database
# merged_df.to_sql('zeromev_with_blocknative', engine, if_exists='replace', index=False)

# print("Data merged and new table 'zeromev_with_blocknative' created in the database.")