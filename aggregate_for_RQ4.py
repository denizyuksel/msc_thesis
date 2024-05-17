
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

log_file_path = os.path.join(log_directory, "aggregate_for_RQ4.log")
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
blocknative_byblock_table_name = 'blocknative_blocks_rq4'

# Define the metadata context
metadata = MetaData()
# Define the table structure with curblocknumber as the primary key
block_table = Table(blocknative_byblock_table_name, metadata,
                    Column('block_number', NUMERIC(18), primary_key=True),
                    Column('block_date', VARCHAR(100)),
                    Column('tx_count', INTEGER),
                    Column('private_tx_count', INTEGER),
                    Column('public_tx_count', INTEGER),
                    
                    # Inclusion Time Impact
                    Column('timepending_block_total', BIGINT),
                    Column('mean_timepending', BIGINT),
                    Column('median_timepending', BIGINT),
                    
                    # Gas Price Impact
                    Column('mean_gasprice_gwei', NUMERIC),
                    Column('median_gasprice_gwei', NUMERIC),
                    
                    # Gas Used In Private/Public
                    Column('public_gasused_gwei', NUMERIC),
                    Column('private_gasused_gwei', NUMERIC),
                    
                    Column('private_mean_gasused', NUMERIC),
                    Column('private_median_gasused', NUMERIC),
                    
                    Column('public_mean_gasused', NUMERIC),
                    Column('public_median_gasused', NUMERIC),

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
        SELECT curblocknumber, detect_date, timepending, gasprice, gasused FROM transactions
        WHERE detect_date = '{detect_date}'
        """
        df = pd.read_sql(transactions_query, engine)
        # Our data processing starts when blocknative defined the first private transaction
        df = df.dropna(subset=['timepending'])
        
        # Apply the scaling factor to the 'gasused' column
        df['gasused'] *= 1e-9  # Scale gas used from wei to Gwei
        df['gasprice'] *= 1e-9  # Scale gas used from wei to Gwei

        # Group by 'curblocknumber'
        grouped = df.groupby('curblocknumber')
        block_data = []
        for curblocknumber, group in grouped:
            tx_count = len(group)
            public_tx_count = group[group['timepending'] != 0].shape[0]
            private_tx_count = tx_count - public_tx_count
        
            mean_gasprice_gwei = group['gasprice'].mean()
            median_gasprice_gwei = group['gasprice'].median()
            
            # Only private tx have timepending > 0
            timepending_block_total = group['timepending'].sum()
            mean_timepending = group['timepending'].mean()
            median_timepending = group['timepending'].median()
            
            private_gasused_gwei = group[group['timepending'] == 0]['gasused'].sum()
            private_mean_gasused = group[group['timepending'] == 0]['gasused'].mean()
            private_median_gasused = group[group['timepending'] == 0]['gasused'].median()
            
            public_gasused_gwei = group[group['timepending'] != 0]['gasused'].sum()
            public_mean_gasused = group[group['timepending'] != 0]['gasused'].mean()
            public_median_gasused = group[group['timepending'] != 0]['gasused'].median()
            
            # Prepare the data row for the detect date and curblocknumber
            block_data.append({
                'block_number': curblocknumber,
                'block_date': detect_date,
                
                # Tx counts
                'tx_count': tx_count,
                'private_tx_count': private_tx_count,
                'public_tx_count': public_tx_count,
                
                # Timepending impact - RQ4
                'timepending_block_total': timepending_block_total,
                'mean_timepending': mean_timepending,
                'median_timepending': median_timepending,
                
                # Gas impact - usage RQ4
                'private_gasused_gwei': private_gasused_gwei,
                'public_gasused_gwei': public_gasused_gwei,
                
                'private_mean_gasused': private_mean_gasused,
                'private_median_gasused': private_median_gasused,
                
                'public_mean_gasused': public_mean_gasused,
                'public_median_gasused': public_median_gasused,
                
                # Gas impact - price RQ4
                'mean_gasprice_gwei': mean_gasprice_gwei,
                'median_gasprice_gwei': median_gasprice_gwei,
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