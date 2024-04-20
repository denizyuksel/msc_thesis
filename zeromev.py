import requests
import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, MetaData, Table, select
from sqlalchemy import Column, Table, MetaData, TIMESTAMP, VARCHAR, NUMERIC, INTEGER, BIGINT, Index
from sqlalchemy.sql import text
import time

from sqlalchemy.orm import sessionmaker
import logging

# Create the logs directory if it does not exist
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file_path = os.path.join(log_directory, "zeromev.log")
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

table_name = 'zeromev_data'

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
metadata = MetaData()

table = Table(table_name, metadata,
    Column('block_number', NUMERIC(18), primary_key=True),
    
    Column('total_user_swap_volume', NUMERIC),
    Column('total_extractor_profit', NUMERIC),
    
    Column('multiple_count', INTEGER),
    Column('aave_count', INTEGER),
    Column('balancer1_count', INTEGER),
    Column('bancor_count', INTEGER),
    Column('compoundv2_count', INTEGER),
    Column('curve_count', INTEGER),
    Column('uniswap2_count', INTEGER),
    Column('uniswap3_count', INTEGER),
    Column('zerox_count', INTEGER),
    Column('unknown_count', INTEGER),
    
    Column('arb_extractor_profit', NUMERIC),
    Column('frontrun_extractor_profit', NUMERIC),
    Column('sandwich_extractor_profit', NUMERIC),
    Column('backrun_extractor_profit', NUMERIC),
    Column('liquid_extractor_profit', NUMERIC),
    Column('swap_extractor_profit', NUMERIC),
    
    Column('arb_user_swap_volume', NUMERIC),
    Column('frontrun_user_swap_volume', NUMERIC),
    Column('sandwich_user_swap_volume', NUMERIC),
    Column('backrun_user_swap_volume', NUMERIC),
    Column('liquid_user_swap_volume', NUMERIC),
    Column('swap_user_swap_volume', NUMERIC),
    
    Column('arb_count', INTEGER),
    Column('frontrun_count', INTEGER),
    Column('sandwich_count', INTEGER),
    Column('backrun_count', INTEGER),
    Column('liquid_count', INTEGER),
    Column('swap_count', INTEGER), # user tx count
    )
    # Create the table
metadata.create_all(engine)
logging.info(f"Table {table_name} has been created/ensured in the database")

# Function to get the MEV transaction count and other details for a specific block number
def get_mev_tx_info(start_block_number, count=100):
    url = f"https://data.zeromev.org/v1/mevBlock?block_number={start_block_number}&count={count}"
    response = requests.get(url, headers={'accept': 'application/json'})
    
    if response.status_code == 200:
        data = response.json()
        logging.info(f"Request successful, retrieved data for block number {start_block_number} to {start_block_number + count - 1}")

        # Initialize dictionaries to track transaction counts
        transaction_counts = {}
        financial_aggregates = {}
        protocol_counts = {}
        block_volumes = {}
        
        for item in data:
            mev_type = item['mev_type']
            block_number = item['block_number']
            protocol = item.get('protocol', 'unknown')  # Handle possible missing 'protocol' field

            # Update transaction counts
            if mev_type not in transaction_counts:
                transaction_counts[mev_type] = {}
            if block_number in transaction_counts[mev_type]:
                transaction_counts[mev_type][block_number] += 1
            else:
                transaction_counts[mev_type][block_number] = 1
                
            if protocol not in protocol_counts:
                protocol_counts[protocol] = {}
            if block_number in protocol_counts[protocol]:
                protocol_counts[protocol][block_number] += 1
            else:
                protocol_counts[protocol][block_number] = 1
                
            # Initialize or update financial aggregates for each mev_type and block_number
            if block_number not in financial_aggregates:
                financial_aggregates[block_number] = {}
                block_volumes[block_number] = {'total_extractor_profit': 0, 'total_user_swap_volume': 0}
            if f"{mev_type}_user_swap_volume" not in financial_aggregates[block_number]:
                financial_aggregates[block_number][f"{mev_type}_user_swap_volume"] = 0
            if f"{mev_type}_extractor_profit" not in financial_aggregates[block_number]:
                financial_aggregates[block_number][f"{mev_type}_extractor_profit"] = 0

                            
            # Sum 'user_swap_volume_usd' and 'extractor_profit_usd' for each mev_type
            user_volume = item.get('user_swap_volume_usd', 0) or 0
            extractor_profit = item.get('extractor_profit_usd', 0) or 0
            financial_aggregates[block_number][f"{mev_type}_user_swap_volume"] += item.get('user_swap_volume_usd', 0) or 0
            financial_aggregates[block_number][f"{mev_type}_extractor_profit"] += item.get('extractor_profit_usd', 0) or 0
        
            # Aggregate total volumes per block
            block_volumes[block_number]['total_user_swap_volume'] += user_volume
            block_volumes[block_number]['total_extractor_profit'] += extractor_profit
            
        # Prepare data for DataFrame conversion and database insertion
        all_transactions = []
        for block_number in set(k for dic in transaction_counts.values() for k in dic):
            transaction_record = {'block_number': block_number}
            # Include transaction counts
            for mev_type, counts in transaction_counts.items():
                if block_number in counts:
                    transaction_record[f"{mev_type}_count"] = counts[block_number]
                    transaction_record.update(financial_aggregates[block_number])
            # Include protocol counts
            for protocol, counts in protocol_counts.items():
                if block_number in counts:
                    transaction_record[f"{protocol}_count"] = counts[block_number]
            # Include total volumes
            transaction_record.update(block_volumes[block_number])
            
            all_transactions.append(transaction_record)

        # Convert the list of transactions to a DataFrame
        df = pd.DataFrame(all_transactions)
        if not df.empty:
            # Pivot the DataFrame to have one row per block_number with separate columns for each mev_type
            df = df.groupby('block_number').sum().reset_index()

        # Return both DataFrames in a tuple
        return df
    else:
        logging.error(f"Failed to fetch data for block number {start_block_number} to {start_block_number + count - 1}")
        return None

def write_to_db(df, engine):
    if not df.empty:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info("Data written successfully to the database.")
    else:
        logging.warning("No data to write to the database.")

def main():
    start_time = time.time()  # Start the clock
    start_block_number = 19220986
    end_block_number = 19499238
    blocks_per_request = 100
    total_blocks = end_block_number - start_block_number + 1
    steps = total_blocks // blocks_per_request

    for i in range(steps + 1):  # Iterate one extra time if necessary to cover all blocks
        current_start_block = start_block_number + i * blocks_per_request
        # Determine the number of blocks to request for the current iteration
        if i < steps:
            current_blocks_count = blocks_per_request
        else:
            current_blocks_count = total_blocks % blocks_per_request

        if current_blocks_count == 0:
            break  # If no more blocks to process, exit the loop

        mev_df = get_mev_tx_info(current_start_block, current_blocks_count)
        if mev_df is not None:
            logging.info(f"Processing for blocks starting at {current_start_block}")
            write_to_db(mev_df, engine)
        else:
            logging.warning(f"No data processed for blocks starting at {current_start_block}")
            
    end_time = time.time()  # End the clock
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")  # Log the total execution time

if __name__ == '__main__':
    main()