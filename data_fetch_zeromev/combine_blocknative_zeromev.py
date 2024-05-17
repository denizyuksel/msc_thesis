import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, MetaData, Table, select
from sqlalchemy import Column, Table, MetaData, TIMESTAMP, VARCHAR, NUMERIC, INTEGER, BIGINT, Index
import logging

# Create the logs directory if it does not exist
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file_path = os.path.join(log_directory, "combine_blocknative_zeromev.log")
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

table_name = 'blocknative_zeromev'
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

metadata = MetaData()

table = Table(table_name, metadata,
    Column('block_number', NUMERIC(18), primary_key=True),
    
    # columns from aggregated blocknative data
    Column('block_date', VARCHAR(100), index=True),
    Column('tx_count', INTEGER),
    Column('private_tx_count', INTEGER),
    Column('public_tx_count', INTEGER),
    Column('gasused_gwei', NUMERIC),
    Column('private_gasused_gwei', NUMERIC),
    Column('public_gasused_gwei', NUMERIC),
    
    # columns from aggregated zeromev data
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

# Load the data from the database into Pandas DataFrames
blocknative_blocks_df = pd.read_sql('SELECT * FROM blocknative_blocks', con=engine)
logging.info(f"Blocknative table read!")

zeromev_data_df = pd.read_sql('SELECT * FROM zeromev_data', con=engine)
logging.info(f"Zeromev table read!")


# Perform a left join using Pandas, focusing on blocknative_blocks_df
joined_df = pd.merge(blocknative_blocks_df, zeromev_data_df, on='block_number', how='left')
logging.info(f"Merge finished!")

# Write the DataFrame to SQL database in a new table or replace the existing one
joined_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)  # Choose 'append' if you wish to add to an existing table
logging.info(f"Data successfully joined and written to new table {table_name}.")
