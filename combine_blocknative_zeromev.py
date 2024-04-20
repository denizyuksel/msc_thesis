import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy import inspect
from sqlalchemy.ext.automap import automap_base
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

engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

# Load the data from the database into Pandas DataFrames
blocknative_blocks_df = pd.read_sql('SELECT * FROM blocknative_blocks', con=engine)
zeromev_data_df = pd.read_sql('SELECT * FROM zeromev_data', con=engine)

# Perform a left join using Pandas, focusing on blocknative_blocks_df
joined_df = pd.merge(blocknative_blocks_df, zeromev_data_df, on='block_number', how='left')

# Write the DataFrame to SQL database in a new table or replace the existing one
new_table_name = 'blocknative_zeromev'
joined_df.to_sql(new_table_name, con=engine, if_exists='replace', index=False)  # Choose 'append' if you wish to add to an existing table

print(f"Data successfully joined and written to new table {new_table_name}.")
