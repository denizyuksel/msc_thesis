from sqlalchemy import create_engine, MetaData, Index


localhost_name = 'localhost'

db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

table_name_transactions = 'transactions'
table_name_blocknative_blocks = 'blocknative_blocks'
table_name_zeromev_data = 'zeromev_data'
table_name_zeromev_data_backup = 'zeromev_data_backup'
table_name_blocknative_zeromev = 'blocknative_zeromev'

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

metadata = MetaData()
metadata.reflect(engine)

# Assuming the table has already been created and is reflected in the metadata
transactions_table = metadata.tables[table_name_transactions]
blocknative_blocks_table = metadata.tables[table_name_blocknative_blocks]
zeromev_data_table = metadata.tables[table_name_zeromev_data]
zeromev_data_table_backup = metadata.tables[table_name_zeromev_data_backup]
blocknative_zeromev_table = metadata.tables[table_name_blocknative_zeromev]

# The only index we need is detect_date.
# Define the index for the 'hash' column
# index_hash = Index('index_hash', transactions_table.c.hash)
# index_hash.create(engine)
# print('Index for hash created!')

# index_date = Index('index_detect_date', transactions_table.c.detect_date)
# index_date.create(engine)
# print('Index for detect_date created!')

# index_date = Index('index_curblocknumber', transactions_table.c.curblocknumber)
# index_date.create(engine)
# print('Index for curblocknumber created!')

# index_zero_timepending = Index('index_zero_timepending', transactions_table.c.id, postgresql_where=transactions_table.c.value == 0)
# index_zero_timepending.create(engine)
# print('Index for zero timepending values created!')

# index_block_number_native = Index('index_blocknumber_blocknative', blocknative_blocks_table.c.block_number)
# index_block_number_native.create(engine)
# print(f'Index for blocknumber for {blocknative_blocks_table} created!')

# index_block_number_zeromev = Index('index_blocknumber_zeromev', zeromev_data_table.c.block_number)
# index_block_number_zeromev.create(engine)
# print(f'Index for blocknumber for {zeromev_data_table} created!')

# index_block_number_zeromev_backup = Index('index_blocknumber_zeromev_backup', zeromev_data_table_backup.c.block_number)
# index_block_number_zeromev_backup.create(engine)
# print(f'Index for blocknumber for {zeromev_data_table_backup} created!')

# index_blocknative_zeromev_date = Index('index_blocknative_zeromev_date', blocknative_zeromev_table.c.block_date)
# index_blocknative_zeromev_date.create(engine)
# print(f'Index for block date for {blocknative_zeromev_table} created!')