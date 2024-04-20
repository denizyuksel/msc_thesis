from sqlalchemy import create_engine, MetaData, Index


localhost_name = 'localhost'

db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

table_name = 'transactions'

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

metadata = MetaData()
metadata.reflect(engine)

# Assuming the table has already been created and is reflected in the metadata
transactions_table = metadata.tables[table_name]


# The only index we need is detect_date.
# Define the index for the 'hash' column
# index_hash = Index('index_hash', transactions_table.c.hash)
# index_hash.create(engine)
# print('Index for hash created!')

# index_date = Index('index_detect_date', transactions_table.c.detect_date)
# index_date.create(engine)
# print('Index for detect_date created!')

index_date = Index('index_curblocknumber', transactions_table.c.curblocknumber)
index_date.create(engine)
print('Index for curblocknumber created!')

# index_zero_timepending = Index('index_zero_timepending', transactions_table.c.id, postgresql_where=transactions_table.c.value == 0)
# index_zero_timepending.create(engine)
# print('Index for zero timepending values created!')