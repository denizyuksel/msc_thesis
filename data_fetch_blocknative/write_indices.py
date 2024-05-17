from sqlalchemy import create_engine, text

# Database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

def create_indexes(engine):
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_detect_date ON transactions (detect_date);",
        "CREATE INDEX IF NOT EXISTS idx_blockspending ON transactions (blockspending);",
        "CREATE INDEX IF NOT EXISTS idx_curblocknumber ON transactions (curblocknumber);",
        "CREATE INDEX IF NOT EXISTS idx_timepending ON transactions (timepending);"
    ]
    
    with engine.connect() as connection:
        for query in index_queries:
            connection.execute(text(query))
            print(f"Executed: {query}")

if __name__ == "__main__":
    create_indexes(engine)
    print("Index creation complete.")
