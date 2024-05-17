from sqlalchemy import create_engine, text

localhost_name = 'localhost'

db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

def create_experiment_table(engine):
    try:
        with engine.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS experiment (
                    detecttime TIMESTAMP,
                    hash VARCHAR(256) NOT NULL PRIMARY KEY,
                    curblocknumber NUMERIC(18),
                    blockspending INT,
                    timepending BIGINT,
                    detect_date VARCHAR(100)
                );
            """))
            print("Table 'experiment' has been created in the database.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Define your database connection string
    engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
    
    # Call the function to create the table
    create_experiment_table(engine)