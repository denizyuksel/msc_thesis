from sqlalchemy import VARCHAR, NUMERIC, INTEGER, BIGINT
from sqlalchemy import create_engine, Column, Table, MetaData, text
from datetime import datetime, timedelta

def create_experiment_table(engine, metadata):
    """
    Creates the 'experiment' table in the database using the provided engine and metadata.
    """
    # Define the 'experiment' table structure
    experiment_table = Table('experiment', metadata,
        Column('hash', VARCHAR(256), primary_key=True),
        Column('curblocknumber', NUMERIC(18)),
        Column('blockspending', INTEGER),
        Column('timepending', BIGINT),
        Column('detect_date', VARCHAR(100), index=True)
    )
    # Create the table
    metadata.create_all(engine)
    print("Table 'experiment' created successfully.")
    
def insert_data_for_dates(engine, start_date, num_days):
    """
    Inserts data from 'transactions' into 'experiment' for the first 10 distinct dates starting from start_date.
    """
    date_list = [start_date + timedelta(days=x) for x in range(num_days)]
    for date in date_list:
        date_str = date.strftime('%Y-%m-%d')
        select_query = f"""
        SELECT hash, curblocknumber, blockspending, timepending, detect_date
        FROM transactions
        WHERE detect_date = '{date_str}';
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(select_query))
            rows = result.fetchall()
            
            if rows:
                for row in rows:
                    insert_query = text("""
                    INSERT INTO experiment (hash, curblocknumber, blockspending, timepending, detect_date)
                    VALUES (:hash, :curblocknumber, :blockspending, :timepending, :detect_date);
                    """)
                    connection.execute(insert_query, hash=row[0], curblocknumber=row[1], blockspending=row[2], timepending=row[3], detect_date=row[4])
                print(f"Data inserted for date {date_str}.")

def main():
    """
    Main function to establish database connection and create tables.
    """
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

    # Define metadata instance
    metadata = MetaData()

    start_time = datetime.now()
    with engine.connect() as connection:
        create_experiment_table(engine, metadata)
        start_date = datetime(2020, 11, 1)
        insert_data_for_dates(engine, start_date, 10)
    end_time = datetime.now()

    duration = end_time - start_time
    print(f"Total time taken: {duration}")

if __name__ == '__main__':
    main()
