import os
import time
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Use environment variable to set the host. Fallback to 'localhost' if not set.
db_host = os.getenv('DB_HOST', 'localhost')
localhost_name = 'localhost'

db_params = {
    'host': localhost_name,
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def copy_data_in_chunks(engine, start_date, end_date, chunk_size=1):
    current_start_date = start_date
    chunk_size = datetime.timedelta(days=chunk_size)  # Defines the chunk size as a number of days
    
    total_rows_copied = 0
    while current_start_date <= end_date:
        chunk_end_date = current_start_date + chunk_size
        with engine.begin() as connection:  # Use a transaction for each chunk
            connection.execute(text("""
                INSERT INTO experiment (hash, curblocknumber, blockspending, timepending, detect_date)
                SELECT hash, curblocknumber, blockspending, timepending, detect_date
                FROM transactions
                WHERE detect_date::date >= :start AND detect_date::date < :end
                ON CONFLICT (hash) DO NOTHING;
                """), {'start': current_start_date, 'end': chunk_end_date})

            # After each insert, count rows to monitor progress
            count_result = connection.execute(text("SELECT COUNT(*) FROM experiment;"))
            rows_after_insert = count_result.fetchone()[0]
            rows_inserted_this_chunk = rows_after_insert - total_rows_copied
            total_rows_copied = rows_after_insert
            
            print(f"Copied {rows_inserted_this_chunk} rows for date range {current_start_date} to {chunk_end_date}. Total rows copied: {total_rows_copied}")
        
        current_start_date = chunk_end_date  # Prepare for the next chunk


if __name__ == "__main__":
    engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
        
    start_date = datetime.date(2020, 11, 1)
    end_date = datetime.date(2020, 11, 5)
    
    copy_data_in_chunks(engine, start_date, end_date)