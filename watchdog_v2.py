import os
import re
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
from queue import Queue
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define database parameters
db_params = {
    'host': 'db',  # Use the service name defined in docker-compose.yml
    'database': 'thesisdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

def wait_for_db(engine, max_retries=10, delay_between_retries=5):
    """Wait for the database to become available by attempting to connect."""
    attempt_count = 0
    while attempt_count < max_retries:
        try:
            # Attempt to connect to the database
            with engine.connect() as connection:
                logging.info("Successfully connected to the database.")
                return True
        except OperationalError as e:
            logging.warning(f"Database connection failed: {e}. Retrying in {delay_between_retries} seconds...")
            attempt_count += 1
            time.sleep(delay_between_retries)
    logging.error(f"Failed to connect to the database after {max_retries} attempts.")
    return False

# Function to create the database table if it doesn't exist
def create_transactions_table(engine):
    with engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS transactions (
                detecttime TIMESTAMP,
                hash VARCHAR(256) NOT NULL,
                status VARCHAR(100),
                region VARCHAR(100),
                reorg VARCHAR(100),
                replace VARCHAR(100),
                curblocknumber NUMERIC(18),
                failurereason VARCHAR(255),
                blockspending INT,
                timepending BIGINT,
                nonce NUMERIC(38),
                gas NUMERIC(38),
                gasprice NUMERIC(38),
                value NUMERIC(38),
                toaddress VARCHAR(256),
                fromaddress VARCHAR(256),
                input VARCHAR(65535),
                network VARCHAR(100),
                type INT,
                maxpriorityfeepergas NUMERIC(38),
                maxfeepergas NUMERIC(38),
                basefeepergas NUMERIC(38),
                dropreason VARCHAR(255),
                rejectionreason VARCHAR(255),
                stuck BOOLEAN,  -- Ensure this matches your needs; initially created as DOUBLE PRECISION?
                gasused INT,
                detect_date VARCHAR(100)
            );
        """))
        # After table creation, modify the 'stuck' column type if necessary, and add indices
        connection.execute(text("""
            ALTER TABLE transactions ALTER COLUMN stuck TYPE BOOLEAN USING CASE WHEN stuck = 0 THEN FALSE ELSE TRUE END;
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions(hash);"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_region ON transactions(region);"))

        logging.info("Table 'transactions' has been ensured in the database, with necessary modifications and indices added.")

# Queue for directories to be processed
directories_queue = Queue()

class DirectoryHandler(FileSystemEventHandler):
    def __init__(self, directories_queue):
        self.directories_queue = directories_queue

    def on_created(self, event):
        if event.is_directory and is_valid_directory(os.path.basename(event.src_path)):
            logging.info(f"Directory detected: {event.src_path}")
            # Wait a bit before processing the directory to allow files to appear
            time.sleep(10)  # Adjust the sleep time based on your needs
            self.directories_queue.put(event.src_path)

def is_valid_directory(directory_name):
    """Check if the directory name matches the 'YYYYMMDD' format."""
    return re.match(r'^\d{8}$', directory_name) is not None

def directory_processing_worker(engine, directories_queue):
    while True:
        directory_path = directories_queue.get()
        if directory_path is None:  # Sentinel value to stop the loop
            directories_queue.task_done()
            break

        logging.info(f"Processing target directory: {directory_path}")
        # Here, we actually call the function to process the directory's contents
        process_and_load_data(engine, directory_path)
        directories_queue.task_done()

def is_file_write_complete(file_path, check_interval=10, retries=3):
    last_size = -1
    attempts = 0
    while attempts < retries:
        current_size = os.path.getsize(file_path)
        logging.info(f"Checking file stability: {file_path}, current size: {current_size}, attempt: {attempts + 1}")
        if current_size == last_size:
            logging.info("File download/write appears complete.")
            return True
        else:
            last_size = current_size
            attempts += 1
            time.sleep(check_interval)
    logging.info("File may still be writing or download incomplete.")
    return False

def process_and_load_data(engine, directory_path):
    accumulator_df = pd.DataFrame()  # Initialize an empty DataFrame for accumulation
    batch_size = 10000  # Adjust based on your memory capacity and data size

    for root, _, files in os.walk(directory_path):
        for file_name in files:
            if file_name.endswith('.csv.gz'):
                file_path = os.path.join(root, file_name)
                
                # Check if the file is completely written
                if not is_file_write_complete(file_path):
                    logging.info(f"Skipping {file_path}, waiting for download to complete.")
                    continue

                logging.info(f"Processing file: {file_path}")
                try:
                    df = pd.read_csv(file_path, sep='\t', compression='gzip')
                    accumulator_df = pd.concat([accumulator_df, df], ignore_index=True)
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")

                # If the batch size is reached, write to the database and reset the DataFrame
                if len(accumulator_df) >= batch_size:
                    accumulator_df.to_sql(name='transactions', con=engine, if_exists='append', index=False)
                    accumulator_df = pd.DataFrame()

    # Process any remaining data in the accumulator
    if not accumulator_df.empty:
        accumulator_df.to_sql(name='transactions', con=engine, if_exists='append', index=False)

    # Cleanup: Delete processed files and check if the directory is empty
    for file_name in files:
        file_path = os.path.join(directory_path, file_name)
        try:
            os.remove(file_path)
            logging.info(f"Deleted processed file: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {e}")

    if not os.listdir(directory_path):
        try:
            os.rmdir(directory_path)
            logging.info(f"Deleted empty directory: {directory_path}")
        except Exception as e:
            logging.error(f"Error deleting directory {directory_path}: {e}")

def keyboard_listener(observer):
    print("Press 'c' and Enter to stop execution.")
    while True:
        if input() == 'c':
            observer.stop()
            print("Stopping observer and exiting...")
            break
        
if __name__ == "__main__":
    # Wait for the database to become available
    if not wait_for_db(engine):
        logging.error("Could not connect to the database. Exiting...")
        exit(1)
        
    create_transactions_table(engine)
    
    # Change directory_path to point to the "data" directory inside the current directory
    directory_path = os.path.join(os.getcwd(), "data")
    
    # Create "data" directory if it does not exist
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Created 'data' directory at {directory_path}")

    # Enqueue existing directories with the correct format within the "data" directory
    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)
        if os.path.isdir(item_path) and is_valid_directory(os.path.basename(item)):
            directories_queue.put(item_path)

    # Setup Watchdog observer to monitor the "data" directory
    event_handler = DirectoryHandler(directories_queue)
    observer = Observer()
    observer.schedule(event_handler, directory_path, recursive=False)
    observer.start()

    # Start the directory processing worker
    worker_thread = threading.Thread(target=directory_processing_worker, args=(engine, directories_queue))
    worker_thread.daemon = True  # Optional: makes the thread exit when the main thread does
    worker_thread.start()

    logging.info("Monitoring for new directories in 'data'...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
