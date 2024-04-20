import os
import re
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import Column, Table, MetaData, TIMESTAMP, VARCHAR, NUMERIC, INTEGER, BIGINT, Index
from sqlalchemy.exc import OperationalError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
from queue import Queue
import logging

# Create the logs directory if it does not exist
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    
# Configure logging
log_file_path = os.path.join(log_directory, "filesystem_write.log")
logging.basicConfig(level=logging.INFO, filename=log_file_path, filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Use environment variable to set the host. Fallback to 'localhost' if not set.
db_host = os.getenv('DB_HOST', 'localhost')
docker_host_name = 'db'
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

def create_transactions_table(engine, metadata):
    """
    Creates the 'transactions' table in the database using the provided engine and metadata.
    """
    # Define the 'transactions' table structure
    transactions_table = Table(table_name, metadata,
        Column('detecttime', TIMESTAMP),
        Column('hash', VARCHAR(256)),
        Column('curblocknumber', NUMERIC(18), index=True),
        Column('blockspending', INTEGER),
        Column('timepending', BIGINT),
        Column('gasprice', NUMERIC(38)),
        Column('gasused', INTEGER),
        Column('nonce', NUMERIC(38)),
        Column('detect_date', VARCHAR(100), index=True)
    )
    # Create the table
    metadata.create_all(engine)
    logging.info("Table 'transactions' has been created/ensured in the database, with 'hash' as the primary key.")

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

class File:
    def __init__(self, path):
        self.path = path
        self.is_skipped = False
        self.data = None  # Placeholder for the data contained in the file

    def check_write_complete(self, max_wait_time=360, sleep_interval=2):
        start_time = time.time()
        last_size = -1
        last_mod_time = -1

        while (time.time() - start_time) < max_wait_time:
            try:
                stats = os.stat(self.path)
                current_size = stats.st_size
                current_mod_time = stats.st_mtime

                logging.info(f"Checking file: {self.path}, size: {current_size}, modified: {current_mod_time}")

                if current_size == last_size and last_mod_time == current_mod_time:
                    logging.info("File download/write appears complete.")
                    return True
                else:
                    last_size = current_size
                    last_mod_time = current_mod_time
                    time.sleep(sleep_interval)
            except Exception as e:
                logging.warning(f"Error checking file: {e}")
                time.sleep(sleep_interval)
                continue

        self.is_skipped = True
        logging.warning(f"File may still be writing or download incomplete after maximum wait time: {self.path}")
        return False

    def load_data(self):
        try:
            self.data = pd.read_csv(self.path, sep='\t', compression='gzip', on_bad_lines='warn')
            # Convert double precision 'stuck' values to boolean:
            # self.data['stuck'] = self.data['stuck'].map({1: True, 0: False, 'true': True, 'false': False, 'True': True, 'False': False}).fillna(False)
            # Filter the DataFrame for the desired conditions
            self.data = self.data[(self.data['status'] == 'confirmed') & (self.data['region'] == 'us-east-1') & (self.data['network'] == 'main')]
            
            # only consider the columns you want to have, don't drop later
            
            # Drop the unneeded columns, in some files 'dropreason' column don't exist at all.
            # if 'dropreason' not in self.data.columns:
            #     self.data['dropreason'] = None
            self.data = self.data.drop(columns=['reorg', 'replace', 'gas', 'value', 'toaddress', 'fromaddress', 'dropreason', 'input', 'type', 'maxpriorityfeepergas', 'maxfeepergas', 'basefeepergas', 'rejectionreason', 'stuck', 'status', 'region', 'failurereason', 'network'])
            return True
        except Exception as e:
            logging.error(f"Error loading data from file {self.path}: {e}")
            return False

def process_and_load_data(engine, directory_path):
    root_directory = os.path.dirname(directory_path)  # This should give you the directory containing 'data'
    skipped_directory_path = os.path.join(root_directory, 'skipped_data')  # This sets 'skipped_data' at the same level as 'data'
    
    if not os.path.exists(skipped_directory_path):
        os.makedirs(skipped_directory_path)

    accumulator_df = pd.DataFrame()
    batch_size = 10000
    directories_to_check = set()

    for root, _, files in os.walk(directory_path):
        for file_name in files:
            if file_name.endswith('.csv.gz'):
                file_obj = File(os.path.join(root, file_name))
                directories_to_check.add(root)  # Track directories for later cleanup

                if not file_obj.check_write_complete():
                    try:
                        new_path = os.path.join(skipped_directory_path, os.path.basename(file_obj.path))
                        os.rename(file_obj.path, new_path)
                        logging.info(f"Moved incomplete file to 'skipped_data': {new_path}")
                    except Exception as e:
                        logging.error(f"Failed to move incomplete file {file_obj.path} to 'skipped_data': {e}")
                    continue

                if file_obj.load_data():
                    accumulator_df = pd.concat([accumulator_df, file_obj.data], ignore_index=True)

                    if len(accumulator_df) >= batch_size:
                        try:
                            accumulator_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                            accumulator_df = pd.DataFrame()
                        except Exception as e:
                            logging.error(f"Failed to write batch to database: {e}")
                    
                    try:
                        os.remove(file_obj.path)
                        logging.info(f"Deleted processed file: {file_obj.path}")
                    except Exception as e:
                        logging.error(f"Failed to delete processed file {file_obj.path}: {e}")
                else:
                    logging.error(f"Failed to process file: {file_obj.path}")

    if not accumulator_df.empty:
        try:
            accumulator_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        except Exception as e:
            logging.error(f"Failed to write remaining data to database: {e}")

    # Cleanup: Delete empty directories
    for dir_path in directories_to_check:
        if not os.listdir(dir_path):  # Check if the directory is empty
            try:
                os.rmdir(dir_path)
                logging.info(f"Deleted empty directory: {dir_path}")
            except Exception as e:
                logging.error(f"Failed to delete directory {dir_path}: {e}")

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
        
    metadata = MetaData()
    create_transactions_table(engine, metadata)
    
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
