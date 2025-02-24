import os
import time
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess

# Configuration
LOG_FILE = "D:/automation_a_xvii_la_supply/processed_files.log"
CLEANING_SCRIPT = "D:/automation_a_xvii_la_supply/python files/clean_data.py"
FOLDER_TO_WATCH = "D:/automation_a_xvii_la_supply/dashboard files"
HOST = 'localhost'
USER = 'root'
PASSWORD = 'your_password'
DATABASE = 'mtb_supply'
TABLE_NAME = 'all_test'

# Create a MySQL connection pool for better performance
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,  # Adjust based on needs
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE
)

def run_cleaning_script(input_file, output_file):
    """Runs the Python script to clean the CSV data."""
    try:
        subprocess.run(["python", CLEANING_SCRIPT, input_file, output_file], check=True)
    except Exception as e:
        print(f"Error running cleaning script: {e}")

def is_file_processed(filename):
    """Checks if a file has already been processed."""
    if not os.path.exists(LOG_FILE):
        return False
    with open(LOG_FILE, "r") as log:
        return filename in log.read().splitlines()

def mark_file_as_processed(filename):
    """Marks a file as processed."""
    with open(LOG_FILE, "a") as log:
        log.write(filename + "\n")

def remove_duplicates_from_db():
    """Removes duplicate rows from MySQL."""
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor()

        cursor.execute(f"""
            DELETE t1 FROM {TABLE_NAME} t1
            INNER JOIN {TABLE_NAME} t2
            WHERE t1.Row_number > t2.Row_number 
            AND t1.Test_date_time = t2.Test_date_time
            AND t1.Profile_id = t2.Profile_id
            AND t1.Patient_id = t2.Patient_id
            AND t1.Test_result = t2.Test_result
            AND t1.Test_status = t2.Test_status
            AND t1.Lab_name = t2.Lab_name
            AND t1.User_name = t2.User_name
            AND t1.Sample_type = t2.Sample_type
            AND t1.Truelab_id = t2.Truelab_id
            AND t1.Lot = t2.Lot
            AND t1.Chip_serial_no = t2.Chip_serial_no
            AND t1.Raw_data_filename = t2.Raw_data_filename
            AND t1.Ct1 = t2.Ct1
            AND t1.Ct2 = t2.Ct2
            AND t1.Ct3 = t2.Ct3
            AND t1.Bayno = t2.Bayno
            AND t1.Chip_batchno = t2.Chip_batchno
            AND t1.Result_recieved_date = t2.Result_recieved_date;
        """)

        connection.commit()
        cursor.close()
        connection.close()
        print(f"Duplicate rows removed from {TABLE_NAME}.")

    except Exception as e:
        print(f"Error removing duplicates: {e}")

def load_csv_to_mysql(file_path, filename):
    """Loads cleaned CSV data into MySQL efficiently."""
    if is_file_processed(filename):
        print(f"Skipping {filename}: Already processed.")
        return

    try:
        df = pd.read_csv(file_path)

        connection = connection_pool.get_connection()
        cursor = connection.cursor()

        # Ensure table exists
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                `Row_number` INT AUTO_INCREMENT PRIMARY KEY,
                Test_date_time DATETIME,
                Profile_id VARCHAR(255),
                Patient_id VARCHAR(255),
                Test_result VARCHAR(255),
                Test_status VARCHAR(255),
                Lab_name VARCHAR(255),
                User_name VARCHAR(255),
                Sample_type VARCHAR(255),
                Truelab_id VARCHAR(255),
                Lot VARCHAR(255),
                Chip_serial_no VARCHAR(255),
                Raw_data_filename VARCHAR(255),
                Ct1 FLOAT,
                Ct2 FLOAT,
                Ct3 FLOAT,
                Bayno VARCHAR(255),
                Chip_batchno VARCHAR(255),
                Result_recieved_date DATETIME
            )''')

        # Bulk insert for efficiency
        data_to_insert = [tuple(row) for row in df.to_numpy()]
        insert_query = f'''
            INSERT INTO {TABLE_NAME} (
                Test_date_time, Profile_id, Patient_id, Test_result, Test_status,
                Lab_name, User_name, Sample_type, Truelab_id, Lot, Chip_serial_no,
                Raw_data_filename, Ct1, Ct2, Ct3, Bayno, Chip_batchno, Result_recieved_date
            ) VALUES ({', '.join(['%s'] * len(df.columns))})
        '''
        cursor.executemany(insert_query, data_to_insert)

        connection.commit()
        cursor.close()
        connection.close()

        print(f"Data from {file_path} loaded successfully.")

        # Mark file as processed
        mark_file_as_processed(filename)

        # Delete file after successful upload
        os.remove(file_path)
        print(f"Deleted: {file_path}")

    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")

class CsvHandler(FileSystemEventHandler):
    """Handles new CSV files detected in the folder."""
    def process_existing_files(self):
        """Processes unprocessed files already in the folder."""
        for filename in os.listdir(FOLDER_TO_WATCH):
            if filename.endswith('.csv') and not is_file_processed(filename):
                file_path = os.path.join(FOLDER_TO_WATCH, filename)
                cleaned_file_path = file_path.replace(".csv", "_cleaned.csv")
                try:
                    print(f"Processing existing file: {filename}")
                    run_cleaning_script(file_path, cleaned_file_path)
                    load_csv_to_mysql(cleaned_file_path, filename)
                except Exception as e:
                    print(f"Error processing {filename}: {e}")

    def on_created(self, event):
        """Handles newly created CSV files."""
        if not event.is_directory and event.src_path.endswith('.csv'):
            filename = os.path.basename(event.src_path)
            print(f"New file detected: {filename}")
            cleaned_file_path = event.src_path.replace(".csv", "_cleaned.csv")
            try:
                run_cleaning_script(event.src_path, cleaned_file_path)
                load_csv_to_mysql(cleaned_file_path, filename)
            except Exception as e:
                print(f"Error processing {filename}: {e}")

def monitor_folder():
    """Monitors the folder for new CSV files."""
    event_handler = CsvHandler()

    # Process existing files before starting live monitoring
    event_handler.process_existing_files()

    observer = Observer()
    observer.schedule(event_handler, FOLDER_TO_WATCH, recursive=False)
    observer.start()

    print(f"Monitoring folder: {FOLDER_TO_WATCH} for new CSV files...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    monitor_folder()
