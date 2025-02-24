import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Directory to be monitored
folder_to_watch = "D:/automation_a_xvii_la_supply/dashboard files"

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        # Trigger the data cleaning process when a new file is added
        print(f"New file added: {event.src_path}")
        clean_data(event.src_path)

def clean_data(file_path):
    # This function will handle the data cleaning process
    print(f"Cleaning the file: {file_path}")
    # Add your cleaning code here (e.g., pandas, etc.)
    # After cleaning, call the function to insert into the database.
    # insert_into_db(cleaned_data)

def monitor_folder():
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_to_watch, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    monitor_folder()
