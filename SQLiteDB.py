import datetime
import logging
import sqlite3
import sys
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

# Every run in application creates a new database.
# Table contains message index, publish datetime, high-res. published time, high-res. received time, calculated message delay and failed true/false.

class SQLiteDB:
    def __init__(self):
        timestamp =  datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.db_file = f'mqtt_testeri_results_{timestamp}.sqlite' # Add timestamp to file name.
        self.conn = self.create_connection()
        self.create_table()
        self.last_publish_index = None
        self.last_result_index = None

    def create_connection(self):
        try:
            conn = sqlite3.connect(self.db_file, check_same_thread=False) #allows the database connection to be shared across multiple threads. 
            logging.info("Connection to SQLite database successful")
            return conn
        except sqlite3.Error as e:
            logging.error(f"The error '{e}' occurred while connecting to database.")
            return None
        
    # Create publish and results tables  
    def create_table(self):
        try:
            c = self.conn.cursor()
            c.execute("BEGIN;")
            c.execute("""CREATE TABLE IF NOT EXISTS results (
                        MessageIndex INTEGER PRIMARY KEY,
                        PublishDateTimeUTC TEXT,
                        HighResPublishTime TEXT,
                        HighResSubscribeTime TEXT,
                        Delay REAL,
                        Failed INTEGER DEFAULT 0
                    );""")
            self.conn.commit()
            logging.info("Table creation successful")
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.error(f"Error creating tables: {e}")

    def insert_result(self, message_index, publish_date_time_utc, publish_time, subscribe_time, delay, failed=False):
        sql = '''INSERT INTO results (MessageIndex, PublishDateTimeUTC, HighResPublishTime, HighResSubscribeTime, Delay, Failed) VALUES (?, ?, ?, ?, ?, ?)'''
        try:
            cur = self.conn.cursor()
            cur.execute("BEGIN;")           
            # When failed is True, set time and delay fields to None, which inserts NULL in the database
            if failed:
                publish_time = None
                subscribe_time = None
                delay = None           
            # Convert delay to a formatted string if it's not None
            formatted_delay = f"{float(delay):.4f}" if delay is not None else None
            # Execute SQL with all necessary values, including the 'Failed' flag
            cur.execute(sql, (message_index, publish_date_time_utc, publish_time, subscribe_time, formatted_delay, int(failed)))
            self.conn.commit()
            self.last_result_index = message_index
            self.update_status()
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.error(f"Error inserting into results table: {e}, Last successful MessageIndex: {message_index}")


    # Updates message index on one line during the run without spamming.
    def update_status(self):
        sys.stdout.write('\r\033[K')  # Move to the beginning and clear the line
        message = f"Latest Result Insert: MessageIndex {self.last_result_index}. "
        sys.stdout.write(message)
        sys.stdout.flush()

    # Ensure that possible log messages appears on a new line during the run.
    def flush_and_log_error(self, error_message, last_index):
        sys.stdout.write('\n')  
        sys.stdout.flush()
        logger.error(f"{error_message}, Last successful MessageIndex: {last_index - 1}. " )

    def close_connection(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")