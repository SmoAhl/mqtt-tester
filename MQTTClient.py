import sys
import paho.mqtt.client as mqtt
import datetime
import json
import logging
import ssl
import random
import time
import signal
from queue import Queue
import threading
from dotenv import load_dotenv
from Report import LoadTestReport
load_dotenv()
logger = logging.getLogger(__name__)

    # Connects to MQTT Broker to publish messages to topic and receives messages from topic.
    # Calculates the message delay time in the process.
    # Saves the results to a database from every run and generates a visual report.

class MQTTClient:
    def __init__(self, args, db):
        self.args = args
        self.db = db
        self.q = Queue() # Stores sent and received message times.
        self.client = self.initialize_client()
        self.connected_event = threading.Event() # Message interval threading.
        self.sent_message_ids = set() # Keep track of message ID.
        self.received_message_ids = set() # Keep track of message ID.
        self.timed_out_message_ids = set()
        self.lock = threading.Lock()  # Lock for thread-safe operations on shared resources.
        self.timers = {}  # Initialize the timers dictionary to manage message timeouts.
        self.publish_times = {}  # Datetimes.

    # MQTT Client Initialization and Connection Management based on chosen protocol.
    def initialize_client(self):
        client_id_with_random = f"{self.args.client_id}{random.randint(100, 999)}" #Generate unic ID.
        if self.args.protocol == 'mqtt':
            client = mqtt.Client(client_id_with_random)
        elif self.args.protocol == 'ws':
            client = mqtt.Client(client_id_with_random, transport='websockets')
            client.ws_set_options(path="/")
        else:
            self.exit_with_message(f"Unsupported protocol: {self.args.protocol}")
        #Check SSL verification needs
        self.ssl_check()
        if self.args.ssl_enabled:
            logging.info(f"SSL/TLS is disabled for {self.args.protocol} in test use.")
            client.tls_set(cert_reqs=ssl.CERT_NONE) #Bypass certificate verification in test use.

        # Broker callback and subscribe
        client.on_connect = self.on_connect
        # The callback called when a message has been received on a topic that the client subscribes to.
        client.on_message = self.on_message
        return client

    # Check protocol and SSL
    def ssl_check(self):
        if self.args.protocol == 'mqtt':
            if self.args.port == 8883:
                if not self.args.ssl_enabled:
                    self.exit_with_message("SSL must be enabled for MQTT on port 8883. Please use --ssl-enabled true.")
            else:
                if self.args.ssl_enabled:
                    self.exit_with_message("No SSL on port 1883 for MQTT. Please use --ssl-enabled false.")
        elif self.args.protocol == 'ws':
            if self.args.port == 443:
                if not self.args.ssl_enabled:
                    self.exit_with_message("SSL must be enabled for WebSocket on port 443. Please use --ssl-enabled true.")
            else:
                if self.args.ssl_enabled:
                    self.exit_with_message("No SSL on port 80 for WebSocket. Please use --ssl-enabled false.")
        else:
            self.exit_with_message(f"Unsupported protocol: {self.args}")
    
    # Connect to a remote broker and subscribe.
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected successfully to the broker.")
            client.subscribe(self.args.topic, 0)
            self.connected_event.set() # send_messages_loop waits for connection.
        else:
            logging.error(f"Failed to connect to the broker with return code {rc}")
 
    # Sends messages repeatedly at defined intervals.
    def send_messages_loop(self):
        self.connected_event.wait()  # Wait until the client is connected and subscribed.
        for message_index in range(1, self.args.message_count + 1):
            self.send_message(self.client, self.args.topic, message_index, self.args.data_string_length)
            time.sleep(self.args.interval)

    # Publish message to topic and sending timeout.
    def send_message(self, client, topic, message_index, data_string_length):
        try:          
            publish_datetime_utc = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds')
            data = "Lorem ipsum dolor sit amet"[:data_string_length]
            message = {
                "PublishDateTimeUTC": publish_datetime_utc,
                "SendTime": time.perf_counter(),
                "MessageIndex": message_index,
                "Data": data
            }
            json_message = json.dumps(message)
            client.publish(topic, json_message)
            self.sent_message_ids.add(message_index)
            self.publish_times[message_index] = publish_datetime_utc
            # Timeout
            timeout_seconds = self.args.timeout
            # Set up a timer for the message
            timer = threading.Timer(timeout_seconds, self.message_timeout, [message_index])
            timer.start()
            with self.lock:
                self.timers[message_index] = timer

        except Exception as e:
            logging.error(f"Failed to publish message {message_index}: {e}")
            # Directly use the captured publish datetime for logging the failure
            self.db.insert_result(message_index, publish_datetime_utc, None, None, None, failed=True)
            self.sent_message_ids.discard(message_index)

    # The callback called when a message has been received on a topic that the client subscribes to.
    # Creates a return topic for received message times.
    def on_message(self, client, userdata, message):
        receive_time = time.perf_counter()  # High-resolution timestamp on message receive.       
        message_data = json.loads(message.payload.decode())
        original_send_time = message_data["SendTime"]
        message_index = message_data["MessageIndex"]
        publish_date_time_utc = message_data["PublishDateTimeUTC"]
        self.received_message_ids.add(message_index) # Update tracking of received messages

        # Re-publish with new timestamp through /return topic.
        if "return" not in message.topic:
            new_payload = json.dumps({
                "SendTime": receive_time,
                "OriginalPayload": message.payload.decode(),
                "RoundTrip": True
            })
            client.publish(self.args.topic + "/return", new_payload)

        # Manage timers for message timeout
        with self.lock:
            if message_index in self.timers:
                self.timers[message_index].cancel()
                del self.timers[message_index]

        # Add timestamps and indexes to Queue for message processing.    
        self.q.put((publish_date_time_utc ,original_send_time, receive_time, message_index))

    # Logic to get sent/received messages from Queue.
    # Calculate message delay and add to database.
    def process_messages(self, q):
        while True:
            item = q.get()
            if item is None:  # Exit signal
                break
            publish_date_time_utc ,original_send_time, receive_time, message_index = item
            delay = (receive_time - original_send_time) * 1000  # Convert delay to milliseconds.
            self.db.insert_result(message_index, publish_date_time_utc, original_send_time, receive_time, delay, failed=False) # Add to database.

    # If a message timeout occurs.
    def message_timeout(self, message_index):
        with self.lock:
            publish_datetime_utc = self.publish_times.get(message_index, "Unknown Time")
            if message_index not in self.received_message_ids:
                logging.error(f"Timeout exceeded for message {message_index}. Marking as failed.")
                self.timed_out_message_ids.add(message_index)
                self.sent_message_ids.discard(message_index)
                self.db.insert_result(message_index, publish_datetime_utc, None, None, None, failed=True)
            if message_index in self.timers:
                self.timers[message_index].cancel()
                del self.timers[message_index]
            self.cleanup_message_index(message_index)

    # Check missing messages.
    def verify_message_integrity(self):
        missing_messages = self.sent_message_ids.difference(self.received_message_ids)
        if missing_messages:
            logging.error(f"Missing message IDs: {sorted(missing_messages)}")
            for message_index in sorted(missing_messages):
                publish_datetime_utc = self.publish_times.get(message_index, "Unknown Time")
                self.db.insert_result(message_index, publish_datetime_utc, None, None, None, failed=True)
                self.cleanup_message_index(message_index)
                
    # Datetime index cleanup
    def cleanup_message_index(self, message_index):
        with self.lock:
            if message_index in self.publish_times:
                del self.publish_times[message_index]

    # CTRL + C
    def handle_signal(self, signal_number, frame):
        print("Signal received, initiating graceful shutdown...")
        self.client.disconnect()  # Disconnect from the MQTT broker
        sys.exit(0)

    # Start non-blocking connection to broker
    # Start threading sent and received messages
    def connect_and_loop(self):
        signal.signal(signal.SIGINT, self.handle_signal) # CTRL + C.
        try:
            self.client.username_pw_set(self.args.username, self.args.password)
            self.client.connect(self.args.host, self.args.port, 60)
            self.client.loop_start()
            # Starting print for user.
            start_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"Beginning loadtest at {start_time}: Sending {self.args.message_count} messages with {self.args.interval} second interval.")

            # Message publish intervall.
            self.sender_thread = threading.Thread(target=self.send_messages_loop)
            self.sender_thread.start()
            
            # Published and subscribed Queue threading.
            processing_thread = threading.Thread(target=self.process_messages, args=(self.q,))
            processing_thread.start()

        except Exception as e:
            logging.error(f"An error occurred: {e}")

        finally:
            # Wait for sending thread to complete --messsage-count.
            self.sender_thread.join()
            print()
            logging.info("Sender thread completed.")

            # End thread after all pubs/subs processed from Queue.
            self.q.put(None)
            processing_thread.join()
            logging.info("Queue processing thread completed.")            
            # Check for any missing messages before stopping everything
            self.verify_message_integrity()

            # Stop MQTT loop and disconnect
            with self.lock:
                for timer in self.timers.values():
                    timer.cancel()
            self.client.loop_stop()
            self.client.disconnect()
            logging.info("Disconnected from MQTT broker and cleaned up resources.")

            # Report and summary
            self.generate_report(self.db.db_file)
            successful_messages = len(self.sent_message_ids)
            failed_messages = self.args.message_count - successful_messages - len(self.timed_out_message_ids)
            timeout_messages = len(self.timed_out_message_ids)

            # Display summary
            end_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"Ending loadtest at {end_time} - Sent {successful_messages} messages successfully, {failed_messages} failed and {timeout_messages} timeout messages.")
            logging.info("All messages processed. Exiting...")

    def generate_report(self, db_path):
        report_generator = LoadTestReport(db_path)
        report_generator.generate_charts_and_tables()

    def exit_with_message(self, message):
        logging.error(message)
        exit(1)