import os
import logging
import argparse
from MQTTClient import MQTTClient
from SQLiteDB import SQLiteDB
from dotenv import load_dotenv
load_dotenv()

    # This script sets up and runs a MQTT client to send messages to an MQTT broker and stores message delay results in a SQLite database.
    # Ensure all required environment variables are set in your .env file or are available in your environment.
    # Use the provided command-line arguments to override any default settings or environment variables.

def main():
    #Command-Line Argument Configuration.
    parser = argparse.ArgumentParser(description="Use the provided command-line arguments to override any default settings or environment variables.")
    parser.add_argument("--username", type=str, default=os.getenv('MQTT_TESTERI_USERNAME'), help="Give an username")
    parser.add_argument("--password", type=str, default=os.getenv('MQTT_TESTERI_PASSWORD'), help="Give a password")
    parser.add_argument("--host", type=str, default=os.getenv('MQTT_TESTERI_HOST'), help="Give an adress")
    parser.add_argument("--client-id", type=str, default=os.getenv('MQTT_TESTERI_CLIENT_ID'), help="Give a Client ID")
    parser.add_argument("--topic", type=str, default=os.getenv('MQTT_TESTERI_TOPIC'), help="Subscribe topic")
    parser.add_argument("--port", type=int, default=int(os.getenv('MQTT_TESTERI_PORT', '1883')), help="Give a port number")
    parser.add_argument("--ssl-enabled", type=str_to_bool, default=os.getenv('MQTT_TESTERI_SSL_ENABLED'), help="Enable SSL: True/False")
    parser.add_argument("--ssl-verify-certificate", type=str_to_bool, default=os.getenv('MQTT_TESTERI_SSL_VERIFY_CERTIFICATE'), help="Verify SSL certificate: True/False") 
    parser.add_argument("--protocol", type=str, default=os.getenv('MQTT_TESTERI_PROTOCOL', 'mqtt'), help="Protocol to use: 'mqtt' or 'ws'")
    parser.add_argument("--verbose", type=str_to_bool, default=False, help="Enable or disable verbose logging output (true/false).") # Debug logging ON/OFF.
    # Message configuration and verbose.
    parser.add_argument("--message-count", type=int, default=10, help="Number of messages to send.")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between messages in seconds.")
    parser.add_argument("--data-string-length", type=int, default=55, help="Length of the data string to send.")
    parser.add_argument("--timeout", type=int, default=60, help=("Sets the maximum number of seconds to wait between messages to be sent and received before timing out." 
                        "This helps prevent indefinite hangs if the network or broker becomes unresponsive during load testing."))
    args = parser.parse_args()
    configure_logging(args.verbose)

    # Initialize SQLite database.
    db = SQLiteDB()
    # Initialize MQTT client.
    mqtt_client = MQTTClient(args, db)
    mqtt_client.connect_and_loop()

    # Verbose settings.
def configure_logging(verbose):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

# Logic to use true/false in Command-Line Argument Configuration.
def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif value.lower() in ('no', 'false', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
    
if __name__=="__main__":
    main()