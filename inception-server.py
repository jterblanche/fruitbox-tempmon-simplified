import logging
import socketserver
import sys
import os
import json
from datetime import datetime, timezone
from google.cloud import pubsub_v1
import argparse
import signal

CONFIG_FILE = "/etc/templogger/config.json"

CONFIG_ERROR = 2
GENERAL_ERROR = 1

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("inception-server")


def validate_gcp_config(gcp_config):
    """
    Validates the provided Google Cloud Platform (GCP) configuration dictionary.
    This function checks if the required keys are present in the GCP configuration.
    If any required key is missing, it logs an error and raises a KeyError.
    Args:
        gcp_config (dict): A dictionary containing GCP configuration details.
    Returns:
        bool: True if the GCP configuration is valid.
    Raises:
        KeyError: If any of the required keys ("project_id", "pubsub_topic", 
                 "service_account_key_path") are missing from the configuration.
    """
    
    required_keys = ["project_id", "pubsub_topic", "service_account_key_path"]
    for key in required_keys:
        if key not in gcp_config:
            logger.error(f"GCP configuration missing required key: {key}")
            raise KeyError(f"GCP configuration missing required key: {key}")
    return True

def read_config_from_json(json_file):
    """
    Reads configuration data from a JSON file and validates its structure.
    Args:
        json_file (str): The path to the JSON configuration file.
    Returns:
        dict or None: A dictionary containing the configuration data if the file is valid and contains
        the required fields. Returns None if the file does not exist, is invalid, or is missing required fields.
    Logs:
        - Logs an informational message when attempting to read the configuration file.
        - Logs an error message if the file does not exist.
        - Logs an error message if required fields are missing in the JSON file.
        - Logs an exception message if an error occurs during file reading or parsing.
    Required JSON Structure:
        {
            "gcp": {
                "project_id": "<GCP Project ID>",
                "pubsub_topic": "<GCP Pub/Sub Topic>",
                "service_account_key_path": "<Path to Service Account Key>"
            },
        }
    """
    try:
        logger.info(f"Reading configuration from {json_file}")
        # Check if the JSON file exists
        if not os.path.exists(json_file):
            logger.error(f"Configuration file {json_file} does not exist.")
            return None
        
        # Read the configuration from a JSON file
        config = {}
        with open(json_file) as f:
            config = json.load(f)

        # Validate the GCP configuration
        try:
            validate_gcp_config(config['gcp'])
        except KeyError:
            logger.error("Invalid GCP configuration.")
            return None
                
        return config
    except Exception:
        logger.exception(f"Error reading configuration from JSON file {json_file}")
        return None

   
def create_pubsub_client(project_id, pubsub_topic, service_account_key_path=None):
    """
    Creates a Google Cloud Pub/Sub client and constructs the topic path.
    Args:
        project_id (str): The Google Cloud project ID.
        pubsub_topic (str): The name of the Pub/Sub topic.
    Returns:
        tuple: A tuple containing:
            - publisher (google.cloud.pubsub_v1.PublisherClient): The Pub/Sub publisher client instance.
            - topic_path (str): The fully qualified topic path.
        If an error occurs, returns (None, None).
    Raises:
        Exception: Logs and handles any exceptions that occur during the creation of the Pub/Sub client.
    """

    if not project_id:
        logger.error("Cannot create Pub/Sub client. Project ID is missing.")
        return None, None
    
    if not pubsub_topic:
        logger.error("Cannot create Pub/Sub client. Pub/Sub topic is missing.")
        return None, None
    
    if not service_account_key_path:
        logger.error("Cannot create Pub/Sub client. Service account key path is missing.")
        return None, None

    try:
        # Create a Pub/Sub client using the provided service account key path.
        publisher = pubsub_v1.PublisherClient.from_service_account_json(service_account_key_path)
        topic_path = publisher.topic_path(project_id, pubsub_topic)
        return publisher, topic_path
    except Exception as e:
        logger.exception(f"Error creating Pub/Sub client: {e}")
        return None, None

class TCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        client_ip = self.client_address[0]        
        logger.info(f"Connection from {client_ip}")
        
        try:
            # Initialize Pub/Sub client once per server instance
            pubsub_client = self.server.publisher_params['pubsub_client']
            topic = self.server.publisher_params['topic']
            timeout = self.server.publisher_params.get('timeout', 30)
            
            # Set socket timeout (in seconds)
            self.request.settimeout(timeout)

            while True:
                try:
                    data = self.request.recv(1024).decode('utf-8')
                    if not data:
                        break

                    logger.info(f"Received data: {data}")

                    dt = datetime.now(timezone.utc)

                    pubsub_data = {
                        # Timestamp in a format that BigQuery will eventually accept
                        'timestamp': dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        'notification_text': data
                    }
                    
                    # Publish to Pub/Sub
                    future = pubsub_client.publish(
                        topic,  # Use the topic variable, not self.topic_path
                        data=json.dumps(pubsub_data).encode('utf-8'),
                        client_ip=client_ip
                    )
                    message_id = future.result()

                    if not message_id:
                        logger.error("Failed to publish message to Pub/Sub.")
                    else:
                        logger.info(f"Published message ID: {message_id}")
                                    
                except (ConnectionResetError, BrokenPipeError) as e:
                    logger.warning(f"Connection error: {str(e)}")
                    break
                except Exception as e:
                    logger.error(f"Publishing error: {str(e)}")
                    break
        except Exception as e:
            logger.error(f"Pub/Sub initialization failed: {str(e)}")
            return
        finally:
            self.request.close()
            logger.info(f"Connection closed for {client_ip}")

def run_server():
    try:
        # Check whether we've been called with a --verbose or --debug flag.
        parser = argparse.ArgumentParser(description="Temperature Logger Application")
        parser.add_argument('--verbose', action='store_true', help="Enable verbose logging")
        parser.add_argument('--debug', action='store_true', help="Enable debug logging")
        args = parser.parse_args()

        if args.debug or args.verbose:
            LOG_LEVEL = logging.DEBUG
        else:
            LOG_LEVEL = logging.INFO

        logger.setLevel(LOG_LEVEL)
        logger.info(f"Log level set to {LOG_LEVEL}")

        # Read configuration from JSON file
        config = read_config_from_json(CONFIG_FILE)
        if config is None:
            logger.error("Failed to read configuration from JSON file.")
            sys.exit(CONFIG_ERROR)

        gcp_config = config.get('gcp', {})
        
        # Create PubSub client once at server startup
        pubsub_client, topic = create_pubsub_client(
            gcp_config.get('project_id'),
            gcp_config.get('pubsub_topic'),
            gcp_config.get('service_account_key_path')
        )
        
        if pubsub_client is None or topic is None:
            logger.error("Failed to create Pub/Sub client.")
            sys.exit(CONFIG_ERROR)
        
        # Create server with shared parameters
        with socketserver.ThreadingTCPServer(('0.0.0.0', 8080), TCPHandler) as server:
            server.publisher_params = {
                'pubsub_client': pubsub_client,
                'topic': topic,
                'timeout': config.get('timeout', 30)  # Default to 30 seconds if not specified
            }
            
            # Set up signal handlers
            def signal_handler(sig, frame):
                logger.info("Shutdown signal received, exiting...")
                server.shutdown()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            server.serve_forever()
            
    except Exception as e:
        logger.error(f"Server startup failed: {str(e)}")
        sys.exit(GENERAL_ERROR)

if __name__ == '__main__':
    run_server()