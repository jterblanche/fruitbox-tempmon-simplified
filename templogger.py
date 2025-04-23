import os
import glob
import time
import json
from datetime import datetime, timezone
import logging
import sys
from google.cloud import pubsub_v1
from typing import Optional
import argparse

# Path to the sensor data file
CONFIG_FILE = "/etc/templogger/config.json"
TIMEOUT_SECONDS = 10

CONFIG_ERROR = 2
SENSOR_DIR_ERROR = 3
GENERAL_ERROR = 1

def create_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Creates and configures a logger with the specified name and logging level.
    Args:
        name (str): The name of the logger.
        level (int, optional): The logging level (e.g., logging.INFO, logging.DEBUG). 
            Defaults to logging.INFO.
    Returns:
        logging.Logger: A configured logger instance.
    """

    # If logger is already created, return it
    if name in logging.Logger.manager.loggerDict:
        return logging.getLogger(name) 

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

logger = create_logger("templogger", logging.INFO)

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
            "sensor_base_dir": "<Base Directory for Sensors>"
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
        
        # Check if the sensor base directory is present
        if 'sensor_base_dir' not in config:
            logger.error("Sensor base directory not found in JSON file.")
            return None
        
        return config
    except Exception:
        logger.exception(f"Error reading configuration from JSON file {json_file}")
        return None



def read_temp_raw(device_file: str) -> list[str]:
    """
    Reads raw temperature data from a specified device file.
    Args:
        device_file (str): The path to the device file containing temperature data.
    Returns:
        list of str: A list of strings, where each string is a line of raw data read from the device file.
    """

    with open(device_file, 'r') as f:
        lines = f.readlines()
    return lines

def read_temp(device_folder: str) -> Optional[float]:
    """
    Reads the temperature from a 1-Wire device file.
    This function reads raw temperature data from a device file located in the
    specified device folder. It processes the data to extract the temperature
    in Celsius. If the data is invalid or a timeout occurs, it logs an error
    and returns None.
    Args:
        device_folder (str): The path to the folder containing the device file.
    Returns:
        float: The temperature in Celsius if successfully read and parsed.
        None: If an error occurs or the data is invalid.
    Logs:
        - Debug logs for reading the device file and raw temperature data.
        - Error logs for issues such as invalid data or timeout.
    """

    device_file = os.path.join(device_folder, 'w1_slave')
    logger.debug(f"Reading temperature from device file: {device_file}")
    
    lines = read_temp_raw(device_file)
    if not lines:
        logger.error(f"Error reading temperature from device file {device_file}")
        return None

    timeout = time.time() + TIMEOUT_SECONDS
    while lines and not lines[0].strip().endswith('YES'):
        if time.time() > timeout:
            logger.error(f"Timeout while waiting for valid temperature data from {device_file}")
            return None
        time.sleep(0.2)
        lines = read_temp_raw(device_file)
        logger.debug(f"Raw temperature data: {lines}")

    if not lines or len(lines) < 2:
        logger.error(f"Invalid temperature data from device file {device_file}")
        return None
    
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        try:
            temp_c = float(temp_string) / 1000.0
        except ValueError:
            logger.error(f"Invalid temperature value: {temp_string}")
            return None

        return temp_c
    else:
        logger.error(f"Error reading temperature from device file {device_file}")
        return None

def check_temperature_reading_validity(temperature: Optional[float]) -> bool:
    """
    Validates the given temperature reading.
    This function checks if the provided temperature reading is valid by ensuring:
    - The temperature is not None.
    - The temperature is of type int or float.
    - The temperature is within the acceptable range of -50 to 150 degrees.
    If any of these conditions are not met, an error is logged, and the function returns False.
    Otherwise, it returns True.
    Args:
        temperature (int | float | None): The temperature reading to validate.
    Returns:
        bool: True if the temperature reading is valid, False otherwise.
    """

    if temperature is None:
        logger.error("Temperature reading is None.")
        return False
    if not isinstance(temperature, (int, float)):
        logger.error(f"Temperature reading is not a number: {temperature}")
        return False
    if temperature < -50 or temperature > 150:
        logger.error(f"Temperature reading out of range: {temperature}")
        return False
    return True
    
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

def convert_unix_timestamp_to_bigquery_timestamp(timestamp_to_convert):
    """
    Converts a Unix timestamp to a BigQuery-compatible timestamp string.
    Args:
        timestamp_to_convert (float): The Unix timestamp to convert. This is the number of seconds 
                           since the Unix epoch (January 1, 1970, 00:00:00 UTC).
    Returns:
        str: A string representing the timestamp in BigQuery format 
             (e.g., "YYYY-MM-DDTHH:MM:SS.ssssssZ").
    """

    dt = datetime.fromtimestamp(timestamp_to_convert, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def convert_record_to_pubsub_message(record: dict) -> str:
    """
    Converts a sensor data record into a JSON-formatted Pub/Sub message.

    The function takes a dictionary containing sensor data and converts it 
    into a JSON string that adheres to the specified Pub/Sub schema.

    Pub/Sub Schema:
        The Pub/Sub Schema for the message is as follows:
        {
            "type": "record",
            "name": "SensorData",
            "fields": [
                {
                "name": "timestamp",
                "type": "string",
                },
                {
                "name": "sensor_id",
                "type": "string"
                },
                {
                "name": "reading",
                "type": "double"
                }
            ]
        }

    Args:
        record (dict): A dictionary containing the sensor data with the following keys:
            - "timestamp" (string): The timestamp of the reading in a format that BigQuery can eventually accept.
            - "sensor_id" (str): The unique identifier of the sensor.
            - "reading" (float): The sensor reading value.

    Returns:
        str: A JSON-formatted string representing the Pub/Sub message.

    Raises:
        KeyError: If any of the required keys ("timestamp", "sensor_id", "reading") 
                  are missing from the input dictionary.
        TypeError: If the input is not a dictionary.
    """
    required_keys = ["timestamp", "sensor_id", "reading"]
    for key in required_keys:
        if key not in record:
            raise KeyError(f"Missing required key: {key}")

    message = {
        "timestamp": record['timestamp'],
        "sensor_id": record['sensor_id'],
        "reading": record['reading']
    }
    return json.dumps(message)

def is_pubsub_config_valid(pubsub_client, topic_path):
    """
    Validates the configuration for a Pub/Sub client and topic path.
    Args:
        pubsub_client: The Pub/Sub client instance to be validated. 
                       Should not be None.
        topic_path: The Pub/Sub topic path as a string. 
                    Should not be empty or None.
    Returns:
        bool: True if both the Pub/Sub client and topic path are valid, 
              False otherwise.
    Logs:
        Logs an error message if the Pub/Sub client is not initialized 
        or if the topic path is not set.
    """

    if not pubsub_client:
        logger.error("Pub/Sub client is not initialized.")
        return False
    if not topic_path:
        logger.error("Pub/Sub topic path is not set.")
        return False
    return True

def publish_to_pubsub(publisher, topic_path, record):
    """
    Publishes a message to a Google Cloud Pub/Sub topic.
    Args:
        publisher (google.cloud.pubsub_v1.PublisherClient): The Pub/Sub publisher client instance.
        topic_path (str): The fully qualified Pub/Sub topic path (e.g., "projects/{project_id}/topics/{topic_name}").
        record (dict): The record to be published, which will be converted to a Pub/Sub message.
    Returns:
        dict: The JSON-decoded representation of the published message if successful.
        None: If an error occurs during publishing.
    Raises:
        Exception: Logs any exceptions that occur during the publishing process.
    Notes:
        - The `convert_record_to_pubsub_message` function is used to transform the record into
          a Pub/Sub-compatible message format.
    """

    try:
        try:
            converted_message = convert_record_to_pubsub_message(record)
        except KeyError as e:
            logger.error(f"Error converting record to Pub/Sub message: {e}")
            return None
        
        logger.debug(f"Publishing message {converted_message} to Pub/Sub topic {topic_path}")
        data = converted_message.encode("utf-8")

        retries = 3
        delay = 1  # Initial delay in seconds
        for attempt in range(retries):
            try:
                future = publisher.publish(topic_path, data=data)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 5  # Incremental backoff
        else:
            logger.error("All retry attempts to publish message to Pub/Sub failed.")
            return None

        try:
            message_id = future.result(timeout=30)
            logger.debug(f"Published message to PubSub with ID: {message_id}")
        except Exception as e:
            logger.error(f"Error getting Pub/Sub publish result: {e}")
            return None

        return json.loads(converted_message)
    except Exception:
        logger.exception("Error publishing message to Pub/Sub")
        return None


def get_project_id(service_account_key_path: str) -> Optional[str]:
    """
    Retrieves the project ID from a Google Cloud service account key file.
    Args:
        service_account_key_path (str): The file path to the service account key JSON file.
    Returns:
        Optional[str]: The project ID if successfully retrieved, or None if an error occurs.
    Raises:
        FileNotFoundError: If the specified file does not exist.
        KeyError: If the "project_id" key is missing in the JSON file.
        json.JSONDecodeError: If the file is not a valid JSON.
    """

    try:
        with open(service_account_key_path, "r") as f:
            data = json.load(f)
            return data["project_id"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        logger.error(f"Invalid service account key file: {e}")
        return None

def get_sensor_device_folders(base_dir):
    """
    Retrieves a list of sensor device folders from the specified base directory.
    This function searches for directories within the given base directory
    that match the pattern '28*', which is typically used for identifying
    temperature sensor devices (e.g., DS18B20 sensors). If no matching
    directories are found, an error is logged.
    Args:
        base_dir (str): The base directory to search for sensor device folders.
    Returns:
        list: A list of paths to the sensor device folders. If no folders are
              found, an empty list is returned.
    Example:
        Input: "/sys/bus/w1/devices/"
        Output: ["/sys/bus/w1/devices/28-000005e2fdc3", "/sys/bus/w1/devices/28-000005e2fdc4"]
    """

    device_folders = glob.glob(os.path.join(base_dir, '28*'))
    if not device_folders:
        logger.error(f"No sensor device folders found in {base_dir}")
    return device_folders

def log_temperatures(device_folders: list[str], pubsub_client: Optional[pubsub_v1.PublisherClient], topic_path: Optional[str]) -> None:
    """
    Logs temperature readings from a list of device folders, validates the readings, 
    and optionally publishes the data to a Pub/Sub topic.
    Args:
        device_folders (list): A list of file paths to device folders containing temperature sensors.
        pubsub_client (google.cloud.pubsub_v1.PublisherClient or None): 
            The Pub/Sub client used to publish messages. If None, publishing is skipped.
        topic_path (str or None): The Pub/Sub topic path to publish messages to. 
            If None, publishing is skipped.
    Raises:
        Exception: Logs and raises any exceptions encountered while reading temperatures 
                   or publishing messages.
    Notes:
        - Each device folder is expected to contain a readable temperature sensor.
        - The function validates the temperature reading before logging or publishing.
        - If the Pub/Sub client or topic path is not provided, the function logs an error 
          and skips publishing.
    """

    reading_timestamp = convert_unix_timestamp_to_bigquery_timestamp(time.time())

    for device_folder in device_folders:
        try:
            logger.debug(f"Reading temperature from device folder: {device_folder}")
            sensor_id = device_folder.split('/')[-1]
            temperature = read_temp(device_folder)

            if not check_temperature_reading_validity(temperature):
                logger.error(f"Invalid temperature reading {temperature} from sensor {sensor_id} in folder {device_folder}")
                continue

            logger.info(f"Sensor ID: {sensor_id}, Temperature: {temperature}°C")
            
            message = {
                "timestamp": reading_timestamp,
                "sensor_id": sensor_id,
                "reading": temperature
            }
            
            # Publish the message to Pub/Sub if the client and topic path are valid
            if is_pubsub_config_valid(pubsub_client, topic_path):
                logger.debug(f"Publishing message to Pub/Sub topic: {topic_path}")
                try:
                    publish_to_pubsub(pubsub_client, topic_path, message)
                except Exception:
                    logger.exception(f"Error publishing message to Pub/Sub topic {topic_path}")
            else:
                logger.error("Pub/Sub client or topic path is not valid. Skipping publishing.")

        except FileNotFoundError:
            logger.error(f"Device folder {device_folder} not found.")
        except PermissionError:
            logger.error(f"Permission denied to read device folder {device_folder}.")
        except OSError as e:
            logger.error(f"Error reading device folder {device_folder}: {e}")
        except Exception as e:
            logger.exception("Unexpected error")

def main():
    """
    Main function for the temperature logger application.
    This function performs the following tasks:
    1. Configures logging based on command-line arguments (`--verbose` or `--debug`).
    2. Reads the application configuration from a JSON file.
    3. Validates the sensor base directory and retrieves sensor device folders.
    4. Initializes a Google Cloud Pub/Sub client and topic.
    5. Reads temperature data from sensors and logs it to the Pub/Sub topic.
    Exits with the following status codes on error:
    - 1: General error (e.g., no sensor device folders found, Pub/Sub client creation failure).
    - 2: Configuration error (e.g., configuration file is invalid or missing).
    - 3: Sensor directory error (e.g., sensor base directory does not exist).
    Dependencies:
    - Requires a valid configuration JSON file.
    - Requires a Google Cloud service account key file for Pub/Sub authentication.
    Returns:
        None
    """

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

    # Read the configuration from the JSON file.
    config = read_config_from_json(CONFIG_FILE)

    if config is None:
        logger.error("Configuration is None. Exiting.")
        sys.exit(CONFIG_ERROR)  # Configuration error

    # Get the project ID from the service account key file
    project_id = get_project_id(config['gcp']['service_account_key_path'])
    if not project_id:
        logger.error("Project ID not found in the service account key file. Exiting.")
        sys.exit(CONFIG_ERROR)

    # Get the sensor base directory
    sensor_base_dir = config['sensor_base_dir']
    if not sensor_base_dir or not os.path.exists(sensor_base_dir):
        logger.error(f"Sensor base directory {sensor_base_dir} not found or does not exist.")
        sys.exit(SENSOR_DIR_ERROR)  # Sensor directory error
    
    # Get the sensor device folders
    device_folders = get_sensor_device_folders(sensor_base_dir)
    if not device_folders or len(device_folders) == 0:
        logger.error(f"No sensor device folders found in {sensor_base_dir}. Exiting.")
        sys.exit(SENSOR_DIR_ERROR)  # Sensor directory error
    else:
        logger.info(f"Found {len(device_folders)} sensor device folders.")

    # Create a Pub/Sub client
    pubsub_client, topic_path = create_pubsub_client(project_id, config['gcp']['pubsub_topic'], config['gcp']['service_account_key_path'])
    if not pubsub_client or not topic_path:
        logger.error("Failed to create Pub/Sub client. Exiting.")
        sys.exit(GENERAL_ERROR)

    logger.info(f"Pub/Sub client created for project {project_id} and topic {topic_path}")

    # Read and log the temperature data.
    log_temperatures(device_folders, pubsub_client, topic_path)

if __name__ == '__main__':
    main()
# vim: set filetype=python ts=4 sw=4 et: