import connexion
from connexion import NoContent
import json
import requests
import yaml
import logging
import logging.config
import datetime
from pykafka import KafkaClient
import os
import time

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info(f"App Conf File: {app_conf_file}")
logger.info(f"Log Conf File: {log_conf_file}")

def report_gps_location_reading(body):
    """Returns current GPS location reading of a given user"""
    
    logger.info(f"Received event report_gps_location_reading request with a unique id of {body['device_id']}")

    producer = topic.get_sync_producer()

    msg = { "type": "location",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": body }

    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(f"Returned event report_gps_location_reading response {body['device_id']} with status 201")

    return NoContent, 201


def report_gps_waypoint_location(body):
    """Returns all waypoint location readings of a given user"""
    
    logger.info(f"Received event report_gps_waypoint_location request with a unique id of {body['device_id']}")

    producer = topic.get_sync_producer()

    msg = { "type": "waypoint",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": body }

    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(f"Returned event report_gps_waypoint_location response {body['device_id']} with status 201")

    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    """Main function"""
    max_retries = int(app_config['events']['max_retries'])
    sleep_time = int(app_config['events']['sleep_time'])
    current_retry = 0

    while current_retry < max_retries:
        try:
            logger.info(f"Trying to connect to Kafka... (Attempt: {current_retry} of {max_retries})")
            client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
            topic = client.topics[str.encode(f"{app_config['events']['topic']}")]
            logger.info("Kafka connection established")
            break
        except Exception:
            logger.error("Failed to connect to Kafka")
            time.sleep(sleep_time)
            current_retry += 1

    app.run(port=8080)
