import connexion
from connexion import NoContent
import json
import logging
import logging.config
import yaml
from pykafka import KafkaClient
from pykafka.common import OffsetType
from flask_cors import CORS, cross_origin
import os

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

def get_gps_location_reading_index(index):
    """ Get location Reading in History """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"

    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)


    logger.info(f"Retrieving location at index {index}")
    try:
        event_index = 0
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)

            if msg['type'] == 'location':
                if event_index == index:
                    logger.info(f"Location at index {index} retrieved: {msg}")
                    return msg['payload'], 200
                else:
                    event_index += 1
        return { "message": "Not Found" }, 404
    except:
        logger.error("No more messages found")

    logger.error(f"Could not find location at index {index}")
    return { "message": "Not Found" }, 404

def get_gps_waypoint_location_index(index):
    """ Get waypoint Reading in History """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"

    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)


    logger.info(f"Retrieving waypoint at index {index}")
    try:
        event_index = 0
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)

            if msg['type'] == 'waypoint':
                if event_index == index:
                    logger.info(f"Waypoint at index {index} retrieved: {msg}")
                    return msg['payload'], 200
                else:
                    event_index += 1
        return { "message": "Not Found" }, 404
    except:
        logger.error("No more messages found")

    logger.error(f"Could not find waypoint at index {index}")
    return { "message": "Not Found" }, 404

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", base_path="/audit", strict_validation=True, validate_responses=True)

if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
    CORS(app.app)
    app.app.config['CORS_HEADERS'] = 'Content-Type'


if __name__ == "__main__":
    """Main function"""
    app.run(port=8110)
