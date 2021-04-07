import connexion
from connexion import NoContent
import json

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from base import Base
from location import Location
from waypoint import Waypoint
import datetime

import mysql.connector
import pymysql
import logging
import logging.config
import yaml

from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

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

DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['datastore']['user']}:{app_config['datastore']['password']}@{app_config['datastore']['hostname']}:{app_config['datastore']['port']}/{app_config['datastore']['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def get_gps_location_readings(start_timestamp, end_timestamp):
    """Gets new gps location readings after the timestamp"""
    
    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S.%f")

    readings = session.query(Location).filter(and_(Location.date_created >= start_timestamp_datetime, Location.date_created < end_timestamp_datetime))

    results_list = []

    for reading in readings:
        results_list.append(reading.to_dict())

    session.close()

    logger.info(f"Query for Location readings after {start_timestamp} returns {len(results_list)}")

    return results_list, 200


def get_gps_waypoint_locations(start_timestamp, end_timestamp):
    """Gets new waypoint location readings after the timestamp"""
    
    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S.%f")

    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S.%f")

    readings = session.query(Waypoint).filter(and_(Waypoint.date_created >= start_timestamp_datetime, Waypoint.date_created < end_timestamp_datetime))

    results_list = []

    for reading in readings:
        results_list.append(reading.to_dict())

    session.close()

    logger.info(f"Query for Waypoint readings after {start_timestamp} returns {len(results_list)}")

    return results_list, 200
    

def process_messages():
    """ Process event messages """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"

    max_retries = int(app_config['events']['max_retries'])
    sleep_time = int(app_config['events']['sleep_time'])
    current_retry = 0

    while current_retry < max_retries:
        try:
            logger.info(f"Trying to connect to Kafka... (Attempt: {current_retry} of {max_retries})")
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config['events']['topic'])]
            logger.info("Kafka connection established")
            break
        except Exception:
            logger.error(f"Failed to connect to Kafka")
            time.sleep(sleep_time)
            current_retry += 1

    # Create a consume on a cosnumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't)
    # read all the old messages from the history in the message queue).
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                        reset_offset_on_start=False,
                                        auto_offset_reset=OffsetType.LATEST)

    # This is for blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info(f"Message: {msg}")

        payload = msg["payload"]

        if msg["type"] == "location":
            # Store the event1
            session = DB_SESSION()

            location = Location(payload['user_id'],
                                payload['device_id'],
                                payload['latitude'],
                                payload['longitude'],
                                payload['timestamp'])

            session.add(location)

            session.commit()
            session.close()

            logger.debug(f"Stored event report_gps_location_reading request with a unique id of {payload['device_id']}")

        elif msg["type"] == "waypoint":
            # Store the event2
            session = DB_SESSION()

            waypoint = Waypoint(payload['user_id'],
                                payload['device_id'],
                                payload['name'],
                                payload['latitude'],
                                payload['longitude'],
                                payload['timestamp'])

            session.add(waypoint)

            session.commit()
            session.close()

            logger.debug(f"Stored event report_gps_waypoint_location request with a unique id of {payload['device_id']}")

        # Commit the new message as being read
        consumer.commit_offsets()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    """Main function"""
    logger.info(f"Connecting to database... Hostname:{app_config['datastore']['hostname']}, Port:{app_config['datastore']['port']}")
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
