import connexion
from connexion import NoContent
import json
import requests
import yaml
import logging
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
import os.path
from os import path
import datetime
from flask_cors import CORS, cross_origin


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

def get_stats():
    """Returns current statistics for events"""

    logger.info(f"Statistic request started")

    # Get current statistics from file if exists
    if path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            try:
                current_stats = json.loads(f.read())
                f.close()
            except Exception:
                f.close()
                logger.error(f"Data file does not exist")
                return "Statistics do not exist", 404
    
    else:
        logger.error(f"Data file does not exist")
        return "Statistics do not exist", 404

    logger.debug(f"Loaded statistic contents {current_stats}")
    logger.info(f"Statistic request completed")

    return current_stats, 200

def populate_stats():
    """Periodically update stats"""
    
    logger.info("Start Periodic Processing")

    # Get current statistics from file if exists
    if path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            try:
                current_stats = json.loads(f.read())
                f.close()
            except Exception:
                f.close()
    else:
        current_stats = None

    # Set the last updated time
    try:
        last_updated = current_stats['last_updated']
    except (NameError, KeyError, ValueError, TypeError):
        last_updated = '2000-02-11 10:08:04.989414'

    current_datetime = datetime.datetime.now()

    # Create GET requests for each event
    location_response = requests.get(f"{app_config['eventstore']['url']}/readings/location?timestamp={last_updated}")
    waypoint_response = requests.get(f"{app_config['eventstore']['url']}/readings/waypoint?timestamp={last_updated}")
    
    # Log any error status codes returned
    for response in [location_response, waypoint_response]:
        if response.status_code != 200:
            logger.error(f"{str(response)} returned {response.status_code} code")

    location_events = json.loads(location_response.text)
    waypoint_events = json.loads(waypoint_response.text)

    # Log total amount of events returned
    logger.info(f"Received {len(location_events) + len(waypoint_events)} event(s)")


    # All events in one list for min and max latitude/longitude calculations
    events = location_events + waypoint_events

    latitudes = []
    longitudes = []
    for event in events:
        latitudes.append(event['latitude'])
        longitudes.append(event['longitude'])


    # Update statistics
    updated_stats = {}

    updated_stats['num_location_readings'] = len(location_events) if current_stats is None or 'num_location_readings' not in current_stats else current_stats['num_location_readings'] + len(location_events)
    updated_stats['num_waypoint_readings'] = len(waypoint_events) if current_stats is None or 'num_waypoint_readings' not in current_stats else current_stats['num_waypoint_readings'] + len(waypoint_events)
    updated_stats['max_latitude'] = 0 if len(latitudes) == 0 else max(latitudes)
    updated_stats['min_latitude'] = 0 if len(latitudes) == 0 else min(latitudes)
    updated_stats['max_longitude'] = 0 if len(longitudes) == 0 else max(longitudes)
    updated_stats['min_longitude'] = 0 if len(longitudes) == 0 else min(longitudes)
    updated_stats['last_updated'] = str(current_datetime)

    # Write updated statistics to file
    with open(app_config['datastore']['filename'], 'w') as f:
        f.write(json.dumps(updated_stats, indent=4))
        f.close()

    logger.debug(f"Updated statistic data file with {updated_stats}")

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval', seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'


if __name__ == "__main__":
    """Main function"""
    init_scheduler()
    app.run(port=8100, use_reloader=False)
