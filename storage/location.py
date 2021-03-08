from sqlalchemy import Column, Integer, Float, String, DateTime
from base import Base
import datetime


class Location(Base):
    """ Location """

    __tablename__ = "location"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(250), nullable=False)
    device_id = Column(String(250), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)

    def __init__(self, user_id, device_id, latitude, longitude, timestamp):
        """ Initializes a location reading """
        self.user_id = user_id
        self.device_id = device_id
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp = timestamp
        self.date_created = datetime.datetime.now()

    def to_dict(self):
        """ Dictionary Representation of a location reading """
        dict = {}

        dict['id'] = self.id
        dict['user_id'] = self.user_id
        dict['device_id'] = self.device_id
        dict['latitude'] = self.latitude
        dict['longitude'] = self.longitude
        dict['timestamp'] = self.timestamp
        dict['date_created'] = self.date_created

        return dict
