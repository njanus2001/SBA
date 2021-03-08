import mysql.connector

connection = mysql.connector.connect(host="njanus.eastus2.cloudapp.azure.com", user="root", password="Password", database="events")

db_cursor = connection.cursor()

db_cursor.execute('''
          CREATE TABLE location
          (id INT NOT NULL AUTO_INCREMENT,
          user_id VARCHAR(250) NOT NULL,
          device_id VARCHAR(250) NOT NULL,
          latitude FLOAT NOT NULL,
          longitude FLOAT NOT NULL,
          timestamp VARCHAR(100) NOT NULL,
          date_created VARCHAR(100) NOT NULL,
          CONSTRAINT location_pk PRIMARY KEY (id))
          ''')

db_cursor.execute('''
          CREATE TABLE waypoint
          (id INT NOT NULL AUTO_INCREMENT,
          user_id VARCHAR(250) NOT NULL,
          device_id VARCHAR(250) NOT NULL,
          name VARCHAR(250) NOT NULL, 
          latitude FLOAT NOT NULL,
          longitude FLOAT NOT NULL,
          timestamp VARCHAR(100) NOT NULL,
          date_created VARCHAR(100) NOT NULL,
          CONSTRAINT waypoint_pk PRIMARY KEY (id))
        ''')

connection.commit()
connection.close()