import mysql.connector

connection = mysql.connector.connect(host="njanus.eastus2.cloudapp.azure.com", user="root", password="Password", database="events")

db_cursor = connection.cursor()

db_cursor.execute('''
          DROP TABLE location, waypoint
          ''')

connection.commit()
connection.close()
