"""
This code is responsible to read csv file of elevations for each 
states, create a table for it, and then insert elevation data in it. 
"""

from constantValues.loginInfo import AwsLogins
from databaseConnection import dataBaseConnect
import boto3
import csv

class createStatesElevationTable:

    def createTable(self):
        """
        This function creates a table for elevation of each states
        """
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        stationTable = '''
                    DROP TABLE IF EXISTS states_elevation;
                    CREATE TABLE states_elevation (
                        id serial PRIMARY KEY,
                        state VARCHAR(5) NOT NULL,
                        elevation real NOT NULL
                    );
                    '''
        cursor.execute(stationTable)
        print("state elevation table is created")
        newConection.commit()
        cursor.close()
        newConection.close() 


    def addDataToTable(self):
        """
        This function read csv file of elevation and then insert its 
        data to table in database
        """
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        client = boto3.client('s3')
        connectAWS = AwsLogins()
        bucket = connectAWS.s3Bucket
        fileName = "states_elevation_csv/lower48elevation.csv"
        lines = client.get_object(Bucket=bucket, Key=fileName)['Body'].read().decode('utf-8').split()
        for row in lines:
           row = row.split(',')
           state_elevation_row = '''
                                 INSERT INTO states_elevation(state, elevation)
                                 VALUES(%s,%s)
                                 '''
           record_to_insert = (row[0], float(row[1]))
           cursor.execute(state_elevation_row, record_to_insert)
        newConection.commit()
        cursor.close()
        newConection.close()
        print("Done!")

if __name__ == '__main__':
    stations = createStatesElevationTable()
    stations.createTable()
    stations.addDataToTable()

