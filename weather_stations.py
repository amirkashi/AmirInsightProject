from constantValues.loginInfo import AwsLogins
from databaseConnection import dataBaseConnect
import boto3


class createStationTable:


    def createStationsTable(self):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        stationTable = '''
                    DROP TABLE IF EXISTS stations;
                    CREATE TABLE stations (
                        id serial PRIMARY KEY,
                        station_name VARCHAR(20) UNIQUE NOT NULL,
                        latitude real NOT NULL,
                        longitude real NOT NULL,
                        elevation real NOT NULL,
                        state VARCHAR(20) NOT NULL,
                        name VARCHAR(100) NOT NULL
                    );
                    '''
        cursor.execute(stationTable)
        print("station table created")
        newConection.commit()
        cursor.close()
        newConection.close() 


    def addDataToTable(self):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        client = boto3.client('s3')
        connectAWS = AwsLogins()
        bucket = connectAWS.s3BucketClimate
        fileName = "ghcnd-stations.txt"
        lines = client.get_object(Bucket=bucket, Key=fileName)['Body'].read().decode('utf-8')
        lines = lines.splitlines()
        for line in lines: 
            if line[0:2] == 'US':
                stationName = line[42:72]
                stationMetaData = [x for x in line[0:41].strip().split(' ') if x!='']
                if stationMetaData[-1] != 'AK' and stationMetaData[-1] != 'HI' and float(stationMetaData[-2]) > 0:
                    station = '''
                             INSERT INTO stations (station_name, latitude, longitude, elevation, state, name)
                             VALUES(%s,%s,%s,%s,%s,%s);
                             '''
                    record_to_insert = (stationMetaData[0], float(stationMetaData[1]), float(stationMetaData[2]),\
                                        float(stationMetaData[3]), stationMetaData[4], stationName)
                    cursor.execute(station, record_to_insert)
        newConection.commit()
        cursor.close()
        newConection.close()
        print("Done!")

if __name__ == '__main__':
    stations = createStationTable()
    stations.createStationsTable()
    stations.addDataToTable()
