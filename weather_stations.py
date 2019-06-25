from constantValues.loginInfo import AwsLogins
from databaseConnection import dataBaseConnect
#from constantValues.databaseConnection import dataBaseConnect

import boto3


class createStationTable:

   
    def addDataToTable(self):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        client = boto3.client('s3')
        connectAWS = AwsLogins()
        bucket = connectAWS.s3BucketClimate
        fileName = "ghcnd-stations.txt"

        #bucket = connectAWS.s3Bucket
        #fileName = "test-climate/ghcnd-stations.txt"

        lines = client.get_object(Bucket=bucket, Key=fileName)['Body'].read().decode('utf-8')
        lines = lines.splitlines()
        #line = lines[0]
        for line in lines: 
            if line[0:2] == 'US':
                #print(line[0:12], line[13:21], line[22:31], line[32:37], line[38:41], line[42:72] )
                station = '''
                         INSERT INTO stations (noaa_id, latitude, longitude, elevation, state, name)
                         VALUES(%s,%s,%s,%s,%s,%s);
                         '''
                record_to_insert = (line[0:12], float(line[13:21]), float(line[22:31]), float(line[32:37]), line[38:41], line[42:72])
                cursor.execute(station, record_to_insert)
        newConection.commit()
        cursor.close()
        newConection.close()
        print("Done!")

if __name__ == '__main__':
    stations = createStationTable()
    stations.addDataToTable()

