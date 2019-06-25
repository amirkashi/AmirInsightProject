from constantValues.loginInfo import AwsLogins
from database.databaseConnection import dataBaseConnect

import boto3


class createStationTable:

   
    def addDataToTable(self):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        client = boto3.client('s3')
        connectAWS = AwsLogins()
        bucket = connectAWS.s3BucketClimate
        fileName = "ghcnd-stations.txt"
        lines = client.get_object(Bucket=bucket, Key=fileName)['Body'].read().decode('utf-8')
        lines = lines.splitlines()
        print(len(lines))
        for l in lines:
            print(l)

if __name__ == '__main__':
    stations = createStationTable()
    #stations.createTable()

