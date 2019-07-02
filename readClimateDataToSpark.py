import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
#from databaseConnection import connectToDataBase
from constantValues.loginInfo import dataBaseLogin
from databaseConnection import dataBaseConnect
from pyspark.sql import DataFrameReader

class readClimateData:

    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark").getOrCreate()

    def getStationTable(self):
        psqlConnect = dataBaseLogin()
        stationTable = self.spark.read.format("jdbc").option("url", psqlConnect.url)\
            .option("user", psqlConnect.user).option("password",psqlConnect.password)\
            .option("dbtable", "stations").load()
        return stationTable

    def getClimateDataFromS3(self):
        data_schema = StructType([StructField('station_name', StringType(), True),\
                                  StructField('date', StringType(), True), \
                                  StructField('data_type', StringType(), True),\
                                  StructField('data', DoubleType(), True)])

        climateData = self.spark.read.csv("s3a://noaa-ghcn-pds/csv/*.csv", header=False, schema=data_schema)
        climateData = climateData.groupby(climateData.station_name, climateData.date).pivot("data_type").avg("data")\
                .select(['station_name', 'date', 'TMAX', 'TMIN', 'PRCP'])\
                .withColumn('country', climateData['station_name'][0:2])
        climateData = climateData.withColumn('TMAX', climateData['TMAX']/10)
        climateData = climateData.withColumn('TMIN', climateData['TMIN']/10)
        return climateData

    def checkIfTableExist(self, tableName):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        ifExist = "DROP TABLE IF EXISTS " + tableName
        cursor.execute(ifExist)
        newConection.commit()
        cursor.close()
        newConection.close()


    def createTableOfLower48(self):
        psqlConnect = dataBaseLogin()
        stations = self.getStationTable()
        climateData = self.getClimateDataFromS3()
        tableName = "lower48_data_elv"
        self.checkIfTableExist(tableName)
        lower48data = stations.join(climateData, stations.station_name == climateData.station_name)\
                              .select(['id', 'date', 'elevation', 'TMAX', 'TMIN', 'PRCP'])
        lower48data.write.format("jdbc").option("url",psqlConnect.url ).option("dbtable", tableName)\
                   .option("user", psqlConnect.user).option("password", psqlConnect.password).save()



if __name__ == '__main__':
    readingClimateData = readClimateData()
    test  = readingClimateData.createTableOfLower48()
    print("Done!")

