"""
This code is responsible to get climate daily data from s3 year by 
year. Find data that belong to stations in lower 48 states of 
the US. Save them as csv format in s3.
"""


import sys
from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from constantValues.loginInfo import dataBaseLogin
from databaseConnection import dataBaseConnect
from pyspark.sql import DataFrameReader
import csv

class readClimateData:

    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark").getOrCreate()

    def getStationTable(self):
        """
        this function get stations iformation of lower the lower 48 
        states in US and create data frame for it. This dataframe was 
        used to find the daily data which belongs to these 48 states. 
        """
        psqlConnect = dataBaseLogin()
        stationTable = self.spark.read.format("jdbc").option("url", psqlConnect.url)\
            .option("user", psqlConnect.user).option("password",psqlConnect.password)\
            .option("dbtable", "stations").load()
        return stationTable

    def getClimateDataFromS3(self, currentYear):
        """
        This function reads a csv file of a given year and then 
        create a data frame of it and then returns it. 
        """
        data_schema = StructType([StructField('station_name', StringType(), True),\
                                  StructField('date', StringType(), True), \
                                  StructField('data_type', StringType(), True),\
                                  StructField('data', DoubleType(), True)])
        fileToReadFromS3 = "s3a://noaa-ghcn-pds/csv/" + str(currentYear) + ".csv"
        climateData = self.spark.read.csv(fileToReadFromS3, header=False, schema=data_schema)
        climateData = climateData.groupby(climateData.station_name, climateData.date).pivot("data_type").avg("data")\
                .select(['station_name', 'date', 'TMAX', 'TMIN', 'PRCP'])\
                .withColumn('country', climateData['station_name'][0:2])
        climateData = climateData.withColumn('TMAX', climateData['TMAX']/10)
        climateData = climateData.withColumn('TMIN', climateData['TMIN']/10)
        return climateData

    def createDataframeOfLower48Daily(self, currentYear):
        """
        This function uses two functions of these class to create data
        frame for stations and daily data. Then it join them and create 
        daily data of stations in the lower 48 states and then same them 
        in s3
        """
        stations = self.getStationTable()
        climateData = self.getClimateDataFromS3(currentYear)
        fileNameToSaveInS3 = "s3a://climate-data-insight-prj/daily_lower48/" + str(currentYear) + ".csv"
        lower48dataDaily = stations.join(climateData, stations.station_name == climateData.station_name)\
                              .select(['id', 'date', 'elevation', 'TMAX', 'TMIN', 'PRCP'])
        lower48dataDaily = lower48dataDaily.withColumn('date', F.to_date(lower48dataDaily.date, format='yyyyMMdd'))
        lower48dataDaily = lower48dataDaily.withColumn("year", year(lower48dataDaily['date']))
        lower48dataDaily = lower48dataDaily.withColumn("month", month(lower48dataDaily['date']))
        lower48dataDaily.write.save(fileNameToSaveInS3, format='csv')
        lower48dataDaily.show()
        lower48dataDaily.printSchema()



if __name__ == '__main__':
    readingClimateData = readClimateData()
    for currentYear in range(1919, 2019):
        readingClimateData.createDataframeOfLower48Daily(currentYear)
    print("Done!")

