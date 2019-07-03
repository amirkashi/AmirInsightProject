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
        #conf = SparkConf()
        #sc = SparkContext(conf=conf)
        #self.sql_context = SQLContext(sc)

    def getStationTable(self):
        psqlConnect = dataBaseLogin()
        stationTable = self.spark.read.format("jdbc").option("url", psqlConnect.url)\
            .option("user", psqlConnect.user).option("password",psqlConnect.password)\
            .option("dbtable", "stations").load()
        return stationTable

    def generateListOfFilesToRead(self):
        startYear = 1919
        endYear = 2018
        listOfFilses = []
        for year in range(startYear, endYear+1):
            listOfFilses.append("s3a://noaa-ghcn-pds/csv/" + str(year) +  ".csv")
        return listOfFilses


    def getClimateDataFromS3(self):
        data_schema = StructType([StructField('station_name', StringType(), True),\
                                  StructField('date', StringType(), True), \
                                  StructField('data_type', StringType(), True),\
                                  StructField('data', DoubleType(), True)])

        listOfFilses = self.generateListOfFilesToRead()
        #climateData = self.spark.read.csv("s3a://noaa-ghcn-pds/csv/1900.csv", header=False, schema=data_schema)
        climateData = self.spark.read.csv(listOfFilses, header=False, schema=data_schema)
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


    def createDataframeOfLower48Daily(self):
        psqlConnect = dataBaseLogin()
        stations = self.getStationTable()
        climateData = self.getClimateDataFromS3()
        tableName = "lower48_data_elv"
        self.checkIfTableExist(tableName)
        lower48dataDaily = stations.join(climateData, stations.station_name == climateData.station_name)\
                              .select(['id', 'date', 'elevation', 'TMAX', 'TMIN', 'PRCP'])
        lower48dataDaily = lower48dataDaily.withColumn('date', F.to_date(lower48dataDaily.date, format='yyyyMMdd'))
        #lower48data.write.format("jdbc").option("url",psqlConnect.url ).option("dbtable", tableName)\
        #           .option("user", psqlConnect.user).option("password", psqlConnect.password).save()
        lower48dataDaily = lower48dataDaily.withColumn("year", year(lower48dataDaily['date']))
        lower48dataDaily = lower48dataDaily.withColumn("month", month(lower48dataDaily['date']))
        #lower48dataDaily.show()
        #lower48dataDaily.printSchema()
        return lower48dataDaily

    def createDataframeOfLower48Montly(self):
        psqlConnect = dataBaseLogin()
        tableName = "lower48_data_elv"
        self.checkIfTableExist(tableName)
        lower48dataDaily = self.createDataframeOfLower48Daily()
        lower48datamontly = lower48dataDaily.groupBy("id" ,"year", "month").agg({"TMAX":"avg", "TMIN":"avg", "PRCP":"sum"})
        lower48datamontly.write.format("jdbc").option("url",psqlConnect.url ).option("dbtable", tableName)\
                   .option("user", psqlConnect.user).option("password", psqlConnect.password).save()


        #lower48datamontly = lower48datamontly.selectExpr("id as id", "year as year", "month as month", "avg(TMAX) as TMAX", "avg(TMIN) as TMIN", "sum(PRCP) as PRCP")
        #lower48dataDaily.withColumnRenamed("avg(TMAX)","TMAX")
        #lower48dataDaily = lower48dataDaily.withColumnRenamed("avg(TMIN)","TMIN")
        #lower48dataDaily = lower48dataDaily.withColumnRenamed("sum(PRCP)","PRCP")
        lower48datamontly.show()
        lower48datamontly.printSchema()

        #lower48dataDaily.show()




if __name__ == '__main__':
    readingClimateData = readClimateData()
    test  = readingClimateData.createDataframeOfLower48Montly()
    #test  = readingClimateData.generateListOfFilesToRead()
    print("Done!")

