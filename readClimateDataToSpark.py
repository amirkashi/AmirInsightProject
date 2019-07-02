import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
#from databaseConnection import connectToDataBase
from constantValues.loginInfo import dataBaseLogin
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



if __name__ == '__main__':
    readingClimateData = readClimateData()
    stationTable = readingClimateData.getStationTable()
    stationTable.show()

