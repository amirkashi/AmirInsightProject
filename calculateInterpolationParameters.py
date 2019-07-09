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
from pyspark.ml.regression import LinearRegression


class calimateDataInterpolation:

    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark").getOrCreate()
        self.monthlyData = self.getMonthlyData()
    '''
    def getElevation(self):
        psqlConnect = dataBaseLogin()
        elevationTable = self.spark.read.format("jdbc").option("url", psqlConnect.url)\
            .option("user", psqlConnect.user).option("password",psqlConnect.password)\
            .option("dbtable", "states_elevation").load()
        elevationTable.show()
        return elevationTable
    '''
    def getMonthlyData(self):
        psqlConnect = dataBaseLogin()
        monthlyData = self.spark.read.format("jdbc").option("url", psqlConnect.url)\
            .option("user", psqlConnect.user).option("password",psqlConnect.password)\
            .option("dbtable", "states_elevation").load()
        # monthlyData = monthlyData.

        #monthlyData.show()
        return monthlyData

    def queryMontlyData(self, startYear, endYear):

        query = "SELECT tmax from " +  self.monthlyData + " "+\
                "lower48_data_elv WHERE year BETWEEN " +
                startYear + " AND " + endYear;



if __name__ == '__main__':
    interpolatingData = calimateDataInterpolation()
    #interpolatingData.getElevation()
    interpolatingData.getMonthlyData()
