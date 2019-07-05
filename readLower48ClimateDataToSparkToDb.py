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

class readMonthlyClimateData:

    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark").getOrCreate()

    def getClimateMontlyFromS3(self):
        data_schema = StructType([StructField('id', IntegerType(), True),\
                                  StructField('date', DateType(), True), \
                                  StructField('elevation', DoubleType(), True),\
                                  StructField('TMAX', DoubleType(), True),\
                                  StructField('TMIN', DoubleType(), True),\
                                  StructField('PRCP', DoubleType(), True),\
                                  StructField('year', IntegerType(), True),\
                                  StructField('month', IntegerType(), True)])


        monthlyClimateData = self.spark.read.csv("s3a://climate-data-insight-prj/daily_lower48/*.csv", header=False, schema=data_schema)
        #monthlyClimateData.show()
        #monthlyClimateData.printSchema()
        return monthlyClimateData


    def checkIfTableExist(self, tableName):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        ifExist = "DROP TABLE IF EXISTS " + tableName
        cursor.execute(ifExist)
        newConection.commit()
        cursor.close()
        newConection.close()


    def createDataframeOfLower48Montly(self):
        psqlConnect = dataBaseLogin()
        tableName = "lower48_data_elv"
        self.checkIfTableExist(tableName)
        lower48dataDaily = self.getClimateMontlyFromS3()
        lower48datamontly = lower48dataDaily.groupBy("id" ,"year", "month").agg({"TMAX":"avg", "TMIN":"avg", "PRCP":"sum"})
        lower48dataDaily = lower48dataDaily.withColumnRenamed("avg(TMIN)","TMIN")
        lower48dataDaily = lower48dataDaily.withColumnRenamed("sum(PRCP)","PRCP")
        lower48dataDaily.write.save("s3a://climate-data-insight-prj/monthly_lower48/monthly-1919-2019.csv", format='csv')
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
    readingClimateData = readMonthlyClimateData()
    test  = readingClimateData.createDataframeOfLower48Montly()
    #test  = readingClimateData.generateListOfFilesToRead()
    print("Done!")

