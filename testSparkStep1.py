
import boto3
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


spark = SparkSession.builder.appName("Spark").getOrCreate()
climateData = spark.read.csv("s3a://climate-data-insight-prj/daily_lower48/1900.csv", header=False)

climateData = climateData.groupBy('_c6').agg({'_c7':'avg'})
climateData.show()
climateData.printSchema()



#f = "s3a://climate-data-insight-prj/daily_lower48/dailyData.csv"


