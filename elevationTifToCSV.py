
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
from constantValues.loginInfo import AwsLogins
from databaseConnection import dataBaseConnect



client = boto3.client('s3')
connectAWS = AwsLogins()
bucket = connectAWS.s3BucketClimate
fileName = "daily_lower48/dailyData.csv"
lines = client.get_object(Bucket=bucket, Key=fileName)['Body'].read().decode('utf-8')


#file = "s3a://climate-data-insight-prj/daily_lower48/dailyData.csv"
print(lines)



