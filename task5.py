import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
from graphframes import *
from operator import add

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    # TASK 1
    # 1
    # Loading data
    # rideshare_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/sample_data.csv",header=True)
    rideshare_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv",header=True)
    taxi_zone_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv", header=True)
    
    # 2
    # doing the first join on pickup location
    join_df = rideshare_df.join(taxi_zone_df, rideshare_df.pickup_location==taxi_zone_df.LocationID, 'inner')
    join_df = join_df.drop('LocationID')
    join_df = join_df.withColumnRenamed('Borough', 'Pickup_Borough')
    join_df = join_df.withColumnRenamed('Zone', 'Pickup_Zone')
    join_df = join_df.withColumnRenamed('service_zone', 'Pickup_service_zone')
   
    # doing the second join on dropoff location
    join_df = join_df.join(taxi_zone_df,join_df.dropoff_location==taxi_zone_df.LocationID, 'inner')
    join_df = join_df.drop('LocationID')
    join_df = join_df.withColumnRenamed('Borough', 'Dropoff_Borough')
    join_df = join_df.withColumnRenamed('Zone', 'Dropoff_Zone')
    join_df = join_df.withColumnRenamed('service_zone', 'Dropoff_service_zone')
    
    # 3
    
    join_df = join_df.withColumn("date", from_unixtime(col("date")).cast("date"))
    join_df = join_df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
                                                                                                         
                                                                                            
    # TASK 5
    # first we filter the data frame to rides which happened in January
    # The condition uses the month function from pyspark.sql.functions to extract the month number from the "date" column.
    df = join_df.filter(month(col("date")) == 1)
    # turning the dataframe file to rdd
    df = df.rdd
    # extracting time of the day and driver_total_pay because these are the columns that we want to analyse
    data_1 = df.map(lambda x: (int(x[9][8:]), float(x[4])))
    # since we need the average I am also going to employ another map on the values precisely to also add a column of 1 as a way to count the values in each key
    data_1 = data_1.mapValues(lambda x:(x, 1))
    # reducing the RDD by summing the driver_total_pay and counting instances of each key
    data_1 = data_1.reduceByKey(lambda x,y :(x[0]+y[0],x[1]+y[1]))
    # divinding the summation of driver_total_pay and counts of each key to calculate average
    averages_1 = data_1.mapValues(lambda x : x[0]/x[1]).sortBy(lambda x: x[0])
    
    
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
                 
                           
    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskFive_1.txt')
    my_result_object.put(Body=json.dumps(averages_1.collect()))

    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_2.txt')
    # my_result_object.put(Body=json.dumps(data_2.collect()))

    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_3.txt')
    # my_result_object.put(Body=json.dumps(data_3.collect()))
                                                                                                                                       
    spark.stop()                           
                      
