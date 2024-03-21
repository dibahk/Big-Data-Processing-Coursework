import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, asc
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
    
                                                                                            
    # TASK 6
    # turning the dataframe file to rdd
    df = join_df.rdd
    # 1
    # function for defining the schema because the key is like [[borough, month], count] and we want it to be [borough, month, count]
    def explode_key(key_value):
        key, value = key_value
        return [key[0], key[1], value]

    # mapping data to find the borough and the month
    # data_1 = df.map(lambda x: ((x[15], x[8]), 1))
    # # reducing to find the number of occurences of the pair
    # data_1 = data_1.reduceByKey(add)
    # data_1 = data_1.filter(lambda x: x[1] > 0 and x[1] < 1000)
    # # Apply the explode_key function to each element of the RDD using flatMap
    # exploded_rdd = data_1.map(explode_key)
    # df_1 = spark.createDataFrame(exploded_rdd, ["Pickup_Borough", "time_of_day", "trip_count"])
    # df_1.show(df_1.count(), truncate = False)

    # 2
    data_2 = df.map(lambda x: ((x[15], x[8]), 1))
    # reducing to find the number of occurences of the pair
    data_2 = data_2.reduceByKey(add)
    data_2 = data_2.filter(lambda x: x[0][1] == 'evening')
    # Apply the explode_key function to each element of the RDD using flatMap
    exploded_rdd = data_2.map(explode_key)
    df_2 = spark.createDataFrame(exploded_rdd, ["Pickup_Borough", "time_of_day", "trip_count"])
    df_2.show(df_2.count(), truncate = False)

    # # 3
    # # defining the schema of the dataframe
    # data_3 = df.map(lambda x: (x[15], x[18], x[16]))
    # # reducing to find the number of occurences of the pair
    # data_2 = data_2.filter(lambda x: x[0] == 'Brooklyn' and x[1] == 'Staten Island')
    # # Turning the RDD to data frame to better show it in the terminal
    # df_2 = spark.createDataFrame(exploded_rdd, ["Pickup_Borough", "Dropoff_Borough", "Pickup_Zone"])
    # df_2.show(10, truncate = False)
    # print("the number of trips from Brooklyn to Staten Island is {}".format(df_2.count())
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
                 
                           
    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskSix_1.txt')
    # my_result_object.put(Body=json.dumps(data_1.collect()))

    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskSix_2.txt')
    my_result_object.put(Body=json.dumps(data_2.collect()))

    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskSix_3.txt')
    # my_result_object.put(Body=json.dumps(data_3.collect()))
                                                                                                                                       
    spark.stop()                           
                      
