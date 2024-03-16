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
    # a
    # Loading data
    rideshare_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/sample_data.csv",header=True)
    # rideshare_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv",header=True)
    taxi_zone_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv", header=True)
    
    # b
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
    
    # c
    # join_df = join_df.withColumn(unix_timestamp("timestamp_col"), "yyyy-MM-dd").cast("date")
    join_df = join_df.withColumn("date", from_unixtime(col("date")).cast("date"))
    join_df = join_df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
                      
    join_df.show()                                                                                                                         

    # d
    # printing the number of rows
    row_count = join_df.count()
    print(f'The DataFrame has {row_count} rows.')                                                                                      
                                                                                            
    # TASK 2                                                                                                             
    # 1
    # turning the dataframe file to rdd
    df = join_df.rdd
    # mapping data to find the business and the month
    data_1 = df.map(lambda x: ((x[0], x[9][5:7]), 1))
    # reducing to find the number of occurences of the pair
    data_1 = data_1.reduceByKey(add)                                                   
    print(data_1.collect())   

    # 2                                                                                                                                                    
    # mapping data to find the rideshare profit
    data_2 = df.map(lambda x: ((x[0],x[9][5:7]), float(x[12])))                                                                
    # adding the rideshare profits of the occurences of business-month
    data_2 = data_2.reduceByKey(add)
    # reducing to find the number of occurences of the pair
    data_2 = data_2.reduceByKey(add)   
    print(data_2.collect())  
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskOne.txt')
    my_result_object.put(Body=json.dumps(join_df.take(100)))                  
                           
    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskTwo_1.txt')
    my_result_object.put(Body=json.dumps(data_1.collect()))

    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskTwo_2.txt')
    my_result_object.put(Body=json.dumps(data_2.collect()))
                                                                                                                                       
    spark.stop()                           
                      
