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
    # join_df = join_df.withColumn(unix_timestamp("timestamp_col"), "yyyy-MM-dd").cast("date")
    join_df = join_df.withColumn("date", from_unixtime(col("date")).cast("date"))
    join_df = join_df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
                                                                                          
                                                                                            
    # TASK 7
    # turning the dataframe file to rdd
    df = join_df.rdd
    # 1
    # defining the schema of the dataframe
    def route(key_value):
        key, value = key_value
        route = '{} to {}'.format(key[0], key[1])
        return [route, value]
        
    data_1 = df.map(lambda x: ((x[15], x[18], x[0]), 1))
    # reducing to find the number of occurences of the pair
    data_3 = data_3.reduceByKey(add).sortBy(lambda x: x[1], ascending = False)
    # Apply the route function to each element of the RDD using flatMap
    exploded_rdd = data_3.map(route)
    df_3 = spark.createDataFrame(exploded_rdd, ["Route", "total_profit"])
    df_3.show(truncate= False) 
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
                 
                           
    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_1.txt')
    my_result_object.put(Body=json.dumps(df_1.show()))

    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_2.txt')
    # my_result_object.put(Body=json.dumps(data_2.collect()))

    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_3.txt')
    # my_result_object.put(Body=json.dumps(data_3.collect()))
                                                                                                                                       
    spark.stop()                           
                      
