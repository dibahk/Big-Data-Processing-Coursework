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
                                                                                                         
                                                                                            
    # TASK 4
    # turning the dataframe file to rdd
    df = join_df.rdd
    # 1
    # extracting time of the day and driver_total_pay because these are the columns that we want to analyse
    data_1 = df.map(lambda x: (x[8], float(x[11])))
    # since we need the average I am also going to employ another map on the values precisely to also add a column of 1 as a way to count the values in each key
    data_1 = data_1.mapValues(lambda x:(x, 1))
    # reducing the RDD by summing the driver_total_pay and counting instances of each key
    data_1 = data_1.reduceByKey(lambda x,y :(x[0]+y[0],x[1]+y[1]))
    # divinding the summation of driver_total_pay and counts of each key to calculate average
    averages_1 = data_1.mapValues(lambda x : x[0]/x[1])
    # creating the dataframe with the required column names
    df_1 = spark.createDataFrame(averages_1, ["time_of_day", "average_drive_total_pay"])
    # showing the dataframe in the terminal and sorting it as required
    df_1.sort(desc("average_drive_total_pay")).show(df_1.count(), truncate = False)

    # 2
    # extracting time of the day and trip_length because these are the columns that we want to analyse
    data_2 = df.map(lambda x: (x[8], float(x[3])))
    # since we need the average I am also going to employ another map on the values precisely to also add a column of 1 as a way to count the values in each key
    data_2 = data_2.mapValues(lambda x:(x, 1))
    # reducing the RDD by summing the trip_length and counting instances of each key
    data_2 = data_2.reduceByKey(lambda x,y :(x[0]+y[0],x[1]+y[1]))
    # divinding the summation of trip_length and counts of each key to calculate average
    averages_2 = data_2.mapValues(lambda x : x[0]/x[1])
    # creating the dataframe with the required column names
    df_2 = spark.createDataFrame(averages_2, ["time_of_day_2", "average_trip_length"])
    # showing the dataframe in the terminal and sorting it as required
    df_2.sort(desc("average_trip_length")).show(df_2.count(), truncate = False)

    # 3
    # Combining the average driver total pay data frame with the average trip length
    # dataframe based on the time of the day to be able to do required calculations on them
    # the joining is done on their time of day feature however, in order to differentiate between the two data frames the time of day column in the second data frame has been renamed.
    df_3 = df_1.join(df_2, df_1.time_of_day == df_2.time_of_day_2, 'inner')
    # in the results we have two time of day columns therefore we delete one of them
    df_3 = df_3.drop('time_of_day_2')
    # adding a new column to the dataframe for the average earning per mile where we divided the average driver_total_pay for each time of day by average trip length
    df_3 = df_3.withColumn('average_earning_per_mile', df_3['average_drive_total_pay'] / \
                           df_3['average_trip_length'])
    # dropping the inital columns of data which we used for division
    df_3 = df_3.drop('average_drive_total_pay')
    df_3 = df_3.drop('average_trip_length')
    df_3.show()
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
                 
                           
    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_1.txt')
    my_result_object.put(Body=json.dumps(averages_1.collect()))

    my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_2.txt')
    my_result_object.put(Body=json.dumps(data_2.collect()))

    # my_result_object = my_bucket_resource.Object(s3_bucket,'Result/taskThree_3.txt')
    # my_result_object.put(Body=json.dumps(data_3.collect()))
                                                                                                                                       
    spark.stop()                           
                      
