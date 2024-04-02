import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")\
        .appName("graphframes")\
        .getOrCreate()

    sqlContext = SQLContext(spark)
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

    # GraphFrames will expect to have our key named as 'id'
    vertexSchema = StructType([StructField("id", IntegerType(), False),
                               StructField("Borough", StringType(), True),
                               StructField("Zone", StringType(), True),
                               StructField("service_zone", StringType(), True)])

    edgeSchema = StructType([StructField("src", IntegerType(), False),
                               StructField("dst", IntegerType(), False)])

    # reading from dataset to load edges and vertices data respectivily
    edgesDF = spark.read.format("csv").options(header='True').schema(edgeSchema).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/sample_data.csv")
    # edgesDF = spark.read.format("csv").options(header='True').schema(edgeSchema).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    verticesDF = spark.read.format("csv").options(header='True').schema(vertexSchema).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    # showing 10 rows from the vertices and edges tables
    verticesDF.show(10, truncate= False)
    edgesDF.show(10, truncate= False)

    # Now create a graph using the vertices and edges
    graph = GraphFrame(verticesDF, edgesDF)

    # Now print the graph using the show() command on "triplets" properties which return DataFrame with columns ‘src’, ‘edge’, and ‘dst’
    graph.triplets.show(3, truncate=False)
