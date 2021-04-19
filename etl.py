import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def create_spark_session():
    spark=SparkSession.builder.getOrCreate()
    return spark


def process_song_data(spark,input_data, output_data):
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    df = spark.read.json(song_data,schema = song_schema)

    