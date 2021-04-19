import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType


def create_spark_session():
    spark=SparkSession.builder.getOrCreate()
    return spark


def process_song_data(spark,input_data,output_data):
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
    songs_fields=['title','artist_id','year','duration']
    songs_table= df.select(songs_fields).dropDuplicates().withColumn("song_id",monotonically_increasing_id())
    songs_table.write.mode("overwrite").parquet(output_data + 'songs')
    
    artists_fields=['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artists_table=df.select(artists_fields).dropDuplicates()
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')