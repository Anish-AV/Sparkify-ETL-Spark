import pyspark
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


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
    
    artists_fields=["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude","artist_longitude as longitude"]
    artists_table=df.selectExpr(artists_fields).dropDuplicates()
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')

def process_log_data(spark,input_data,output_data):
    log_data = os.path.join(input_data,"log-data/*.json")
    log_df = spark.read.json(log_data)
    log_df = log_df.filter(log_df.page=='NextSong')

    users_fields = ['userID as user_id', ' firstName as first_name', 'lastName as last_name','gender','level']
    users_table = log_df.selectExpr(users_fields).dropDuplicates()
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    get_timestamp = udf(lambda x:x /1000, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))

    get_datetime =udf(lambda x :datetime.fromtimestamp(x/1000),TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.ts))

    log_df = log_df.withColumn("hour", hour("start_time")).withColumn("day", dayofmonth("start_time")).withColumn("week", weekofyear("start_time")).withColumn("month", month("start_time")).withColumn("year", year("start_time")).withColumn("weekday", dayofweek("start_time"))

    time_fields = ['start_time', 'hour', 'day','week','month','year','weekday']
    time_table = log_df.select(time_fields)
    time_table.write.mode("overwrite").parquet(output_data + 'time')

    songs_df = spark.read.parquet(os.path.join(output_data,'songs'))
    songs_log = log_df.join(songs_df, (log_df.song==songs_df.title)).drop(songs_df.year).drop(songs_df.artist_id)
    artists_df = spark.read.parquet(os.path.join(output_data,'artists'))
    artists_df = artists_df.drop(artists_df.location)
    artists_songs_log = songs_log.join(artists_df,(songs_log.artist==artists_df.name))
    songplays = artists_songs_log

    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).repartition("year", "month")

    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data +'songplays')


def main():

    spark= create_spark_session()
    input_data="/FileStore/tables/Data"
    output_data="/FileStore/tables/NewData/"
    process_song_data(spark,input_data,output_data)
    process_log_data(spark,input_data,output_data)

if __name__=="__main__":
  main()