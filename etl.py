import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create/Retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Reads data from S3 bucket (parameter input_data) into a data frame, processes it using Spark and loads data back to S3 in songs and artists table
        :param spark: Spark session
        :param input_data: source S3 bucket
        :param output_data: destination S3 bucket
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates()
    artists_table.createOrReplaceTempView('artists')

    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Reads data from S3 bucket (parameter input_data) into a data frame, processes it using Spark and loads data back to S3 in users, time, and songplays table
        :param spark: Spark session
        :param input_data: source S3 bucket
        :param output_data: destination S3 bucket
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = df.filter(df.page == 'NextSong') \
                  .select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
                    .dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table = log_df.select('datetime') \
                           .withColumn('start_time', log_df.datetime) \
                           .withColumn('hour', F.hour('datetime')) \
                           .withColumn('day', F.dayofmonth('datetime')) \
                           .withColumn('week', F.weekofyear('datetime')) \
                           .withColumn('month', F.month('datetime')) \
                           .withColumn('year', F.year('datetime')) \
                           .withColumn('weekday', F.dayofweek('datetime')) \
                           .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')
    
    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    log_song_df = log_df.join(song_df, (log_df.artist == song_df.artist_name))

    
    songplays_table = log_song_df.select(
        F.monotonically_increasing_id().alias('songplay_id'),
        F.col('datetime').alias('start_time'),
        F.year('datetime').alias('year'),
        F.month('datetime').alias('month'),
        F.col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        F.col('sessionId').alias('session_id'),
        'location',
        F.col('userAgent').alias('user_agent')
    )
    
    songplays_table.createOrReplaceTempView('songplays')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                   .parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')
    


def main():
    """
       Creates Spark Session Instance,
       defines input_data and output_data and assigns source and destination S3 buckets,
       calls process_song_data() and process_log_data() functions to process the ETL pipelines
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://abhi-spark-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()