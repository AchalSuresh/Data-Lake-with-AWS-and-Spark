import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Function to create a Spark session in AWS
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    The function takes in the input data as json from S3 location and processes it and stores in the output table.
    The function is taking the data from songs folder
    
    Keyword Arguments:
        spark: References the spark session created
        input_data: References the input data from S3.
        output_data: References the output location to be used to store processed data.
        
    Output:
    songs_table: Contains the parquet files of song data
    artist_table: Contains the parquet files of artist data
    '''
    # filepath to song data file
    song_data = input_data + 'song_data/*/*/*.json'
    
    # read song data file
    df = spark.read.json('song_data')
    
    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year', 'duration').dropDuplicates()
    
    #View the loaded data
    songs_table.printSchema()
    songs_tables.show(5, truncate=False)
    
    #Creating a view of the data to be processed
    songs_table.createOrReplaceTempView('songs')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs_table.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table =  df.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude').dropDuplicates()
    
    #View the artist table data
    artists_table.printSchema()
    artist_table.show(5,truncated=False)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    The function takes in the input data as json from S3 location and processes it and stores in the output table.
    The function is taking the data from log folder and processes the playing history
    
    Keyword Arguments:
        spark: References the spark session created
        input_data: References the input data from S3.
        output_data: References the output location to be used to store processed data.
        
    Output:
    users_table: Contains the parquet files of users data
    time_table: Contains the parquet files of time at which song was played data
    songplays_table: Contains information about the song playing history
    
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays(Only select records that have page action of 'Next Song')
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName','gender', 'level').dropDuplicates()
    users_table.printSchema()
    users_table.show(5,truncated=False)
    
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output,'users_table.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x/1000)))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('date',get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                           .withColumn('start_time', df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates()
    
    time_table.printSchema()
    time_table.show(5,truncated=False)
    
    
    # write time table to parquet files
    time_table.write.parquet(os.path.join(output,'time_table.parquet'),'overwrite')

    # read in song data to use for songplays table
    song_df = input_data + 'song_data/*/*/*/*.json'

    # extract columns from joined song and log datasets to create songplays table 
    join_table = df.join(song_df,col(df.artist)==col(song_df.artist_id) & col(df.song)==col(song_df.title))
    
    songplays_table = join_table.select(
        col('join_table.datetime').alias('start_time'),
        col('join_table.userId').alias('user_id'),
        col('join_table.level').alias('level'),
        col('join_table.song_id').alias('song_id'),
        col('join_table.artist_id').alias('artist_id'),
        col('join_table.sessionId').alias('session_id'),
        col('join_table.location').alias('location'), 
        col('join_table.userAgent').alias('user_agent'),
        year('join_table.datetime').alias('year'),
        month('join_table.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())
    
    songplays_table.printSchema()
    songplays_table.show(5,truncated=False)
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays_table.parquet'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config['S3']['OUTPUT_DATA']
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
