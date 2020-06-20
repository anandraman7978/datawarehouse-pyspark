import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as lng
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from pyspark import SparkContext


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
KEY= config['AWS']['AWS_ACCESS_KEY_ID']
SECRET= config['AWS']['AWS_SECRET_ACCESS_KEY']
print(KEY)
print(SECRET)


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Loads the artists and songs tables from the s3 location, creates tables and loads them back to s3 location as parquet

            Parameters:
                    spark -- spark session
                    input_data -- input s3 location
                    output data -- output s3 location
                    
            Returns:
                    None
    '''
    
    # read song data file
    input_data = "s3a://udacity-dend/"
    song_data = os.path.join(input_data, "song_data/A/A/B/*.json")
    dfsong = spark.read.json(song_data)

    # extract columns to create songs table
    dfSongWithSchema= dfsong.select([c for c in  dfsong.columns if c in ['song_id','title','artist_id','year','duration']])
    dfSongWithSchema.createOrReplaceTempView("songs")
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table
    dfSongWithSchema.write.partitionBy("year", "artist_id").mode('overwrite').parquet("s3a://udacity-demo-1-1/song.parquet")
    print("complete song file")
    spark.sql("SELECT count(*) FROM songs").show()

    # extract columns to create artists table
    dfArtistWithSchema = dfsong.select([c for c in  dfsong.columns if c in ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']])
    dfArtistWithSchema.createOrReplaceTempView("artists")
    
    # write artists table to parquet files
    #artists_table
    dfArtistWithSchema.write.mode('overwrite').parquet("s3a://udacity-demo-1-1/artists.parquet")
    print("complete artists file")
    spark.sql("SELECT count(*) FROM artists").show()


def process_log_data(spark, input_data, output_data):
    '''
    Loads the users,time and song_plays tables from the s3 location, creates tables and loads them back to s3 location as parquet

            Parameters:
                    spark -- spark session
                    input_data -- input s3 location
                    output data -- output s3 location
                    
            Returns:
                    None
    '''
    # get filepath to log data file
    log_data =os.path.join(input_data, "log-data/2018/11/*.json")

    # read log data file
    dflogs = spark.read.json(log_data) 
    
    # filter by actions for song plays
    filterDF = dflogs.where("page=='NextSong'") 

    # extract columns for users table    
    #artists_table = 
    dfUserWithSchema = dflogs.select([c for c in  dflogs.columns if c in ['userId','firstName','lastName','gender','level']])
    dfUserWithSchema.createOrReplaceTempView("users")   
    
    # write users table to parquet files
    dfUserWithSchema.write.mode('overwrite').parquet("s3a://udacity-demo-1-1/users.parquet")
    print("complete users file")
    spark.sql("SELECT count(*) FROM users").show()
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    timestDF = filterDF.withColumn("timestamp", get_timestamp(filterDF.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    datetimeDF = filterDF.withColumn("datetime", get_datetime(filterDF.ts))
    
    # extract columns to create time table
    get_time_val = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%H-%M-%S'))
    timeallDF =  timestDF.withColumn('starttime', get_time_val(filterDF.ts))
    timeallDF = timeallDF.withColumn('hour', hour ('timestamp'))
    timeallDF = timeallDF.withColumn('day', dayofmonth ('timestamp'))
    timeallDF = timeallDF.withColumn('week', weekofyear ('timestamp'))
    timeallDF = timeallDF.withColumn('month', month ('timestamp'))
    timeallDF = timeallDF.withColumn('year', year ('timestamp'))
    timeallDF = timeallDF.withColumn('weekday', date_format('timestamp','E'))
    timeDF = timeallDF.select([c for c in  timeallDF.columns if c in ['starttime','hour','day','week','month','year','weekday']])
    parqtimeDF= timeDF.write.partitionBy("year", "month").mode('overwrite').parquet("s3a://udacity-demo-1-1/time.parquet")
    parquettimeDF = spark.read.parquet("s3a://udacity-demo-1-1/time.parquet")
    parquettimeDF.createOrReplaceTempView("time")
    print("complete time file")
    spark.sql("SELECT count(*) FROM time").show()
    

    # read in song data to use for songplays table
    song_artist_df=spark.sql("SELECT song_id, a.artist_id,title,artist_name,duration FROM songs a inner join artists b where a.artist_id=b.artist_id")
    song_artist_df.createOrReplaceTempView("song_artist") 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = filterDF.join(song_artist_df, (filterDF.song == song_artist_df.title) & (filterDF.artist == song_artist_df.artist_name), 'left_outer')\
        .select(
            get_time_val(filterDF.ts).alias('starttime'),
            col("userId").alias('user_id'),
            filterDF.level,
            (song_artist_df.title).alias('song_id'),
            song_artist_df.artist_id,
            col("sessionId").alias("session_id"),
            filterDF.location,
            col("useragent").alias("user_agent"),
            year(get_datetime(filterDF.ts)).alias('year'),
            month(get_datetime(filterDF.ts)).alias('month')
           
        )
    songplays_table= songplays_table.withColumn('songplay_id',monotonically_increasing_id())
    songplays_table.createOrReplaceTempView("song_plays")        

    # write songplays table to parquet files partitioned by year and month
    parqsongplaysDF= songplays_table.write.partitionBy('year','month').mode('overwrite').parquet("s3a://udacity-demo-1-1/songplay.parquet")
    print("complete song_plays file")
    spark.sql("SELECT count(*) FROM song_plays").show()
def main():
    '''
    Main Function initiates spark session. calls function process_data with the song and log files, spark session, input s3 location and 
    output s3 location are provided as parameters to the functions.

            Parameters:
                    None
            Returns:
                    None
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-demo-1-1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
