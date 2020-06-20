import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table IF EXISTS staging_events;"
staging_songs_table_drop = "drop table IF EXISTS staging_songs;"
songplay_table_drop = "drop table IF EXISTS songplays;"
user_table_drop = "drop table IF EXISTS users;"
song_table_drop = "drop table IF EXISTS songs;"
artist_table_drop = "drop table IF EXISTS artists;"
time_table_drop = "drop table IF EXISTS time;"

# CREATE TABLES

staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs int,artist_id varchar(255), artist_latitude varchar(255), artist_longitude varchar(255), artist_location varchar(2000), artist_name varchar(2000), song_id varchar(255), title varchar(1000), duration float, year int);
""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar(1000),auth varchar(255), firstName varchar(255), gendre varchar(10), iteminSession int, lastName varchar(255), length float, level varchar(255), location varchar(255),method varchar(10),page varchar(255),registration text,sessionid int,song varchar(255),status int,ts bigint,useragent varchar(1000),userid int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int identity(0,1) PRIMARY KEY NOT NULL,start_time varchar, user_id int, level varchar(255), song_id varchar(255), artist_id varchar(255), session_id int, location varchar(255), user_agent varchar(1000));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int PRIMARY KEY NOT NULL, first_name varchar(255), last_name varchar(255), gender varchar(10), level varchar(255));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar(255) PRIMARY KEY NOT NULL, title varchar(1000), artist_id varchar(255), year int, duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar(255) PRIMARY KEY NOT NULL, name varchar(2000), location varchar(2000), latitude varchar(255), longtitude varchar(255));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time varchar(255) PRIMARY KEY NOT NULL, hour int, day varchar, week int, month int,year int,weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_songs from 's3://udacity-dend/song_data' CREDENTIALS 'aws_iam_role={}' REGION 'us-west-2' JSON 'auto' truncatecolumns;
""").format('arn:aws:iam::860495413408:role/myRedshiftRole')





staging_songs_copy = ("""copy staging_events from 's3://udacity-dend/log_data' CREDENTIALS 'aws_iam_role={}'  REGION 'us-west-2' JSON 'auto' truncatecolumns;
""").format('arn:aws:iam::860495413408:role/myRedshiftRole')


# FINAL TABLES

songplay_table_insert = ("""insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) select TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time, userid, level, song_id, artist_id, sessionid, location, useragent from staging_events a inner join staging_songs b on b.title=a.song and a.artist=b.artist_name and b.duration=a.length where a.page='NextSong';
""")

user_table_insert = ("""

create temp table users_stg as
SELECT DISTINCT a.userid,
a.firstName,
a.lastName,
a.gendre,
a.level
FROM staging_events a
where a.userid is not null ;


begin transaction;


update users
set level = users_stg.level
from users_stg
where users.user_id = users_stg.userid
and users.level != users_stg.level;



delete from users_stg
using users
where users.user_id = users_stg.userid;



insert into users
select * from users_stg;


end transaction;

-- Drop the staging table

drop table users_stg;
""")

song_table_insert = ("""
insert into songs select distinct song_id,title,artist_id,CAST(nullif(year, 0) AS integer),duration from staging_songs where song_id is not null;
""")

artist_table_insert = ("""
insert into artists select distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude from staging_songs where artist_id is not null;
""")

time_table_insert = ("""insert into time select distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time,EXTRACT(HOUR from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS hour,EXTRACT(DAY from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS day,EXTRACT(WEEK from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS week,EXTRACT(MONTH from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS month,EXTRACT(YEAR from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS year,EXTRACT(DOW from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS weekday from staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
