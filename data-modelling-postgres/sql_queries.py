# DROP TABLES

songplay_table_drop = "drop table IF EXISTS songplays;"
user_table_drop = "drop table IF EXISTS users;"
song_table_drop = "drop table IF EXISTS songs;"
artist_table_drop = "drop table IF EXISTS artists;"
time_table_drop = "drop table IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY NOT NULL,start_time varchar, user_id text, level text, song_id varchar, artist_id varchar, session_id text, location varchar, user_agent text);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id varchar primary key NOT NULL, first_name varchar, last_name varchar, gender text, level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar primary key NOT NULL, title varchar, artist_id varchar, year int, duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar primary key NOT NULL, name varchar, location varchar, latitude text, longtitude text);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time varchar primary key NOT NULL, hour int, day varchar, week int, month int,year int,weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)values(%s,%s,%s,%s,%s,%s,%s,%s)""")

user_table_insert = ("""insert into users(user_id, first_name, last_name, gender, level)values(%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;
""")


song_table_insert = ("""insert into songs(song_id,title,artist_id,year,duration)values(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""insert into artists(artist_id,name,location,latitude,longtitude)values(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;""")


time_table_insert = ("""insert into time(start_time,hour,day,week,month,year,weekday) values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""select b.song_id,A.artist_id from artists A inner join songs B on A.artist_id=B.artist_id  
where title=%s and name=%s and duration=%s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
