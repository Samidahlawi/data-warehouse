import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DEFINEED the arn, song_data, log_data, log_jsonpath to use it later with STAGING TABLES
SONG_D = config["S3"]["SONG_DATA"]
LOG_D = config["S3"]["LOG_DATA"]
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']
ARN = config["IAM_ROLE"]["ARN"]

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_fname VARCHAR(255),
    user_gender VARCHAR(1),
    item_in_session INTEGER,
    user_lname VARCHAR(255),
    song_length FLOAT,
    user_level VARCHAR(50),
    location INTEGER,
    method VARCHAR(20),
    page VARCHAR,
    registration FLOAT,
    session_id INTEGER, 
    song_title VARCHAR(255),
    status INTEGER, 
    ts VARCHAR(50),
    user_agent VARCHAR,
    user_id VARCHAR(100),
    PRIMARY KEY (event_id)
    )
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id))
""")

songplay_table_create = ("""CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1) sortkey,
    start_time TIMESTAMP,
    user_id INTEGER NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(100) ,
    artist_id VARCHAR(100),
    session_id INTEGER,
    location VARCHAR(255),
    user_agent VARCHAR,
    PRIMARY KEY (songplay_id)
    )
""")

user_table_create = ("""CREATE TABLE users(
    user_id INTEGER NOT NULL sortkey,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(50),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR(100) NOT NULL distkey sortkey,
    title VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR(100) NOT NULL sortkey,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP NOT NULL sortkey,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY(start_time)
    )
""")

# STAGING TABLES

staging_events_copy =  ("""COPY staging_events FROM {}
  CREDENTIALS 'aws_iam_role={}'REGION 'us-west-2'
  FORMAT AS JSON {};
""").format(LOG_D, ARN, LOG_JSON_PATH )

staging_songs_copy = ("""COPY staging_songs FROM {}
   CREDENTIALS 'aws_iam_role={}' REGION 'us-west-2'
   JSON 'auto';
""").format(SONG_D, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT (TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 sec') As start_time, 
        se.user_id, se.user_level,
        ss.song_id,ss.artist_id,
        se.session_id,se.location,se.user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong'
          AND se.song_title = ss.title
          AND user_id NOT IN (SELECT DISTINCT ss.user_id FROM songplays ss WHERE ss.user_id = se.user_id 
              AND ss.start_time = se.start_time AND ss.session_id = se.session_id )
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)  
    SELECT  
        s.user_id,
        s.user_fname,
        s.user_lname,
        s.user_gender, 
        s.user_level
    FROM staging_events s
    WHERE s.page = 'NextSong'
    AND s.user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT  
        s.song_id, 
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT
        s.artist_id,
        s.artist_name,
        s.artist_location,
        s.artist_latitude,
        s.artist_longitude
    FROM staging_songs s
    WHERE s.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

# I am going to extract year, hours, and so on.. from start_time
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(wd from start_time) AS weekday 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 sec' AS start_time 
        FROM staging_events    
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
