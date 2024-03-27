import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists events"
staging_songs_table_drop = "drop table if exists songs"
songplay_table_drop = "drop table if exists songplays cascade"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events
    (
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS raw_songs (
        num_songs        INT,
        artist_id        TEXT,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  TEXT,
        artist_name      TEXT,
        song_id          TEXT,
        title            TEXT,
        duration         FLOAT,
        year             INT
);
""")

songplay_table_create = ("""
create table if not exists songplays
(
    songplay_id    bigint identity(1,1) primary key,
    user_id        text not null distkey,
    start_time     timestamp not null sortkey,
    level          varchar(10),
    song_id        text,
    artist_id      text,
    session_id     integer,
    location       varchar(100),
    user_agent     text
) diststyle key;
""")

user_table_create = ("""
create table if not exists users 
(
    user_id     text primary key sortkey,
    first_name  varchar(100) not null,
    last_name   varchar(100) not null,
    gender      varchar(1) not null,
    level       varchar(10) not null
) diststyle all;
""")

song_table_create = ("""
create table if not exists songs (
    song_id       text primary key sortkey,
    title         varchar(100),
    artist_id     varchar(100) distkey,
    year          smallint,
    duration      float4

) diststyle key;
""")

artist_table_create = ("""
create table artists (
    artist_id     text primary key sortkey,
    name          varchar(100),
    location      varchar(100),
    latitude      float4,
    longitude     float4
) diststyle all;
""")

time_table_create = ("""
create table time (
    start_time timestamp primary key sortkey,
    hour       smallint,
    day        smallint,
    week       smallint,
    month      smallint,
    year       smallint distkey,
    weekday    smallint
) diststyle key
""")

# STAGING TABLES

staging_songs_copy = ("""
copy raw_songs from {}
region 'us-west-2'
credentials 'aws_iam_role={}'
json 'auto';
""").format(config.get("S3", "SONG_DATA"),
            config.get("IAM_ROLE", "ARN"))

staging_events_copy = ("""
copy events from {}
region 'us-west-2'
credentials 'aws_iam_role={}'
json {};
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
           config.get("S3", "LOG_JSONPATH"))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(user_id, start_time,  level, song_id, artist_id, session_id, location, user_agent)
select e.user_id, 
       TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'), 
       e.level, 
       s.song_id, 
       s.artist_id, 
       e.session_id, 
       e.location, 
       e.user_agent
from raw_songs s 
join events e on (s.artist_name = e.artist) and (s.title = e.song)
where e.user_id is not null
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name ,gender, level)
select distinct(user_id) user_id, 
       first_name, 
       last_name, 
       gender, 
       level
from events
where user_id is not null
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select distinct(song_id) song_id, 
       title, 
       artist_id, 
       year, 
       duration
from raw_songs
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
select distinct(artist_id) artist_id, 
       artist_name, 
       artist_location, 
       artist_latitude, 
       artist_longitude
from raw_songs
""")

time_table_insert = ("""
insert into time
       WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts        FROM events)
       select distinct ts,
       extract(hour from ts),
       extract(day from ts),
       extract(week from ts),
       extract(month from ts),
       extract(year from ts),
       extract(dow from ts)
from temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
