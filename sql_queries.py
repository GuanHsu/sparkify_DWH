import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""                           
    CREATE TABLE staging_events (                                             
      artist            text,      
      auth              text, 
      firstName         text,      
      gender            text,      
      itemInSession     int,      
      lastName          text,      
      length            float,                             
      level             text,      
      location          text,                              
      method            text,      
      page              text,       
      registration      float,         
      sessionid         int,         
      song              text,                             
      status            int,  
      ts                numeric,
      userAgent         text,                             
      userid            int
	);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
      num_songs	        int,         
      artist_id         varchar(18),         
      artist_latitude   float,                            
      artist_longitude  float,                             
      artist_location   text,                             
      artist_name       text,         
      song_id           text,         
      title             text,          
      duration          float,                            
      year              int		
	);
""")

songplay_table_create = ("""
	CREATE TABLE songplay (
	  songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
	  start_time  text   NOT NULL, 
	  user_id     int    NOT NULL, 
	  level       text, 
	  song_id     text   NOT NULL, 
	  artist_id   text   NOT NULL, 
	  session_id  int    NOT NULL, 
      location    text, 
      user_agent  text
    );
""")

users_table_create = ("""
	CREATE TABLE users (
        user_id   int PRIMARY KEY, 
        first_name text, 
        last_name  text, 
        gender    text, 
        level     text
    ) diststyle all;    
""")

songs_table_create = ("""
	CREATE TABLE songs (
	   song_id    text   PRIMARY KEY, 
       title      text   NOT NULL, 
       artist_id  text   NOT NULL, 
       year       int    NOT NULL, 
       duration   float
    ) ;   
""")

artists_table_create = ("""
	CREATE TABLE artists (
        artist_id  text PRIMARY KEY, 
        name       text NOT NULL, 
        location   text, 
        lattitude  float, 
        longitude  float 
    ) diststyle all;   
""")

time_table_create = ("""
	CREATE TABLE time (
	   start_time  text PRIMARY KEY, 
       hour        int  NOT NULL,
       day         int  NOT NULL, 
       week        int  NOT NULL, 
       month       int  NOT NULL, 
       year        int  NOT NULL, 
       weekday     int
    );     
""")


# STAGING TABLES

staging_events_copy = ("""        
    COPY     staging_events   
    FROM     {}               
    IAM_ROLE {}               
    JSON     {}               
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH']
            )

staging_songs_copy = ("""    
     COPY     staging_songs  
     FROM     {}             
     IAM_ROLE {}             
     JSON     'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

staging_events_data_check = ("""        
    COPY     staging_events   
    FROM     {}               
    IAM_ROLE {}               
    JSON     {}   
    NOLOAD
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH']
            )

staging_songs_data_check = ("""    
     COPY     staging_songs  
     FROM     {}             
     IAM_ROLE {}             
     JSON     'auto'
     NOLOAD
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
	INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, 
                           session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)   
""")

users_table_insert = ("""
	INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) 
""")

songs_table_insert = ("""
	INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)  
""")

artists_table_insert = ("""
	INSERT INTO artists (artist_id, name, location, lattitude, longitude) 
         VALUES (%s, %s, %s, %s, %s)  
""")

time_table_insert = ("""
	INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)  
""")

artists_select = ("""
        SELECT DISTINCT artist_id, artist_latitude, artist_longitude, artist_location, artist_name 
            FROM staging_songs ss 
            JOIN staging_events se   ON ss.title = se.song 
	        WHERE abs(ss.duration - se.length) <1.0
       """)
songs_select = ("""
        SELECT DISTINCT artist_id, song_id, title, duration, year 
            FROM staging_songs ss 
            JOIN staging_events se   ON ss.title = se.song 
	        WHERE abs(ss.duration - se.length) <1.0
        """)
songplay_select = ("""
    SELECT se.ts, se.userid, se.level, ss.song_id, ss.artist_id, se.page, se.sessionid, se.location, se.useragent
	FROM staging_events se 
    JOIN staging_songs ss ON se.song = ss.title  
	WHERE abs(ss.duration - se.length) <1.0
""")

# QUERY LISTS

create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [songplay_table_create, users_table_create,
                           songs_table_create, artists_table_create, time_table_create]
drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_table_queries = [songplay_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert,
                        songs_table_insert, artists_table_insert, time_table_insert]
