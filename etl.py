import configparser
import psycopg2
from sql_queries import *
from create_tables import *
import pandas as pd
import boto3
import json
import datetime as dt

#-------------------------------------------------------------------
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


#-------------------------------------------------------------------
def insert_songs_table (cur, conn):
    """
     Description: Populate songs dimention Table  in Sparkify database 
                  Data are query from staging based on queries defined in sql_queries.py
                  Some minimal data quality check is done at insertion

     Arguments:  cur - cursor of the database connection
                 conn - connection to the target database

     Returns:  None

    """   
    songs_df = pd.read_sql_query(songs_select, conn)

    for i, r in songs_df.iterrows():
        try:
            cur.execute(songs_table_insert, (r.song_id, r.title, r.artist_id, r.year, r.duration))
        except psycopg2.Error as e:
            print('insert_users_data error:\n')
            print(e)
    
#-------------------------------------------------------------------                
def insert_artists_table (cur, conn):
    """
     Description: Populate artists dimention Table  in Sparkify database 
                  Data are query from staging based on queries defined in sql_queries.py
                  Some minimal data quality check is done at insertion

     Arguments:  cur - cursor of the database connection
                 conn - connection to the target database

     Returns:  None

    """   
    artists_df = pd.read_sql_query (artists_select, conn)
           
    for i, r in artists_df.iterrows():
        try:
            cur.execute(artists_table_insert, (r.artist_id, r.artist_name, r.artist_location, r.artist_latitude, r.artist_longitude))
        except psycopg2.Error as e:
            print('insert_artists_data error:\n')
            print(e)
                
    
#-------------------------------------------------------------------               
def insert_users_table (cur, conn):
    """
     Description: Populate users dimention Table  in Sparkify database 
                  Data are query from staging based on queries defined in sql_queries.py
                  Some minimal data quality check is done at insertion

     Arguments:  cur - cursor of the database connection
                 conn - connection to the target database

     Returns:  None

    """   
    cur.execute('SELECT DISTINCT userid, firstName, lastName, gender, level  from staging_events')
    users_list = cur.fetchall()

    for r in users_list:
        if isinstance (r[0], int):
            try:
                cur.execute(users_table_insert, (r[0], r[1], r[2], r[3], r[4]))
            except psycopg2.Error as e:
                print('insert_users_data error:\n')
                print(e)

    
#-------------------------------------------------------------------
def insert_time_table (cur, conn):
    """
     Description: Populate time dimention Table  in Sparkify database 
                  Data are query from staging based on queries defined in sql_queries.py
                  Some minimal data quality check is done at insertion

     Arguments:  cur - cursor of the database connection
                 conn - connection to the target database

     Returns:  None

    """   
    
    cur.execute('SELECT DISTINCT ts  from staging_events')
    ts_list = cur.fetchall()

    for r in ts_list:
        ts = int(r[0])
        dts = dt.datetime.fromtimestamp(ts/1000.0)
        start_time = dts.isoformat()
        week_of_year = dts.isocalendar()[1]
        #  start_time, hour, day, week, month, year, weekday
        try:
            cur.execute(time_table_insert, (start_time, dts.hour, dts.day, week_of_year, dts.month, dts.year, dts.weekday()))
        except psycopg2.Error as e:
            print('insert_users_data error:\n')
            print(e)

    
#-------------------------------------------------------------------
def insert_dimension_tables (cur, conn):
    """
    Description: Populate Dimention Tables in Sparkify database 
                Data are query from staging based on queries defined in sql_queries.py
                Some minimal data quality check is done at insertion

    Arguments:  cur - cursor of the database connection
                conn - connection to the target database

    Returns:  None

    """   
    print("insert_artists_table")
    insert_artists_table (cur, conn)

    print("insert_users_table")
    insert_users_table (cur, conn)

    print("insert_time_table")
    insert_time_table (cur, conn)

    print("insert_songs_table")
    insert_songs_table (cur, conn)

#-------------------------------------------------------------------
def insert_songplay_table (cur, conn):
    """
     Description: Populate Dimention Tables in Sparkify database 
                  Data are query from staging based on queries defined in sql_queries.py
                  Some minimal data quality check is done at insertion
    Arguments:  cur - cursor of the database connection
                conn - connection to the target database
    Returns:  None
    """
    
    column_name = ('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')

    df=pd.read_sql_query(songplay_select, conn)
    df=df[df.page == 'NextSong']
    
    for i, r in df.iterrows():
        dts = dt.datetime.fromtimestamp(r.ts/1000.0)
        start_time = dts.isoformat()
        try:
            #print((start_time, r.userid, r.level, r.song_id, r.artist_id, r.sessionid, r.location, r.useragent) )
            cur.execute(songplay_table_insert, (start_time, r.userid, r.level, r.song_id, r.artist_id, r.sessionid, r.location, str(r.useragent) ) )
        except psycopg2.Error as e:
            print('insert_users_data error:\n')
            print(e) 

#-------------------------------------------------------------------   
def main():

    print('\n\n ETL load for Sparkify DWH: \n\n 1.    Connect to Sparkify DWH:\n')
    print(dt.datetime.now())
    cur, conn = connect_DWH_db ('dwh-sp.cfg')

    print('\n\n 2.    Create Tables:\n')
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    print('\n\n 3.    Load Staging Tables:\n')
    print(dt.datetime.now())
    load_staging_tables(cur, conn)
    
    print('\n\n 4.    Load Dimensional Tables:\n')
    print(dt.datetime.now())
    insert_dimension_tables (cur, conn)
    
    print('\n\n 5.    Load Fact Table songplay:\n')
    print(dt.datetime.now())
    insert_songplay_table (cur, conn)
    
    print('\n\n ETL Load Process Completed.\n')
    cur.close()
    conn.close()
    print(dt.datetime.now())


if __name__ == "__main__":
    main()