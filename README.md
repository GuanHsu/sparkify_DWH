# Project: ETL pipeline for Sparkify  DWH on AWS

### Introduction

We model data for a startup called Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

We build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytical team to find insight in what songs their users are listening to.  

### Running the ETL pipeline

#### Files:

One needs the following files to run the ETL pipeline:

    create_tables.py
    etl.py
    sql_queries.py
    dwh-sp.cfg - Please note the following values are removed from the cfg files
    
        ARN
        KEY 
        SECRETE 
        HOST 
        
You need to update these values with the values of your cluster before running.
    

###### Running 

After you verify that the files and data source are available, and the Amazon Redshift Cluster defined by the config file dwh-sp.cfg.

Note: We need to have access to a healthy Redshift Cluster/  If you don't already have one ready, please see the Appendix on steps to start a cluster.


### Database design

The database sparkify is designed with a Star schema with 1 fact table : 


#### Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong

    *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimensional Tables
    users - users in the app.
        *user_id, first_name, last_name, gender, level*
    songs - songs in music database
        *song_id, title, artist_id, year, duration*
    artists - artists in music database
        *artist_id, name, location, lattitude, longitude*
    time - timestamps of records in songplays broken down into specific units
        *start_time, hour, day, week, month, year, weekday*

#### Source Data files

Data files needs to be provided in the directory under the workspace

    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`
    - Log data `json path: s3://udacity-dend/log_json_path.json`

#### Source Data Sample:

The source data for Sparkify ETL are saved in S3 bucket.  Both data sets are in JSON format :

**Song Data: **

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

**Event Data **

<a href='staging_events_data_sample.PNG'>Event data sample</a>


###### Testing 

Two notebook files are provided for effective test & help with the development of the pipeline

    test.ipynb   - Simple steps are provided to query the 5 tables and pull a small number of entries in the tables to verify that the tables are indeed created and filled properly.
    etl.ipynb   




#### Appendix: DWH on AWS

There should already be a running Amazon Redshift Cluster serving as our data warehouse on AWS as defined by the config file

    dwh-sp.cfg
    
If the DWH Cluster is not already running, you need to update the values and run (from python prompt):

    >>> ID = start_DWH('dwh-sp.cfg')
    
Once that is started, you need to monitor the status of the new Redshift cluster on AWS Management Console.  Only after the staus becomes 'available', you can then run

    >>> props = get_DWH_Info (ID, 'dwh-sp.cfg')
    
Sometimes you need to open the port for accessing cluster.   Just run this:  

    >>> open_DWH_Port (ID, 'dwh-sp.cfg')

