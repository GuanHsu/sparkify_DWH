import configparser
import psycopg2
import pandas as pd
from sql_queries import *
import boto3


# -- Set up DWH & DB connections

def connect_DWH_db (AWS_DWH_ConfigFile):
	    """
    Description: 
        Connects to an existing AWS Redshift cluster that is already started using
		the config in the file AWH_DWH_ConfigFile.

    Arguments:  
		AWS_DWH_ConfigFile - Config for an exisiting Redshift cluster.

    Returns:  
        cur - cursor of the database connection
        conn - connection to the target database
    """   
	config = configparser.ConfigParser()
	config.read_file(open(AWS_DWH_ConfigFile))
	
	KEY                    = config.get('AWS','KEY')
	SECRET                 = config.get('AWS','SECRET')
	
	DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
	
	redshift = boto3.client('redshift',
	                       region_name="us-west-2",
	                       aws_access_key_id=KEY,
	                       aws_secret_access_key=SECRET
                       )
	
	myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
	DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
	DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
	
	HOST                = DWH_ENDPOINT
	DB_NAME             = config.get("DWH","DWH_DB")
	DB_USER             = config.get("DWH","DWH_DB_USER")
	DB_PASSWORD         = config.get("DWH","DWH_DB_PASSWORD")
	DB_PORT             = config.get("DWH","DWH_PORT")
	
	conn_string = "host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)
	print_conn_string = "host={} dbname={} user={} password=XXXXX port={}".format(HOST, DB_NAME, DB_USER, DB_PORT)
	print(print_conn_string)
	
	conn = psycopg2.connect(conn_string)
	conn.set_session(autocommit=True)
	cur = conn.cursor()	
	
	return cur, conn

def run_queries (cur, conn, queries):
    """
    Description: 
        Executes queries

    Arguments:  
        cur - cursor of the database connection
        conn - connection to the target database
		queries - A list of queries

    Returns:  
        None
    """
	for q in queries:
        try:
            cur.execute(q)
        except psycopg2.Error as e:
            print("Error: failed to execute query {} ".format(q))
            print(e)
        conn.commit()

def drop_tables(cur, conn):
    """
    Description: 
        Drop all Tables as defined in global variable drop_table_queries

    Arguments:  
        cur - cursor of the database connection
        conn - connection to the target database

    Returns:  
        None
    """   
    run_queries (cur, conn, drop_table_queries)


def create_tables(cur, conn):
    """
    Description: 
        Drop all Tables as defined in global variable drop_table_queries

    Arguments:  
        cur - cursor of the database connection
        conn - connection to the target database

    Returns:  
        None
    """   
	run_queries (cur, conn, create_table_queries)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()