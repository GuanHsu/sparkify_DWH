import configparser
import pandas as pd
import boto3

def prettyRedshiftProps(props):
    """
    Description: 
        Pretty Print AWS Redshift DWH config

    Arguments:  
        props 

    Returns:  
        CLUSTER_IDENTIFIER 
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def start_DWH (AWS_DWH_ConfigFile):
    """
    Description: 
        Set up a new AWS Redshift DWH Cluster based on provided config

    Arguments:  
        AWS_DWH_ConfigFile - Config for an exisiting Redshift cluster.

    Returns:  
        CLUSTER_IDENTIFIER - A string used as identifier of the AWS Redshift Cluster.  Return just for convinience.
    """   
    config = configparser.ConfigParser()
    config.read(AWS_DWH_ConfigFile)

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    ### Create a RedShift Cluster

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

    print('Do not proceed until the Cluster is up and available.  Then run get_DWH_Info to get info needed to run DWH activities')

    return DWH_CLUSTER_IDENTIFIER

def get_DWH_Info (DWH_CLUSTER_IDENTIFIER):
    """
    Description: 
        Get information to a AWS Redshift DWH Cluster based on CLUSTER IDENTIFER

    Arguments:  
        AWS_DWH_ConfigFile - Config for an exisiting Redshift cluster.

    Returns:  
        myClusterPro - A cluster property object for the newly started Redshift Cluster
    """   
    newClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(newClusterProps)

    DWH_ENDPOINT = newClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = newClusterProps['IamRoles'][0]['IamRoleArn']

    return newClusterProps

def open_DWH_Port (newClusterProps, port):
    """
    Description: 
        Open an incoming  TCP port to access the cluster ednpoint.  This should only need to be done once for a Cluster.

    Arguments:  
        newClusterProps - Config Obj for an exisiting Redshift cluster.
        PORT - The port to be open for access

    Returns:  
        None
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)

def main():
    
    #  We can put the config as an input parameter.
    AWS_DWH_ConfigFile = 'dwh_sp.cfg'

    config = configparser.ConfigParser()
    config.read(AWS_DWH_ConfigFile)

    DWH_PORT               = config.get("DWH","DWH_PORT")

    CLUSTER_IDENTIFIER = start_DWH(AWS_DWH_ConfigFile)
    #  This program should have the option to add to the config 
    newClusterProp = get_DWH_Info (CLUSTER_IDENTIFIER) 

    open_DWH_Port (newClusterProp, DWH_PORT)


if __name__ == "__main__":
    main()