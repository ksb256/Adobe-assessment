#1. Upload hit-data file to S3 that triggers the Lambda function (#2)
aws s3 cp Data[82].tsv s3://adobe-sailendra-staging/

#2. Create a Lambda function that spins up EMR cluster, runs the pyspark script and uploads the result tsv files to a S3 location (serverless)
"""
Key parameters
-Name (Name of Spark cluster)
-LogUri (S3 bucket to store EMR logs)
-Ec2SubnetId (The subnet to launch the cluster into)
-JobFlowRole (Service role for EC2)
-ServiceRole (Service role for Amazon EMR)
The following parameters are additional parameters for the Spark job itself. Change the bucket name and prefix for the Spark job (located at the bottom).
-s3://adobe-sailendra-staging/Analytics.py (PySpark file)
-s3://adobe-sailendra-staging/data[82].tsv (Input data file in S3)
"""
import json
import boto3


client = boto3.client('emr')


def lambda_handler(event, context):
    print('start')
    response = client.run_job_flow(
        Name= 'spark_job_cluster',
        LogUri= 's3://aws-logs-463961516505-us-east-2/elasticmapreduce/',
        ReleaseLabel= 'emr-5.34.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.large',
            'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-4a059206'
        },
        Applications = [ 
            {
                'Name': 'Spark'
            },
            {
                'Name': 'Hive'
            }
        ],
        Configurations = [
            { 'Classification': 'spark-hive-site',
              'Properties': {
                  'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole = 'EMR_EC2_DefaultRole',
        ServiceRole = 'EMR_DefaultRole',
        Steps=[
            {
                'Name': 'flow-log-analysis',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    "Jar": "command-runner.jar",
                    'Args': [
                        'spark-submit',
                        's3://adobe-sailendra-staging/Analytics.py',
                        's3://adobe-sailendra-staging/data[82].tsv'
                    ]
                }
            }
        ]
    )
    print(response)
