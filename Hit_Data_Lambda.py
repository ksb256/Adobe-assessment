"""
Key parameters

-Name (Name of Spark cluster)
-LogUri (S3 bucket to store EMR logs)
-Ec2SubnetId (The subnet to launch the cluster into)
-JobFlowRole (Service role for EC2)
-ServiceRole (Service role for Amazon EMR)

The following parameters are additional parameters for the Spark job itself.

-s3://adobe-sailendra-staging/adobe-hit-data-analytics.py (PySpark file)
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
        ReleaseLabel= 'emr-6.0.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.large',
            'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-4a059206'
        },
        Applications = [ {'Name': 'Spark'} ],
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
                            '--deploy-mode', 'cluster',
                            '--conf','spark.yarn.submit.waitAppCompletion=false',
                            's3://adobe-sailendra-staging/adobe-hit-data-analytics.py',
                            's3://adobe-sailendra-staging/data[82].tsv'
                        ]
                }
            }
        ]
    )
