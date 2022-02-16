# Problem description:
Given a simple tab separated file which contains "hit level data". A hit level record is a single "hit" from a visitor on the client's site. Based on the client's implementation, several variables can be set and sent to Adobe Analytics for deeper analysis. Exercise is to write a Python application that is capable of reading this hit level data file and answer the client's question which is:

        How much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?
        
   
 
   
# Solution:

1. Copy the source hit data TSV file and the [pyspark script](https://github.com/sailendrakalyanam/Adobe-assessment/blob/main/com/sailendra/data/Analytics.py) that answers the client's question to S3. For convenience, result is shown [here](https://github.com/sailendrakalyanam/Adobe-assessment/blob/main/final_result)
      
2. Create a [Lambda function](https://github.com/sailendrakalyanam/Adobe-assessment/blob/main/serverless-deployment-script.py) that has a trigger on the above S3 bucket for TSV files. This function would spin up a transient EMR cluster with step to run this pyspark script and auto-terminates upon script's execution.
