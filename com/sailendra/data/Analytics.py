"""
Analytics.py
~~~~~~~~~~

This Python module contains a PySpark ETL job
that implements analytics on hit data feed. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,
    $spark-submit ~/Analytics.py <input-file>

"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from decimal import Decimal
import datetime
import sys
import pyspark.sql.functions as f

class Analytics(object):
    def __init__(self, input_file_location):
        self.input_file_location = input_file_location
        self.spark = SparkSession.builder.appName('hit_list_analytics').getOrCreate()
        self.formatted_file_location = ''

    def format(self) -> str():
        file_name = self.input_file_location.split('/')[-1]
        if(file_name):
            folder_path_ends_at = self.input_file_location.rfind('/') + 1
            escape_special_characters = file_name.translate({ord(c): "\\"+c for c in "!@#$%^&*()[]{};:,/<>?\|`~-=_+"})
            self.formatted_file_location = self.input_file_location[:folder_path_ends_at] + escape_special_characters
        return self.formatted_file_location

        
    def read_input_file(self) -> DataFrame:
        
        #Explicit schema definition
        customSchema = StructType([
            StructField('hit_time_gmt', StringType(), nullable=False),
            StructField('date_time', StringType(), nullable=False),
            StructField('user_agent', StringType(), nullable=True),
            StructField('ip', StringType(), nullable=False),
            StructField('event_list', StringType(), nullable=True),
            StructField('geo_city', StringType(), nullable=True),
            StructField('geo_region', StringType(), nullable=True),
            StructField('geo_country', StringType(), nullable=True),
            StructField('pagename', StringType(), nullable=True),
            StructField('page_url', StringType(), nullable=True),
            StructField('product_list', StringType(), nullable=True),
            StructField('referrer', StringType(), nullable=True)
        ])
        
        #Read TSV from S3 and cache it due to eventual frequent reads
        hit_df = self.spark.read.options(delimiter='\t').csv(self.formatted_file_location,header=True,schema=customSchema,inferSchema=False).select('product_list', 'ip', 'referrer','event_list').cache()
        hit_df.createOrReplaceTempView("hit_list")
        
        return hit_df

    def fetch_external_sources(self, hit_df) -> DataFrame:
        return self.spark.sql("select ip, referrer, split(referrer,'/')[2] as Search_Engine_Domain from hit_list where split(referrer,'/')[2] NOT LIKE '%esshopzilla.com'")

        
    def calculate_total_revenue(self, hit_df) -> DataFrame:  
 
        # To calculate total revenue, fetch only those rows that are of 'Purchase' event
        raw_product_list_df = self.spark.sql(
            "select ip, split(product_list,',') as split_product_list from hit_list where event_list = '1'"
        ).collect()
        
        #store list of dictionaries to later convert it to dataframe
        result = []
        
        #To access revenue of a product/products for given row in source feed file, this method splits the product_list column
        # first by comma delimiter to get each entry of product's attributes
        # and further split each product attribute by semi-colon delimiter to access its revenue variable.
        for row in raw_product_list_df:
            temp = row.asDict()  #convert row object to dictionary to easily iterate it
            total_revenue = 0
            for i in range(len(row['split_product_list'])):
                p_list = str(row['split_product_list'][i])
                total_revenue += Decimal(p_list.split(';')[3])  #calculate revenue for each entry and cast it to Decimal for accuracy
                temp['Revenue'] = total_revenue  #add revenue value to the dictionary
                del temp['split_product_list']  #delete this entry from dictionary to optimize the space
                result.append(temp)
        
        # schema to determine the dataframe after calculating revenue of products
        schema = StructType([
            StructField("ip", StringType(), True),
            StructField("Revenue", DecimalType(), True)
        ])
        
        #finally, created a dataframe from the list of dicts
        revenue_add_df = self.spark.createDataFrame(result, schema)
        
        revenue_positive_df = revenue_add_df.where(revenue_add_df['Revenue'] > 0)
        
        return revenue_positive_df

    
    
    def find_keyword(self, external_source_df) -> DataFrame:
        #Yahoo URLs and the rest of the search engine URLs differ at the letter 'p' and 'q' respectively before the search keyword.
        #So extracting it out in form of two columns:
            #1. partialURL1 that captures keyword for rest of the search engine URLs and
            #2. partialURL2 to capture keyword for Yahoo domain
            
        keyword_df = external_source_df.withColumn(
            "partialURL1",
            f.split('referrer', "q=")[1]).withColumn(
                "partialURL2",
                f.split('referrer', "p=")[1]).select(
                    'ip', 'referrer',
                    f.substring_index(
                        'partialURL1', '&', 1).alias('partialKeyWord1'),
                    f.substring_index(
                        'partialURL2', '&', 1).alias('partialKeyWord2')).fillna('-')
        
        keyword_df = keyword_df.drop('referrer') 
        
        #Merging partialURL1 and partialURL2 to a final column for keyword called 'Search Keyword'
        final_keyword_df = keyword_df.withColumn('Search Keyword',
            f.lower(
                f.regexp_replace(
                    f.concat('partialKeyWord1', 'partialKeyWord2'), '-',
                                 '')
                )
            ).select('Search Keyword', 'ip')
            
        return final_keyword_df
    


    def join_the_dfs(self, external_source_df, revenue_positive_df, search_keyword_df) -> DataFrame:
        return revenue_positive_df.join(external_source_df.drop('referrer'), ["ip"]).join(search_keyword_df, ["ip"]).drop("ip")

    

    def sort_by_revenue(self, final_df) -> DataFrame:
        return final_df.groupBy("Search_Engine_Domain", "Search Keyword").agg(f.sum('Revenue').alias('Revenue')).sort(f.desc('Revenue'))
        
        
    def save_to_s3(self, result_df):
        currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
        destination = 's3://adobe-sailendra-staging/' + currentdate + '_SearchKeyWordPerformance.tab'

        result_df.write.format("com.databricks.spark.csv") \
        .option('header','true').option("delimiter","\t") \
        .save(destination,mode='overwrite')
        
        #Verification
        self.spark.read.options(header=True)    \
        .options(delimiter='\t')    \
        .csv(destination+'/*.csv')  \
        .show(truncate=False)

    

if __name__ == "__main__":
    obj = Analytics(str(sys.argv[1]))   
    
    #Escape any special characters included in the input file name
    formatted_file_location = obj.format()
    
    #Read input file(s)
    hit_df = obj.read_input_file()
    
    ###### End goal is to A JOIN B JOIN C on unique column 'IP' where A, B and C are defined below #######
    
    #A. Fetch rows from external websites
    external_source_df = obj.fetch_external_sources(hit_df)
    
    #B. Fetch rows with actualized revenue
    revenue_positive_df = obj.calculate_total_revenue(hit_df)
    
    #C. Fetch rows with search keyword
    search_keyword_df = obj.find_keyword(external_source_df)
        
    # JOIN A, B and C to fetch hits that generated revenue that are sourced from search engines along with its search keyword   
    final_df = obj.join_the_dfs(external_source_df, revenue_positive_df, search_keyword_df)

    #Finally, sum the revenue w.r.t its external search engine domain AND respective search keyword.
    result_df = obj.sort_by_revenue(final_df)
    
    #Save it to S3
    obj.save_to_s3(result_df)
        