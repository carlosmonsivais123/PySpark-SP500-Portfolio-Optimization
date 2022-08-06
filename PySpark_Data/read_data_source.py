from data_schema import Original_Schema
from upload_to_gcp import Upload_To_GCP
from pyspark.sql import SparkSession

import re

class Read_In_Data_Source:
    def __init__(self):
        self.gcp_functions = Upload_To_GCP()



    def read_original_data(self):
        spark = SparkSession.builder.appName("stock").getOrCreate()
        sc = spark.sparkContext
        data_file = "gs://stock-sp500/Data/S&P_500_Full_Stock_Data.csv"

        og_schema = Original_Schema()
        stock_schema = og_schema.full_stock_data_schema()

        self.stock_df = spark.read.csv(data_file,
                            header = True,
                            schema = stock_schema).cache()

        return self.stock_df


    def read_in_data_data_cleaning(self):
        spark = SparkSession.builder.appName("stock_clean").getOrCreate()
        sc = spark.sparkContext

        gcp_data_files = self.gcp_functions.list_blobs(bucket_name = "stock-sp500")
        clean_data_file_name = [x for x in gcp_data_files if re.match(r'Data/S&P_500_Clean_Data.csv/part*',x)][0]

        full_bucket_name = "gs://stock-sp500/"+clean_data_file_name

        og_schema = Original_Schema()
        clean_stock_schema = og_schema.clean_stock_data_schema()

        self.stock_df_clean = spark.read.csv(full_bucket_name,
                                                header = True,
                                                schema = clean_stock_schema).cache()

        return self.stock_df_clean