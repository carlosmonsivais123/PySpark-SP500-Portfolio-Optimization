from data_schema import Original_Schema
from upload_to_gcp import Upload_To_GCP

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import isnan, when, count, col, date_format, year, month, dayofmonth, lag,\
round, regexp_replace, max, min, avg, stddev
from pyspark.sql.window import Window

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import ticker
import re

class EDA_Plots:
    def __init__(self):
        self.gcp_functions = Upload_To_GCP()

    def read_in_data_data_cleaning(self):
        spark = SparkSession.builder.appName("stock_clean").getOrCreate()
        sc = spark.sparkContext

        gcp_data_files = self.gcp_functions.list_blobs(bucket_name = "stock-sp500")
        clean_data_file_name = [x for x in gcp_data_files if re.match(r'Data/S&P_500_Clean_Data.csv/part*',x)][0]

        full_bucket_name = "gs://stock-sp500/"+clean_data_file_name

        og_schema = Original_Schema()
        stock_schema = og_schema.full_stock_data_schema()

        self.stock_df_clean = spark.read.csv(full_bucket_name,
                                             header = True,
                                             schema = stock_schema).cache()

        return self.stock_df_clean

    def eda_plot_creation(self):
        # Category Counts
        