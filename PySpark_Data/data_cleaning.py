from PySpark_Data.data_schema import Original_Schema

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import isnan, when, count, col, date_format, year, month, dayofmonth, lag,\
round, regexp_replace, max, min, avg, stddev
from pyspark.sql.window import Window

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import ticker

class Data_Cleaning_Stock:
    def clean_full_stock_data():
        spark = SparkSession.builder.appName("stock").getOrCreate()
        sc = spark.sparkContext
        data_file = "gs://stock-sp500/Data/S&P_500_Full_Stock_Data.csv"

        og_schema = Original_Schema()
        stock_schema = og_schema.full_stock_data_schema()

        stock_df = spark.read.csv(data_file,
                            header = True,
                            schema = stock_schema).cache()

        