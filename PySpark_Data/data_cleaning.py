from PySpark_Data.data_schema import Original_Schema
from GCP_Functions.upload_to_gcp import Upload_To_GCP

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import isnan, when, count, col, date_format, year, month, dayofmonth, lag,\
round, regexp_replace, max, min, avg, stddev
from pyspark.sql.window import Window

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import ticker

class Data_Cleaning_Stock:
    def read_in_data(self):
        spark = SparkSession.builder.appName("stock").getOrCreate()
        sc = spark.sparkContext
        data_file = "gs://stock-sp500/Data/S&P_500_Full_Stock_Data.csv"

        og_schema = Original_Schema()
        stock_schema = og_schema.full_stock_data_schema()

        self.stock_df = spark.read.csv(data_file,
                            header = True,
                            schema = stock_schema).cache()

        return self.stock_df

    def null_value_analysis(self)
        # Only looking at ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'] columns because the schema defined the other columns as not nullable.
        null_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        eda_log_string = ""

        # 1. Number of Null values per column
        nulls_test = self.stock_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in null_columns])
        eda_log_string += f"{datetime.now()}: {nulls_test._jdf.showString(20, 0, False)}\n"

        # 2. Creating dataframe with the null values
        agg_expression = [F.sum(when(self.stock_df[x].isNull(), 1).otherwise(0)).alias(x) for x in null_columns]
        null_values_by_stock = self.stock_df.groupby("Symbol").agg(*agg_expression)
        null_values_by_stock = null_values_by_stock.withColumn('Missing Values Sum', sum([F.col(c) for c in null_columns]))
        null_values_by_stock = null_values_by_stock.filter(null_values_by_stock["Missing Values Sum"] > 0)
        eda_log_string += f"{datetime.now()}: {null_values_by_stock._jdf.showString(20, 0, False)}\n"

        # 3. Counting the number of missing values
        stock_df_missing_values = self.stock_df.filter(col("Open").isNull()|col("High").isNull()\
                                                 |col("Low").isNull()|col("Close").isNull()\
                                                 |col("Adj Close").isNull()|col("Volume").isNull())
        num_misssing_rows = "There are {} rows with missing values.".format(stock_df_missing_values.count())
        eda_log_string += f"{datetime.now()}: {num_misssing_rows}\n"

        #4. Missing values heatmap visualization per ticker
        pandas_missing_values = stock_df_missing_values.toPandas()
        missing_stock_symbols = pandas_missing_values['Symbol'].unique().tolist()

        n_cols = 2
        n_rows = int(np.ceil(len(missing_stock_symbols)/n_cols))
        fig, axes = plt.subplots(nrows=n_rows, 
                                ncols=n_cols, 
                                figsize=(30,50))

        for _, ax in zip(missing_stock_symbols, axes.flatten()):
            pandas_missing_values_t = pandas_missing_values[pandas_missing_values['Symbol'] == _]
            pandas_missing_values_t = pandas_missing_values_t[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
            pandas_missing_values_t.set_index("Date", inplace = True, drop = True)
            pandas_missing_values_t = pandas_missing_values_t.T
            np_missing_values_array = pandas_missing_values_t.values
            
            missing_dates_array = np.array(pandas_missing_values_t.columns)
            missing_dates_array = missing_dates_array.astype('datetime64[D]')
            missing_dates_array = np.datetime_as_string(missing_dates_array)
            missing_dates_list = missing_dates_array.tolist()

            # Plot heatmap
            ax.set_title(_, fontsize=30, weight='bold')
            ax.set_ylabel('Stock Data', fontsize=20, weight='bold')
            ax.set_xlabel('Date Index', fontsize=20, weight='bold')
            
            positions = [0, 1, 2, 3, 4, 5]
            labels = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            ax.yaxis.set_major_locator(ticker.FixedLocator(positions))
            ax.yaxis.set_major_formatter(ticker.FixedFormatter(labels))
            ax.yaxis.set_tick_params(labelsize=20)

            ax.imshow(np_missing_values_array)
            
            h, w = np_missing_values_array.shape
            ax.set_aspect(w/h)
            
        fig.delaxes(axes[n_rows - 1, -1])
        plt.tight_layout() 
        
         








    # from google.cloud import storage

    # # def write_to_blob(bucket_name,file_name):
    # storage_client = storage.Client()
    # bucket = storage_client.bucket("stock-sp500")
    # blob = bucket.blob("Logs/eda23.txt")
    # blob.upload_from_string("{}".format(nulls_test._jdf.showString(20, 0, False)))