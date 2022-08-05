from data_schema import Original_Schema
from upload_to_gcp import Upload_To_GCP

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import isnan, when, count, col, date_format, year, month, dayofmonth, lag, regexp_replace
from pyspark.sql.window import Window

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker

class Data_Cleaning_Stock:
    def __init__(self):
        self.gcp_functions = Upload_To_GCP()
        

    def read_in_data_data_cleaning(self):
        spark = SparkSession.builder.appName("stock").getOrCreate()
        sc = spark.sparkContext
        data_file = "gs://stock-sp500/Data/S&P_500_Full_Stock_Data.csv"

        og_schema = Original_Schema()
        stock_schema = og_schema.full_stock_data_schema()

        self.stock_df = spark.read.csv(data_file,
                            header = True,
                            schema = stock_schema).cache()

        return self.stock_df

    def null_value_analysis(self):
        # Only looking at ['Open', 'High', 'Low', 'Close', 'Volume'] columns because the schema defined the other columns as not nullable.
        null_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        eda_log_string = ""


        # 1. Number of Null values per column
        nulls_test = self.stock_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in null_columns])
        eda_log_string += f"{datetime.now()}: \n{nulls_test._jdf.showString(20, 0, False)}\n"


        # 2. Creating dataframe with the null values
        agg_expression = [F.sum(when(self.stock_df[x].isNull(), 1).otherwise(0)).alias(x) for x in null_columns]
        null_values_by_stock = self.stock_df.groupby("Symbol").agg(*agg_expression)
        null_values_by_stock = null_values_by_stock.withColumn('Missing Values Sum', sum([F.col(c) for c in null_columns]))
        null_values_by_stock = null_values_by_stock.filter(null_values_by_stock["Missing Values Sum"] > 0)
        eda_log_string += f"{datetime.now()}: \n{null_values_by_stock._jdf.showString(20, 0, False)}\n"


        # 3. Counting the number of missing values
        stock_df_missing_values = self.stock_df.filter(col("Open").isNull()|col("High").isNull()\
                                                 |col("Low").isNull()|col("Close").isNull()\
                                                 |col("Volume").isNull())
        num_misssing_rows = "There are {} rows with missing values.".format(stock_df_missing_values.count())
        eda_log_string += f"{datetime.now()}: \n{num_misssing_rows}\n"


        # 4. Missing values heatmap visualization per ticker
        pandas_missing_values = stock_df_missing_values.toPandas()
        missing_stock_symbols = pandas_missing_values['Symbol'].unique().tolist()

        n_cols = 2
        n_rows = int(np.ceil(len(missing_stock_symbols)/n_cols))
        fig, axes = plt.subplots(nrows=n_rows, 
                                ncols=n_cols, 
                                figsize=(30,50))

        for _, ax in zip(missing_stock_symbols, axes.flatten()):
            pandas_missing_values_t = pandas_missing_values[pandas_missing_values['Symbol'] == _]
            pandas_missing_values_t = pandas_missing_values_t[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
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
            labels = ['Open', 'High', 'Low', 'Close', 'Volume']
            ax.yaxis.set_major_locator(ticker.FixedLocator(positions))
            ax.yaxis.set_major_formatter(ticker.FixedFormatter(labels))
            ax.yaxis.set_tick_params(labelsize=20)

            ax.imshow(np_missing_values_array)
            
            h, w = np_missing_values_array.shape
            ax.set_aspect(w/h)
            
        plt.tight_layout()
        fig.savefig("null_heatmap.png")

        # Uploading this heatmap figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", file_name= "null_heatmap.png", destination_blob_name="Data_Cleaning/null_heatmap.png")


        # 5. Removing Symbols with null values.
        remove_symbols = np.array(null_values_by_stock.select('Symbol').collect()).reshape(-1)
        for value in remove_symbols:
            cond = (F.col('Symbol') == value)
            self.stock_df = self.stock_df.filter(~cond)
        removed_missing_values_1 = self.stock_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in null_columns])

        eda_log_string += f'''\n{datetime.now()}: \n{removed_missing_values_1._jdf.showString(20, 0, False)}\
Now we don't have any missing values in the main data columns we will be using.\n'''


        # 6. Feature Creation
        # Date Features: day_of_week, month, year, day_of_month variables.
        self.stock_df_new = self.stock_df.withColumn("day_of_week", date_format(col("Date"), "EEEE"))\
            .withColumn("year", year(col("Date")))\
                .withColumn("month", month(col("Date")))\
                    .withColumn("day_of_month", dayofmonth(col("Date")))

        # Daily Return Feature: daily_return
        self.stock_df_new = self.stock_df_new.withColumn('daily_return', (self.stock_df_new['Close'] - self.stock_df_new['Open']))

        # Lag Features: lag_1, lag_2, lag_3, lag_4, lag_5, lag_6
        windowSpec = Window.partitionBy("Symbol").orderBy("Symbol")
        self.stock_df_new = self.stock_df_new.withColumn("lag_1", lag("daily_return",1).over(windowSpec))\
            .withColumn("lag_2", lag("daily_return",2).over(windowSpec))\
                .withColumn("lag_3", lag("daily_return",3).over(windowSpec))\
                    .withColumn("lag_4", lag("daily_return",4).over(windowSpec))\
                        .withColumn("lag_5", lag("daily_return",5).over(windowSpec))\
                            .withColumn("lag_6", lag("daily_return",6).over(windowSpec))

        # Cummulative Return Feature:
        cummulative_window = (Window.partitionBy('Symbol').orderBy('Date').rangeBetween(Window.unboundedPreceding, 0))
        self.stock_df_new = self.stock_df_new.withColumn('cumulative_return', F.sum('daily_return').over(cummulative_window))


        # 7. Clean dataframe with new feautures
        stock_df_new_null_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'day_of_week',
        'year', 'month', 'day_of_month', 'lag_1', 'lag_2', 'lag_3', 'lag_4', 'lag_5', 'lag_6', 'daily_return', 'cumulative_return']
        missing_values_2 = self.stock_df_new.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in stock_df_new_null_columns])
        eda_log_string += f'''\n{datetime.now()}: \n{missing_values_2._jdf.showString(20, 0, False)}\
Due to the lag features created, we lost 6 rows per stock symbol.\n'''

        self.stock_df_new = self.stock_df_new.dropna(how='any')
        missing_values3 = self.stock_df_new.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in stock_df_new_null_columns])
        eda_log_string += f'''\n{datetime.now()}: \n{missing_values3._jdf.showString(20, 0, False)}\
Now we don't have any missing values in the main data columns we will be uploading to GCP as the clean dataset.\n'''

        # GICS sector clean up
        self.stock_df_new = self.stock_df_new.\
            withColumn('GICS Sector', regexp_replace('GICS Sector', 'Information technology', 'Information Technology'))


        # 8. Pushing clean stock data with features into GCP bucket
        self.stock_df_new.coalesce(1)\
                .write\
                    .option('header', 'true')\
                        .csv('gs://stock-sp500/Data/S&P_500_Clean_Data.csv', mode='overwrite')

        eda_log_string += f'''\n{datetime.now()}: \n\
Clean data uploaded to GCP SUCCESFULLY.'''        


        # 9. Uploading compiled strings into GCP bucket as a text file called eda_test.txt
        self.gcp_functions.upload_string_message(bucket_name="stock-sp500", contents=eda_log_string, destination_blob_name="Data_Cleaning/data_cleaning_log.txt")