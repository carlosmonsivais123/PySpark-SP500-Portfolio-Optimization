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
        clean_stock_schema = og_schema.clean_stock_data_schema()

        self.stock_df_clean = spark.read.csv(full_bucket_name,
                                             header = True,
                                             schema = clean_stock_schema).cache()

        return self.stock_df_clean

    def eda_category_counts(self):
        # Category Counts Data
        tot_count = self.stock_df_clean.count()

        cat2_counts = self.stock_df_clean\
            .groupBy('Category2')\
                .count()\
                    .withColumn('Propotion %', F.round((F.col('count')/tot_count),2))\
                        .orderBy('count', ascending=False)


        cat3_counts = self.stock_df_clean\
            .groupBy('Category3')\
                .count()\
                    .withColumn('Propotion %', F.round((F.col('count')/tot_count),2))\
                        .orderBy('count', ascending=False)


        sector_counts = self.stock_df_clean\
            .groupBy('GICS Sector')\
                .count()\
                    .withColumn('Propotion %', F.round((F.col('count')/tot_count),2))\
                        .orderBy('count', ascending=False)

        year_counts = self.stock_df_clean\
            .groupBy('year')\
                .count()\
                    .withColumn('Propotion %', F.round((F.col('count')/tot_count),2))\
                        .orderBy('year', ascending=True)

        # Category Counts Data to Pandas Dataframe to plot in Matplotlib
        cat2_counts_df = cat2_counts.toPandas()
        cat2_counts_df['str Prop2'] = cat2_counts_df['Propotion %'] * 100
        cat2_counts_df['str Prop2'] = cat2_counts_df['str Prop2'].round(2)
        cat2_counts_df['str Prop2'] = cat2_counts_df['str Prop2'].astype(str) + '%'

        cat3_counts_df = cat3_counts.toPandas()
        cat3_counts_df['str Prop3'] = cat3_counts_df['Propotion %'] * 100
        cat3_counts_df['str Prop3'] = cat3_counts_df['str Prop3'].round(2)
        cat3_counts_df['str Prop3'] = cat3_counts_df['str Prop3'].astype(str) + '%'

        sector_counts_df = sector_counts.toPandas()
        sector_counts_df['str Prop4'] = sector_counts_df['Propotion %'] * 100
        sector_counts_df['str Prop4'] = sector_counts_df['str Prop4'].round(2)
        sector_counts_df['str Prop4'] = sector_counts_df['str Prop4'].astype(str) + '%'

        year_counts_df = year_counts.toPandas()
        year_counts_df['str Prop5'] = year_counts_df['Propotion %'] * 100
        year_counts_df['str Prop5'] = year_counts_df['str Prop5'].round(2)
        year_counts_df['str Prop5'] = year_counts_df['str Prop5'].astype(str) + '%'

        # Creating Figure for Category Counts
        fig, axes = plt.subplots(2,2, figsize=(15, 10), sharey=False)
        fig.suptitle('Category EDA Counts')

        # Category2
        sns.barplot(ax=axes[0,0], x=cat2_counts_df['Category2'], y=cat2_counts_df['count'])
        axes[0,0].set_title("Category2 Stock Types")
        axes[0,0].bar_label(axes[0,0].containers[0], labels = cat2_counts_df['str Prop2'])
        axes[0,0].set(xlabel=None)

        # Category3
        sns.barplot(ax=axes[0,1], x=cat3_counts_df['Category3'], y=cat3_counts_df['count'])
        axes[0,1].set_title("Category3 Stock Types")
        axes[0,1].bar_label(axes[0,1].containers[0], labels = cat3_counts_df['str Prop3'])
        axes[0,1].set(xlabel=None)

        # Category3
        sns.barplot(ax=axes[1,0], x=sector_counts_df['GICS Sector'], y=sector_counts_df['count'])
        axes[1,0].set_title("GICS Stock Types")
        axes[1,0].bar_label(axes[1,0].containers[0], labels = sector_counts_df['str Prop4'])
        axes[1,0].tick_params(labelrotation=90)
        axes[1,0].set(xlabel=None)

        # Category3
        sns.barplot(ax=axes[1,1], x=year_counts_df['year'], y=year_counts_df['count'])
        axes[1,1].set_title("GICS Stock Types")
        axes[1,1].bar_label(axes[1,1].containers[0], labels = year_counts_df['str Prop5'])
        axes[1,1].tick_params(labelrotation=90)
        axes[1,1].set(xlabel=None)

        fig.tight_layout()
        fig.savefig("category_counts.png")

        # Uploading this figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", 
                                           file_name= "category_counts.png", 
                                           destination_blob_name="EDA_Plots/category_counts.png")


    def best_day_of_week_stocks(self):
        # Stock prices by day of the week plot
        best_time_to_buy = self.stock_df_clean.groupBy('day_of_week')\
            .agg(F.round(min('Close'), 2).alias('Close_min'),\
                F.round(max('Close'), 2).alias('Close_max'),\
                    F.round(avg('Close'), 2).alias('Close_avg'),\
                        F.round(F.percentile_approx("Close", 0.5), 2).alias("Close_med"),\
                            F.round(stddev('Close'), 2).alias('Close_stddev'))\
                                .orderBy(when(col("day_of_week") == "Monday", 1)\
                                    .when(col("day_of_week") == "Tuesday", 2)\
                                        .when(col("day_of_week") == "Wednesday", 3)\
                                            .when(col("day_of_week") == "Thursday", 4)\
                                                .when(col("day_of_week") == "Friday", 5))

        # Stock close prices by day of week to Pandas Dataframe to plot in Matplotlib
        best_time_to_buy_df = best_time_to_buy.toPandas()
        best_time_to_buy_df["Close_med"] = best_time_to_buy_df["Close_med"].astype(float).round(2)
        best_time_to_buy_df["Close_avg"] = best_time_to_buy_df["Close_avg"].astype(float).round(2)

        fig, ax1 = plt.subplots(figsize=(12,6))
        best_day_plot = best_time_to_buy_df[["day_of_week", "Close_avg", "Close_med"]].plot.bar(x='day_of_week', ax=ax1)

        for p in best_day_plot.patches:
            best_day_plot.annotate(format(p.get_height()),
                        (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center', va='center',
                        xytext=(0, 9),
                        textcoords='offset points')

        plt.tight_layout()
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Day of the Week")
        plt.ylabel("Close Price")
        plt.title("Close Price by Day of the Week")
        plt.legend(loc="upper right", framealpha=0.15)
        plt.savefig("day_of_week_stock_counts.png")

        # Uploading this figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", 
                                           file_name= "day_of_week_stock_counts.png", 
                                           destination_blob_name="EDA_Plots/day_of_week_stock_counts.png")