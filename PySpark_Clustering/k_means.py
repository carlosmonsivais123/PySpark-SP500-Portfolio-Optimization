from upload_to_gcp import Upload_To_GCP
from read_data_source import Read_In_Data_Source

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import avg, variance, col
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import re


class K_Means_Stocks_Clustering:
    def __init__(self):
        self.gcp_functions = Upload_To_GCP()
        self.read_in_data_source = Read_In_Data_Source()
        self.stock_df_clean = self.read_in_data_source.read_in_data_data_cleaning()


    def daily_returns_avg_var_cluster(self):
        self.k_means_log = ""
        number_of_clusters = 4

        k_means_set = self.stock_df_clean.select('Symbol', 'Date', 'daily_return', 'GICS Sector')
        cluster_set = k_means_set.groupBy('Symbol')\
            .agg(F.round(avg('daily_return'), 4).alias('avg(daily_return)'), F.round(variance('daily_return'), 4).alias('variance(daily_return)'))

        assemble=VectorAssembler(inputCols=['avg(daily_return)', 'variance(daily_return)'], outputCol='features')
        assembled_data=assemble.transform(cluster_set)

        scale=StandardScaler(inputCol='features',outputCol='standardized')
        data_scale=scale.fit(assembled_data)
        data_scale_output=data_scale.transform(assembled_data)


        evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                        metricName='silhouette', distanceMeasure='squaredEuclidean')
            
        KMeans_algo=KMeans(featuresCol='standardized', k=number_of_clusters)
        KMeans_fit=KMeans_algo.fit(data_scale_output)
        output=KMeans_fit.transform(data_scale_output)
        score=evaluator.evaluate(output)

        self.k_means_log += f"{datetime.now()}: \nThe Number of Clusters is: {number_of_clusters}\n"
        self.k_means_log += f"\n{datetime.now()}: \nThe Silhouette score is: {score}\n"
        self.gcp_functions.upload_string_message(bucket_name="stock-sp500", contents=self.k_means_log, destination_blob_name="Clustering/k_means_log.txt")
    
        cluster_df = output.withColumn("xs", vector_to_array("standardized")) \
            .select(['Symbol','avg(daily_return)', 'variance(daily_return)', 'prediction'] + [col("xs")[i] for i in range(2)])


        final_cluster_df = cluster_df.join(k_means_set, 'Symbol', how = 'left').\
            select('Symbol', 'avg(daily_return)', 'variance(daily_return)', 'prediction','xs[0]','xs[1]', 'GICS Sector').\
                distinct().toPandas()

        final_cluster_df.to_csv('gs://stock-sp500/Clustering/k_means_clustered_data.csv', index = False)

        final_cluster_df['prediction'] = final_cluster_df['prediction'].astype('str')
        final_cluster_df.rename({"xs[0]": "daily_return_standardized", "xs[1]": "volume_standardized"}, axis = "columns", inplace = True)

        new_final_cluster_df = final_cluster_df[final_cluster_df["variance(daily_return)"]<=10]


        # Counts per Cluster
        counts_per_cluster = sns.countplot(x="prediction", data=final_cluster_df, order=['0','1','2','3'])
        counts_per_cluster.bar_label(counts_per_cluster.containers[0])
        plt.title('Counts by Cluster')
        plt.savefig("counts_per_cluster.png")

        # Uploading this figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", 
                                           file_name= "counts_per_cluster.png", 
                                           destination_blob_name="Clustering/counts_per_cluster.png")


        fig, axs = plt.subplots(ncols=2, figsize=(15,5))
        all_data = sns.scatterplot(data=final_cluster_df, 
                            x="avg(daily_return)", 
                            y="variance(daily_return)", 
                            hue="prediction",
                            alpha=0.75,
                        hue_order = ['0', '1', '2', '3'], ax=axs[0], legend = False)

        zoomed_in = sns.scatterplot(data=new_final_cluster_df, 
                        x="avg(daily_return)", 
                            y="variance(daily_return)", 
                            hue="prediction",
                            alpha=0.75,
                        hue_order = ['0', '1', '2', '3'], ax=axs[1])
        axs[1].set_title('Zoomed In: K-Means Clusters')
        axs[0].set_title('All Data: K-Means Clusters')
        zoomed_in.legend(loc='center left', bbox_to_anchor=(1, 0.5), ncol=1, title = "Cluster Group")
        fig.tight_layout()
        plt.savefig("k_means_cluster.png")

        # Uploading this figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", 
                                           file_name= "k_means_cluster.png", 
                                           destination_blob_name="Clustering/k_means_cluster.png")

        
        # Average Daily Returns Histogram
        fig, axs = plt.subplots(ncols=2, figsize=(15,5))
        all_data_hist = sns.histplot(data=final_cluster_df, x="avg(daily_return)", kde=True, ax=axs[0])
        zoomed_data_hist = sns.histplot(data=new_final_cluster_df, x="avg(daily_return)", kde=True, ax=axs[1])
        axs[0].set_title('Average Daily Returns: All Data')
        axs[1].set_title('Average Daily Returns: Zoomed In')
        fig.tight_layout()
        plt.savefig("avg_dr_hist.png")

        # Uploading this figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", 
                                           file_name= "avg_dr_hist.png", 
                                           destination_blob_name="Clustering/avg_dr_hist.png")


        # Variance Daily Returns Histogram
        fig, axs = plt.subplots(ncols=2, figsize=(15,5))
        all_data_hist = sns.histplot(data=final_cluster_df, x="variance(daily_return)", kde=True, ax=axs[0])
        zoomed_data_hist = sns.histplot(data=new_final_cluster_df, x="variance(daily_return)", kde=True, ax=axs[1])
        axs[0].set_title('Variance Daily Returns: All Data')
        axs[1].set_title('Variance Daily Returns: Zoomed In')
        fig.tight_layout()
        plt.savefig("var_dr_hist.png")

        # Uploading this figure up to GCP bucket
        self.gcp_functions.upload_filename(bucket_name="stock-sp500", 
                                           file_name= "var_dr_hist.png", 
                                           destination_blob_name="Clustering/var_dr_hist.png")