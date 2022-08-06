from upload_to_gcp import Upload_To_GCP
from read_data_source import Read_In_Data_Source
from data_transforms import Data_Model_Transforms

from pyspark.sql import Window
from pyspark.sql.functions import percent_rank

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

import matplotlib.pyplot as plt
from datetime import datetime


class ML_Model:
    def __init__(self):
        self.gcp_functions = Upload_To_GCP()
        self.read_in_data_source = Read_In_Data_Source()
        self.stock_df_clean = self.read_in_data_source.read_in_data_data_cleaning()
        self.data_model_transforms = Data_Model_Transforms()



    def linear_regression_model(self):
        self.lr_model_log_string = ""

        # One stock at a time for now
        stock = self.stock_df_clean.filter(self.stock_df_clean.Symbol == "ABMD")
        stock = stock.withColumn("rank", percent_rank().over(Window.partitionBy().orderBy("Date")))

        # Variables we want in our final datafame with predictions
        vars_model = ['Date', 'Symbol', 'GICS Sector', 'lag_1', 'day_of_week', 'month', 'volume_lag_1', 'daily_return']

        train_stock = stock.filter(stock.rank<0.80).select(vars_model)
        test_stock = stock.filter(stock.rank >= 0.80).select(vars_model)

        train_transformed_data = self.data_model_transforms.lr_model_transform(train_stock)

        lr = LinearRegression(featuresCol="features", labelCol="daily_return")

        ##### Training Data #####
        # Fitting Model to Training Data
        lr_fit = lr.fit(train_transformed_data)
        train_preds = lr_fit.transform(train_transformed_data)
        lrevaluator = RegressionEvaluator(predictionCol="prediction", labelCol="daily_return", metricName="rmse")

        # RMSE score on training data
        training_rmse = lrevaluator.evaluate(train_preds)


        ##### Testing Data #####
        # Predictions Based on Testing Data
        test_transformed_data = self.data_model_transforms.lr_model_transform(test_stock)
        test_preds = lr_fit.transform(test_transformed_data)
        testing_rmse = lrevaluator.evaluate(test_preds)

        ## Saving Model ##
        lr_fit.write().overwrite().save('gs://stock-sp500/Models/LR_model')
        

        self.lr_model_log_string += f"{datetime.now()}: \nTrain RMSE: {training_rmse}\t Test RMSE: {testing_rmse}\n"
        self.gcp_functions.upload_string_message(bucket_name="stock-sp500", contents=self.lr_model_log_string, destination_blob_name="Models/RMSE_vals.txt")

        final_preds = test_preds.select('Date', 'Symbol', 'GICS Sector', 'daily_return', 'prediction')

        final_preds.coalesce(1)\
            .write\
                .option('header', 'true')\
                    .csv('gs://stock-sp500/Models/ABMD_preds.csv', mode='overwrite')