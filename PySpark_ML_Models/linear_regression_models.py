from upload_to_gcp import Upload_To_GCP
from read_data_source import Read_In_Data_Source
from data_transforms import Data_Model_Transforms


from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import numpy as np
import joblib
from google.cloud import storage
from sklearn.linear_model import LinearRegression
from sklearn.neural_network import MLPRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

class ML_Model:
    def __init__(self):
        self.gcp_functions = Upload_To_GCP()
        self.read_in_data_source = Read_In_Data_Source()
        self.stock_df_clean = self.read_in_data_source.read_in_data_data_cleaning()
        self.data_model_transforms = Data_Model_Transforms()


    def linear_regression_models(self):
        vars_needed = self.stock_df_clean.select('Symbol', 'Date', 'lag_1', 'lag_2', 'lag_3', 'lag_4', 'lag_5', 'lag_6', 
                                                 'day_of_week', 'month', 'volume_lag_1', 'daily_return')
        
        # PandasUDF Inputs
        group_column = 'Symbol'
        y_column = 'daily_return'
        x_columns = ['lag_1', 'lag_2', 'lag_3', 'lag_4', 'lag_5', 'lag_6', 'day_of_week', 'month', 'volume_lag_1']
        random_state = 10

        modeling_schema = StructType([StructField('date', TimestampType(), True),
                                    StructField('symbol', StringType(), True),
                                    StructField('model_type', StringType(), True),
                                    StructField('daily_return', FloatType(), True),
                                    StructField('pred_daily_return', FloatType(), True),
                                    StructField('rmse', FloatType(), True)]) 


        @pandas_udf(modeling_schema, PandasUDFType.GROUPED_MAP)
        # Input/output are both a pandas.DataFrame
        def linear_regression(pdf):
            group_key = pdf[group_column].iloc[0]
            
            ohe_transform = ColumnTransformer(transformers=[('onehot', OneHotEncoder(), ['day_of_week', 'month'])], 
                                              remainder='passthrough')
            
            X = ohe_transform.fit_transform(pdf[x_columns])    
            y = pdf[y_column]

            X_train, X_test, y_train, y_test = train_test_split(X, 
                                                                y, 
                                                                test_size=0.20, 
                                                                random_state=random_state, 
                                                                shuffle=False)
            
            model_algs = {'LR': LinearRegression(), 
                          'KNN': KNeighborsRegressor(), 
                          'RF': RandomForestRegressor(random_state = random_state),
                          'GB': GradientBoostingRegressor(random_state=random_state),
                          'NN': MLPRegressor(random_state=random_state)}
            rmse_values = []
            
            for key, value in model_algs.items():
                reg = value.fit(X_train, y_train)
                y_preds = value.predict(X_test)
                rmse = mean_squared_error(y_test, y_preds, squared=False)
                rmse_values.append(rmse)
                
            best_model_index = np.argmin(rmse_values)
            model_type = list(model_algs.values())[best_model_index]
            model_name = list(model_algs.keys())[best_model_index]
            
            reg = model_type.fit(X_train, y_train)
            y_preds = reg.predict(X_test)
            rmse = mean_squared_error(y_test, y_preds, squared=False)

            dates = pdf.iloc[y_test.index]['Date']
        
            pred_df = pd.DataFrame({'date': dates,
                                    'symbol': group_key,
                                    'model_type': model_name,
                                    'daily_return': y_test, 
                                    'pred_daily_return': y_preds,
                                    'rmse': rmse})

            model_filename = f'{group_key}_{model_name}_model.joblib'
            joblib.dump(reg, model_filename)
            
            bucket_name="stock-sp500"
            destination_blob_name=f"Modeling/Models/{model_filename}"

            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_filename(model_filename)
            
            return pred_df


        
        lr_stocks = vars_needed.groupby("Symbol").apply(linear_regression).toPandas()
        lr_stocks.to_csv("gs://stock-sp500/Modeling/predictions.csv", index = False, header = True)