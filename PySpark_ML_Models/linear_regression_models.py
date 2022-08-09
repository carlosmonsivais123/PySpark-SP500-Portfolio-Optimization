from upload_to_gcp import Upload_To_GCP
from read_data_source import Read_In_Data_Source
from data_transforms import Data_Model_Transforms


from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import joblib
from google.cloud import storage
from sklearn.linear_model import LinearRegression
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
        vars_needed = self.stock_df_clean.select('Symbol', 'Date', 'lag_1', 'day_of_week', 'month', 'volume_lag_1', 'daily_return')
        
        # PandasUDF Inputs
        group_column = 'Symbol'
        y_column = 'daily_return'
        x_columns = ['lag_1', 'day_of_week', 'month', 'volume_lag_1']
        random_state = 10

        modeling_schema = StructType([StructField('date', TimestampType(), True),
                                     StructField('symbol', StringType(), True),
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

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=10, shuffle=False)
            
            reg = LinearRegression().fit(X_train, y_train)
            y_preds = reg.predict(X_test)
            rmse = mean_squared_error(y_test, y_preds, squared=False)

            dates = pdf.iloc[y_test.index]['Date']
            
            preds_df = pd.DataFrame({'date': dates,
                                     'symbol': group_key, 
                                     'daily_return': y_test, 
                                     'pred_daily_return': y_preds,
                                     'rmse': rmse})
            
            # Export the model to a file
            model_filename = f'{group_key}_lr_model.joblib'
            joblib.dump(reg, model_filename)
            
            
            bucket_name="stock-sp500"
            destination_blob_name=f"Modeling/Models/{model_filename}"

            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_filename(model_filename)
            
            
            return preds_df
        
        lr_stocks = vars_needed.groupby("Symbol").apply(linear_regression).toPandas()
        lr_stocks.to_csv("gs://stock-sp500/Modeling/predictions.csv", index = False, header = True)