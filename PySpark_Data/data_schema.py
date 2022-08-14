from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

class Original_Schema:
    def full_stock_data_schema(self):
        stock_schema = StructType([StructField('Symbol', StringType(), False),
                                   StructField('Date', TimestampType(), False),
                                   StructField('Open', FloatType(), True),
                                   StructField('High', FloatType(), True),
                                   StructField('Low', FloatType(), True),
                                   StructField('Close', FloatType(), True),
                                   StructField('Volume', IntegerType(), True),
                                   StructField('Description', StringType(), False),
                                   StructField('Category2', StringType(), False),
                                   StructField('Category3', StringType(), False),
                                   StructField('GICS Sector', StringType(), False)])

        return stock_schema


    def clean_stock_data_schema(self):
        clean_stock_schema = StructType([StructField('Symbol', StringType(), False),
                                         StructField('Date', TimestampType(), False),
                                         StructField('Open', FloatType(), False),
                                         StructField('High', FloatType(), False),
                                         StructField('Low', FloatType(), False),
                                         StructField('Close', FloatType(), False),
                                         StructField('Volume', IntegerType(), False),
                                         StructField('Description', StringType(), False),
                                         StructField('Category2', StringType(), False),
                                         StructField('Category3', StringType(), False),
                                         StructField('GICS Sector', StringType(), False),
                                         StructField('day_of_week', StringType(), False),
                                         StructField('year', StringType(), False),
                                         StructField('month', StringType(), False),
                                         StructField('day_of_month', StringType(), False),
                                         StructField('lag_1', FloatType(), False),
                                         StructField('volume_lag_1', FloatType(), False),
                                         StructField('daily_return', FloatType(), False),
                                         StructField( 'cumulative_return', FloatType(), False)])  

        return clean_stock_schema


class Pandas_UDF_Schema:
    def clean_data_lag_schema(self):
        lag_schema = StructType([StructField('Symbol', StringType(), True),
                                StructField('Date', TimestampType(), True),
                                StructField('Open', FloatType(), True),
                                StructField('High', FloatType(), True),
                                StructField('Low', FloatType(), True),
                                StructField('Close', FloatType(), True),
                                StructField('Volume', IntegerType(), True),
                                StructField('Description', StringType(), True),
                                StructField('Category2', StringType(), True),
                                StructField('Category3', StringType(), True),
                                StructField('GICS Sector', StringType(), True),
                                StructField('day_of_week', StringType(), True),
                                StructField('year', StringType(), True),
                                StructField('month', StringType(), True),
                                StructField('day_of_month', StringType(), True),
                                StructField('lag_1', FloatType(), True),
                                StructField('volume_lag_1', FloatType(), True),
                                StructField('daily_return', FloatType(), True)
                                ])     

        return lag_schema

    def modeling_schema_clean(self):
                modeling_schema = StructType([StructField('date', TimestampType(), True),
                                             StructField('symbol', StringType(), True),
                                             StructField('model_type', StringType(), True),
                                             StructField('daily_return', FloatType(), True),
                                             StructField('pred_daily_return', FloatType(), True),
                                             StructField('rmse', FloatType(), True)]) 

                return modeling_schema