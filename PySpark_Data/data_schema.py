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
                                         StructField('lag_2', FloatType(), False),
                                         StructField('lag_3', FloatType(), False),
                                         StructField('lag_4', FloatType(), False),
                                         StructField('lag_5', FloatType(), False),
                                         StructField('lag_6', FloatType(), False),
                                         StructField('daily_return', FloatType(), False),
                                         StructField( 'cumulative_return', FloatType(), False)])

        return clean_stock_schema