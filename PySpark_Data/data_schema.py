from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

class Original_Schema:
    def full_stock_data_schema():
        stock_schema = StructType([StructField('Symbol', StringType(), False),
                                   StructField('Date', TimestampType(), False),
                                   StructField('Open', FloatType(), True),
                                   StructField('High', FloatType(), True),
                                   StructField('Low', FloatType(), True),
                                   StructField('Close', FloatType(), True),
                                   StructField('Adj Close', FloatType(), True),
                                   StructField('Volume', IntegerType(), True),
                                   StructField('Description', StringType(), False),
                                   StructField('Category2', StringType(), False),
                                   StructField('Category3', StringType(), False),
                                   StructField('GICS Sector', StringType(), False)])

        return stock_schema