from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

class Data_Model_Transforms:
    def lr_model_transform(self, df):
        stage_1 = StringIndexer(inputCol= 'day_of_week', outputCol= 'day_of_week_index')
        stage_2 = StringIndexer(inputCol= 'month', outputCol= 'month_index')
        stage_3 = OneHotEncoder(inputCols=[stage_1.getOutputCol(), stage_2.getOutputCol()], 
                                        outputCols= ['day_of_week_encoded', 'month_encoded'])

        vector = VectorAssembler(inputCols = ['lag_1', 'day_of_week_encoded', 'month_encoded', 'volume_lag_1'], outputCol = 'features')        
        data_pipeline = Pipeline(stages= [stage_1, stage_2, stage_3, vector])
        data_model = data_pipeline.fit(df).transform(df)
        
        return data_model