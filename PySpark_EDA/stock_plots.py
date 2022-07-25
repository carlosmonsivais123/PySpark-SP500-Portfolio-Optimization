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

class EDA_Plots:
    