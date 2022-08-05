from retrieve_data import Get_Stock_Data
from data_cleaning import Data_Cleaning_Stock
from stock_plots import EDA_Plots
from k_means import K_Means_Stocks_Clustering

# Retrieving Data: retrieve_data.py
get_stock_data = Get_Stock_Data() # Class name Get_Stock_Data()
get_stock_data.sp_500_ticks() # Extracting S&P 500 ticker symbols
get_stock_data.yahoo_scrape() # Extracting all S&P 500 data

# Data Cleaning: data_cleaning.py
data_cleaning_stock = Data_Cleaning_Stock()
data_cleaning_stock.read_in_data_data_cleaning()
data_cleaning_stock.null_value_analysis()

# EDA: stock_plots.py
eda_plots = EDA_Plots()
eda_plots.read_in_data_data_cleaning()
eda_plots.eda_category_counts()
eda_plots.best_day_of_week_stocks()
eda_plots.most_valuable_gcis()
eda_plots.stock_daily_returns_correlation_plot()
eda_plots.industry_daily_returns_correlation_plot()

# Clustering: k_means.py
k_means_clustering = K_Means_Stocks_Clustering()
k_means_clustering.read_in_data_data_cleaning()
k_means_clustering.daily_returns_avg_var_cluster()

# ML Models:
# Linear Regression