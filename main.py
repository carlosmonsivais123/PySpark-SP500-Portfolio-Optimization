from data_cleaning import Data_Cleaning_Stock
from stock_plots import EDA_Plots

# Data Cleaning: data_cleaning.py
# data_cleaning_stock = Data_Cleaning_Stock()
# data_cleaning_stock.read_in_data_data_cleaning()
# data_cleaning_stock.null_value_analysis()

# EDA: stock_plots.py
eda_plots = EDA_Plots()
eda_plots.read_in_data_data_cleaning()
# eda_plots.eda_category_counts()
# eda_plots.best_day_of_week_stocks()
# eda_plots.most_valuable_gcis()
eda_plots.stock_symbol_correlation_plot()